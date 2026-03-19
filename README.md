# Интеграционная шина Эвотор ↔ МойСклад

Интеграционная шина между кассой **Эвотор** и учётной системой **МойСклад**.

Проект объединяет три контура интеграции:

1. **Продажи Эвотор → МойСклад** — event-driven pipeline через webhook, `event_store`, `worker`, `sale_handler`, `sale_mapper`.
2. **Товары и остатки МойСклад → Эвотор** — API-driven синхронизация через `sync.py`.
3. **Автоматическая синхронизация остатков** — webhook от МойСклад с обновлением остатков в Эвотор в реальном времени.

---

## Что реализовано

### Продажи Эвотор → МойСклад

- Приём webhook событий от Эвотор: старый формат (`SELL`) и новый формат (`ReceiptCreated`)
- Нормализация webhook payload во внутренний sale-payload
- Хранение событий в `event_store`
- Идемпотентность через `processed_events`
- Worker с optimistic locking и retry/backoff
- Повторная обработка `FAILED` событий через `/events/{id}/requeue`
- Маппинг `evotor_product_id -> ms_product_id` через `MappingStore`
- Создание документа **Отгрузка** (`entity/demand`) в МойСклад по каждой продаже
- Передача данных покупателя из чека Эвотор в МойСклад:
  - поиск контрагента по email;
  - поиск по телефону;
  - создание нового контрагента при отсутствии;
  - fallback на `default agent`, если buyer data отсутствует
- Поддержка скидок в позициях чека:
  - обработка скидки из реального webhook Эвотор (`discount`, `totalDiscount`);
  - обработка enriched/test payload (`resultPrice`, `resultSum`, `positionDiscount`);
  - запись скидки в позиции demand МойСклад отдельным полем `discount`
- Поддержка налогов в продаже:
  - маппинг налога из фактического чека Эвотор в `vat / vatEnabled` в МойСклад;
  - источник истины по НДС продажи — **чек Эвотор**, а не карточка товара

### Товары и остатки МойСклад → Эвотор

- Первичная синхронизация товаров Эвотор → МойСклад через `POST /sync/{tenant_id}/initial`
- Переключение tenant в рабочий режим МойСклад → Эвотор после initial sync (`sync_completed_at`)
- Синхронизация одного товара МойСклад → Эвотор через `POST /sync/{tenant_id}/product/{ms_product_id}`
- Корректная синхронизация НДС товара МойСклад → Эвотор:
  - `vat / vatEnabled` из МойСклад маппится в `tax` Эвотор;
  - поддержаны `NO_VAT`, `VAT_0`, `VAT_10`, `VAT_18`, `VAT_20`, `VAT_5`, `VAT_7`, `VAT_22`
- Одиночная синхронизация остатка через `POST /sync/{tenant_id}/stock/{ms_product_id}`
- Массовая синхронизация остатков через `POST /sync/{tenant_id}/stock/reconcile`
- Статус синхронизации остатков через `GET /sync/{tenant_id}/stock/status`
- Отдельное хранение статуса stock sync в таблице `stock_sync_status`

### Автоматическая синхронизация остатков

- Webhook от МойСклад при создании/изменении документов
- Поддержка документов:
  - `demand`
  - `supply`
  - `inventory`
  - `loss`
  - `enter`
- Автоматическое извлечение затронутых товаров из позиций документа
- Обновление остатков в Эвотор в реальном времени без ручного вмешательства

### Диагностика и обслуживание

- REST API для просмотра событий, ошибок и mappings
- Сохранение токена Эвотор при установке приложения
- Логирование API-потока и worker-потока
- Ручные и batch-операции по синхронизации

---

## Архитектура

### 1. Продажи: event-driven pipeline

```text
Эвотор Webhook → Event Store → Worker → Dispatch → Sale Handler → Sale Mapper → МойСклад API
```

#### Ingest Layer

`POST /webhooks/evotor/{tenant_id}` принимает события от Эвотор и сохраняет их в `event_store` со статусом `NEW`.

Дополнительно endpoint принимает событие установки приложения и сохраняет токен облака Эвотор в `tenants`.

#### Worker

Фоновый процесс:

1. Выбирает события `NEW` или `RETRY`
2. Переводит событие в `PROCESSING`
3. Вызывает `dispatch_event(row)`
4. Переводит событие в `DONE`, `RETRY` или `FAILED`
5. Записывает `processed_events`
6. Записывает ошибку в `errors`, если обработка не удалась

#### Dispatch Layer

`event_dispatcher.py` маршрутизирует событие по `event_type`.

| `event_type` | Поведение |
|---|---|
| `sale` | Передаётся в `handle_sale` |
| `product` | Логируется и пропускается |
| Остальное | Ошибка → `RETRY` / `FAILED` |

#### Sale Handler / Mapper

`sale_handler.py`:

- загружает tenant-конфиг;
- резолвит контрагента по данным покупателя из чека;
- вызывает `sale_mapper.py`;
- отправляет demand в МойСклад.

`sale_mapper.py`:

- валидирует payload продажи;
- резолвит `evotor_id -> ms_id`;
- формирует `assortment.meta.href`;
- маппит скидки в позиции demand;
- маппит НДС из фактического чека в `vat / vatEnabled`;
- выставляет `syncId`.

Результат — документ **Отгрузка** (`POST /entity/demand`) в МойСклад.

### 2. Товары и остатки: API-driven sync

```text
Manual/API Trigger → sync.py → MoySklad API / Evotor API → mappings / stock_sync_status
```

Этот контур используется для справочников и остатков.

#### Первичная синхронизация

`POST /sync/{tenant_id}/initial`

Алгоритм:

1. Получить все товары из Эвотор
2. Создать товары в МойСклад
3. Сохранить mappings `evotor_id ↔ ms_id`
4. Проставить `sync_completed_at`

После этого tenant переходит в рабочий режим **МойСклад → Эвотор**.

#### Синхронизация товара

`POST /sync/{tenant_id}/product/{ms_product_id}`

Используется для создания или обновления карточки товара в Эвотор по данным из МойСклад.

Поддерживается синхронизация:

- названия;
- цены;
- себестоимости;
- единицы измерения;
- штрихкодов;
- статьи;
- описания;
- НДС товара.

Важно: обычная синхронизация товара **не должна перезаписывать остаток**.

#### Синхронизация остатков

Одиночная:

`POST /sync/{tenant_id}/stock/{ms_product_id}`

- по `ms_product_id` ищется `evotor_id` в mappings;
- остаток читается из МойСклад;
- в Эвотор обновляется товар с новым `quantity`.

Массовая:

`POST /sync/{tenant_id}/stock/reconcile`

- берутся все product mappings tenant'а;
- по каждому товару читается остаток из МойСклад;
- остаток обновляется в Эвотор;
- обновляется агрегированный статус в `stock_sync_status`.

#### Статус синхронизации остатков

`GET /sync/{tenant_id}/stock/status`

Возвращает:

- `status: configured | in_progress | ok | error`
- `last_sync_time`
- `last_error`
- `count_synced_items`
- `total_items_count`

### 3. Автоматическая синхронизация остатков: webhook МойСклад

```text
МойСклад создал документ → Webhook → moysklad_webhooks.py → позиции документа → Evotor API
```

При изменении остатков в МойСклад (через отгрузку, приёмку, инвентаризацию, списание) остатки автоматически обновляются в Эвотор.

#### Поддерживаемые типы документов

| Тип | Описание | Эффект на остатки |
|---|---|---|
| `demand` | Отгрузка | Уменьшение |
| `supply` | Приёмка | Увеличение |
| `inventory` | Инвентаризация | Корректировка |
| `loss` | Списание | Уменьшение |
| `enter` | Оприходование | Увеличение |

---

## Форматы событий Эвотор

Система поддерживает два формата webhook продаж от Эвотор.

### Новый формат — `ReceiptCreated`

Актуальный production-формат. Внешний тип события — `ReceiptCreated`, внутренний тип документа — `data.type = SELL`.

Поддерживаются поля:

- buyer data (`customer`)
- скидки (`discount`, `totalDiscount`, а также enriched/test `resultPrice`, `resultSum`, `positionDiscount`)
- налоги (`tax`, `taxPercent`, `totalTax`)

### Старый формат — `SELL`

Поддерживается для обратной совместимости.

Оба формата нормализуются во внутренний sale-payload и проходят одинаковый pipeline.

---

## Хранилища и таблицы

Основные таблицы БД:

- `tenants` — tenant'ы и конфигурация интеграции
- `event_store` — очередь событий
- `processed_events` — идемпотентность
- `errors` — журнал ошибок
- `mappings` — связи `evotor_id ↔ ms_id`
- `stock_sync_status` — агрегированный статус последней синхронизации остатков

---

## Жизненный цикл sale-события

```text
NEW → PROCESSING → DONE
               ↘
               RETRY (до 5 раз) → FAILED
                                       ↓
                                  requeue → NEW
```

---

## Обработка ошибок

### Классификация

| Тип ошибки | Решение |
|---|---|
| Timeout / ConnectionError | RETRY |
| HTTP 429 | RETRY |
| HTTP 5xx | RETRY |
| HTTP 400 / 401 / 403 / 422 | FAILED |
| `SalePayloadError` | FAILED |
| `MappingNotFoundError` | FAILED |
| Остальные неизвестные | RETRY |

### Retry-политика

- Exponential backoff: `1m → 2m → 4m → 8m → 16m`
- Максимум 5 попыток, затем → `FAILED`

### Таблица `errors`

При неуспешной обработке sale-события сохраняются:

- `event_id`
- `tenant_id`
- `error_code`
- `message`
- `payload_snapshot`
- диагностическая информация об ошибке

---

## Структура проекта

```text
integration-bus/
├── app/
│   ├── api/
│   │   ├── errors.py               — журнал ошибок
│   │   ├── events.py               — просмотр и requeue событий
│   │   ├── evotor.py               — token callback и служебные endpoint'ы Эвотор
│   │   ├── mappings.py             — CRUD /mappings
│   │   ├── moysklad_webhooks.py    — POST /webhooks/moysklad/{tenant_id}
│   │   ├── sync.py                 — initial sync, product sync, stock sync, stock status
│   │   ├── tenants.py              — tenants и конфигурация MoySklad/Evotor
│   │   └── webhooks.py             — POST /webhooks/evotor/{tenant_id}
│   ├── clients/
│   │   ├── evotor_client.py        — клиент API Эвотор
│   │   └── moysklad_client.py      — клиент API МойСклад
│   ├── handlers/
│   │   └── sale_handler.py
│   ├── mappers/
│   │   └── sale_mapper.py
│   ├── scripts/
│   │   └── init_db.py
│   ├── services/
│   │   ├── counterparty_resolver.py — резолвинг контрагента
│   │   ├── error_logic.py
│   │   └── event_dispatcher.py
│   ├── stores/
│   │   ├── error_store.py
│   │   └── mapping_store.py
│   ├── workers/
│   │   └── worker.py
│   ├── db.py
│   ├── logger.py
│   └── main.py
├── data/
│   └── app.db
├── docs/
│   └── PAYLOAD_CONTRACTS.md
├── tests/
│   ├── e2e_test.py
│   └── test_sale2.py
├── README.md
└── requirements.txt
```

---

## Требования

- Python 3.11+
- macOS / Linux / Windows
- Доступ к API Эвотор
- Доступ к API МойСклад

---

## Установка и запуск

### 1. Клонировать проект

```bash
git clone <repo-url>
cd integration-bus
```

### 2. Создать виртуальное окружение

macOS / Linux:

```bash
python3.11 -m venv venv
source venv/bin/activate
```

Windows:

```powershell
py -3.11 -m venv venv
venv\Scripts\activate
```

### 3. Установить зависимости

```bash
pip install -r requirements.txt
```

### 4. Инициализировать базу данных

```bash
python -m app.scripts.init_db
```

`init_db` идемпотентен и безопасен для повторного запуска.

---

## Запуск

### Терминал 1 — API сервер

```bash
uvicorn app.main:app --reload
```

Swagger: `http://127.0.0.1:8000/docs`

### Терминал 2 — Worker

```bash
python -m app.workers.worker
```

---

## Базовый сценарий настройки

1. Создать tenant:

```http
POST /tenants
```

2. Настроить реквизиты МойСклад и store Эвотор:

```http
PATCH /tenants/{tenant_id}/moysklad
```

3. Подключить callback токена Эвотор (настраивается в `dev.evotor.ru`):

```http
POST /api/v1/user/token
```

4. Выполнить первичную синхронизацию:

```http
POST /sync/{tenant_id}/initial
```

5. Зарегистрировать webhooks в МойСклад для документов `demand`, `supply`, `inventory`, `loss`, `enter`.

6. Проверить общий статус:

```http
GET /sync/{tenant_id}/status
```

---

## Примеры запросов

### Одиночная синхронизация товара

```bash
curl -X POST "http://127.0.0.1:8000/sync/{tenant_id}/product/{ms_product_id}"
```

### Одиночная синхронизация остатка

```bash
curl -X POST "http://127.0.0.1:8000/sync/{tenant_id}/stock/{ms_product_id}"
```

### Массовая синхронизация остатков

```bash
curl -X POST "http://127.0.0.1:8000/sync/{tenant_id}/stock/reconcile"
```

### Статус синхронизации остатков

```bash
curl "http://127.0.0.1:8000/sync/{tenant_id}/stock/status"
```

---

## API endpoint'ы

### Infrastructure

| Метод | URL | Описание |
|---|---|---|
| GET | `/health` | Проверка сервера |

### Tenants

| Метод | URL | Описание |
|---|---|---|
| POST | `/tenants` | Создать tenant |
| GET | `/tenants` | Список tenants |
| PATCH | `/tenants/{tenant_id}/moysklad` | Сохранить конфигурацию tenant |
| POST | `/tenants/{tenant_id}/complete-sync` | Отметить initial sync как завершённую |
| DELETE | `/tenants/{tenant_id}/complete-sync` | Сбросить initial sync |

### Webhooks

| Метод | URL | Описание |
|---|---|---|
| POST | `/webhooks/evotor/{tenant_id}` | Принять webhook от Эвотор |
| POST | `/webhooks/moysklad/{tenant_id}` | Принять webhook от МойСклад |
| POST | `/api/v1/user/token` | Сохранить токен Эвотор |

### Sync API

| Метод | URL | Описание |
|---|---|---|
| POST | `/sync/{tenant_id}/initial` | Первичная синхронизация товаров Эвотор → МойСклад |
| GET | `/sync/{tenant_id}/status` | Общий статус синхронизации tenant |
| POST | `/sync/{tenant_id}/product/{ms_product_id}` | Синхронизация одного товара МойСклад → Эвотор |
| POST | `/sync/{tenant_id}/stock/{ms_product_id}` | Синхронизация остатка одного товара |
| POST | `/sync/{tenant_id}/stock/reconcile` | Batch-синхронизация остатков |
| GET | `/sync/{tenant_id}/stock/status` | Статус последней синхронизации остатков |

### Mappings

| Метод | URL | Описание |
|---|---|---|
| GET | `/mappings` | Список mappings |
| POST | `/mappings/` | Создать или обновить mapping |
| DELETE | `/mappings/` | Удалить mapping |

### Диагностика

| Метод | URL | Описание |
|---|---|---|
| GET | `/events` | Последние события |
| GET | `/events/retry` | События в статусе RETRY |
| GET | `/events/failed` | События в статусе FAILED |
| GET | `/events/{id}` | Детали события |
| POST | `/events/{id}/requeue` | Повторная постановка FAILED → NEW |
| GET | `/errors` | Журнал ошибок |

---

## Логирование

Формат:

```text
timestamp | level | logger | message
```

Основные логгеры:

- `api`
- `api.sync`
- `api.webhooks`
- `api.webhooks.moysklad`
- `worker`
- `dispatcher`
- `sale_handler`
- `sale_mapper`
- `moysklad`
- `evotor_client`

---

## Что важно помнить

- Продажи обрабатываются через `event_store + worker`.
- Остатки обновляются автоматически через webhook МойСклад → `moysklad_webhooks.py`.
- Ручные вызовы `/sync/...` обрабатываются напрямую в API-процессе — лог появляется в `uvicorn`, не в `worker`.
- Для `/sync/{tenant_id}/stock/{ms_product_id}` нужен существующий mapping товара.
- Для `/sync/{tenant_id}/stock/reconcile` используются все product mappings tenant'а.
- Для sale-pipeline источник истины по скидкам и НДС — **сам чек Эвотор**.
- Для карточки товара источник истины по НДС — **МойСклад**.
- Webhooks МойСклад настраиваются через API, не через UI.

---

## Дальнейшее развитие

- Аутентификация на admin API
- Dockerization + деплой на VPS
- Расширенный `/health` — статус воркера, БД, последнее событие
- Алерты на `FAILED` события в Telegram / email
- Поддержка покупателей и скидок для дополнительных форматов чеков
- Расширение маппинга налогов для нестандартных ставок
- Фискализация: документ из МойСклад → Эвотор
- Маркировка товара
- Мониторинг / dashboard
- Валидация подписи webhook Эвотор
