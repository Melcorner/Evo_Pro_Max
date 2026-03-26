# Интеграционная шина Эвотор ↔ МойСклад

Интеграционная шина между кассой **Эвотор** и учётной системой **МойСклад**.

Проект объединяет три основных контура интеграции:

1. **Продажи Эвотор → МойСклад** — event-driven pipeline через webhook, `event_store`, `worker`, `sale_handler`, `sale_mapper`.
2. **Товары и остатки МойСклад → Эвотор** — API-driven синхронизация через `sync.py`.
3. **Автоматическая синхронизация остатков** — webhook от МойСклад с обновлением остатков в Эвотор в реальном времени.

---

## Что реализовано

### Продажи Эвотор → МойСклад

- Приём webhook событий от Эвотор:
  - старый формат (`SELL`);
  - новый production-формат (`ReceiptCreated`).
- Нормализация webhook payload во внутренний `sale-payload`.
- Хранение событий в `event_store`.
- Идемпотентность через `processed_events`.
- Worker с optimistic locking и retry/backoff.
- Повторная обработка `FAILED` событий через `/events/{id}/requeue`.
- Маппинг `evotor_product_id -> ms_product_id` через `MappingStore`.
- Создание документа **Отгрузка** (`entity/demand`) в МойСклад по каждой продаже.

#### Данные покупателя

Поддерживается передача покупателя из чека Эвотор в МойСклад:

- поиск контрагента по `email`;
- поиск по `phone` (с нормализацией: `8xxx` → `7xxx`);
- создание нового контрагента при отсутствии;
- при отсутствии имени покупателя — fallback на `email → phone → "Покупатель"`;
- fallback на `default agent`, если buyer data отсутствует или резолв завершился ошибкой.

Источник резолва (`resolution_source`) логируется и передаётся в описание документа МойСклад.

#### Скидки в позициях чека

Поддерживаются оба сценария:

- enriched/test payload:
  - `resultPrice`
  - `resultSum`
  - `positionDiscount.discountPercent`
- реальный production webhook Эвотор:
  - `discount`
  - `totalDiscount`

Скидка в МойСклад передаётся корректно:

- базовая цена позиции остаётся базовой;
- скидка уходит отдельным полем `discount` в процентах;
- итоговая сумма demand считается после скидки.

#### Налоги в продаже

НДС для документа продажи переносится **из фактического чека Эвотор**:

- `taxPercent = 0` → `vat = 0`, `vatEnabled = false`
- `taxPercent = 10` → `vat = 10`, `vatEnabled = true`
- `taxPercent = 20` → `vat = 20`, `vatEnabled = true`

Источник истины по НДС продажи — **сам чек Эвотор**, а не карточка товара.

---

### Товары и остатки МойСклад → Эвотор

- Первичная синхронизация товаров Эвотор → МойСклад через `POST /sync/{tenant_id}/initial`.
- После завершения initial sync tenant переводится в рабочий режим **МойСклад → Эвотор** (`sync_completed_at`).
- Синхронизация одного товара МойСклад → Эвотор выполняется через:
  - `POST /sync/{tenant_id}/product/{ms_product_id}`
- Если товар уже связан через `mapping`, его карточка обновляется в Эвотор.
- Если товара ещё нет в Эвотор, он создаётся автоматически, после чего сохраняется связь `ms_id ↔ evotor_id`.
- Обычная синхронизация товара **не изменяет остаток**.
- При создании товара в МойСклад в рамках initial sync передаются **все штрихкоды** из Эвотор.
- Поддерживается синхронизация НДС товара МойСклад → Эвотор:
  - `vat / vatEnabled` из МойСклад маппится в `tax` Эвотор;
  - поддержаны: `NO_VAT`, `VAT_0`, `VAT_5`, `VAT_7`, `VAT_10`, `VAT_18`, `VAT_20`, `VAT_22`
- Поддерживается синхронизация типа маркируемого товара:
  - `trackingType = MILK` из МойСклад маппится в `type = DAIRY_MARKED` в Эвотор.
- Доступен вспомогательный endpoint для поиска товаров МойСклад:
  - `GET /sync/{tenant_id}/moysklad/products?search=...`
- Endpoint поиска возвращает: `ms_id`, `ui_id`, `ui_url`, `tracking_type`, `vat`, `vat_enabled`, `is_serial_trackable`

#### Синхронизация остатков

Остатки читаются из МойСклад через `/report/stock/all` с фильтром по полному href товара. При нулевом или отсутствующем остатке возвращается `0.0`.

Одиночная:

- `POST /sync/{tenant_id}/stock/{ms_product_id}`

Массовая:

- `POST /sync/{tenant_id}/stock/reconcile`

Статус:

- `GET /sync/{tenant_id}/stock/status`

Для статуса используется отдельная таблица `stock_sync_status`. Одиночная синхронизация не перезаписывает агрегированный статус от последнего `reconcile`.

---

### Автоматическая синхронизация остатков

Webhook от МойСклад автоматически обновляет остатки в Эвотор при изменении документов.

Поддерживаемые типы документов:

- `demand` — Отгрузка
- `supply` — Приёмка
- `inventory` — Инвентаризация
- `loss` — Списание
- `enter` — Оприходование

Цепочка:

```text
МойСклад документ → webhook → извлечение product_ids → /report/stock/all → обновление quantity в Эвотор
```

---

### Диагностика и обслуживание

- REST API для просмотра событий, ошибок и mappings.
- Сохранение токена Эвотор при установке приложения.
- Логирование API-потока и worker-потока.
- Ручные и batch-операции по синхронизации.
- Endpoint для поиска товаров МойСклад с API/UI id.

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
2. Захватывает событие через optimistic locking (`status IN ('NEW','RETRY')`)
3. Переводит событие в `PROCESSING`
4. Вызывает `dispatch_event(row)`
5. Переводит событие в `DONE`, `RETRY` или `FAILED`
6. Записывает `processed_events`
7. Записывает ошибку в `errors`, если обработка не удалась
8. Соединение с БД закрывается гарантированно через `try/finally`

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

- валидирует payload продажи (тип документа строго `SELL`);
- резолвит `evotor_id -> ms_id`;
- формирует `assortment.meta.href`;
- маппит скидки в позиции demand;
- маппит НДС из фактического чека в `vat / vatEnabled`;
- выставляет `syncId`.

Результат — документ **Отгрузка** (`POST /entity/demand`) в МойСклад.

---

### 2. Товары и остатки: API-driven sync

```text
Manual/API Trigger → sync.py → MoySklad API / Evotor API → mappings / stock_sync_status
```

#### Первичная синхронизация

`POST /sync/{tenant_id}/initial`

Алгоритм:

1. Получить все товары из Эвотор
2. Создать товары в МойСклад (со всеми штрихкодами)
3. Сохранить mappings `evotor_id ↔ ms_id`
4. Проставить `sync_completed_at`

После этого tenant переходит в рабочий режим **МойСклад → Эвотор**.

#### Синхронизация товара

`POST|PUT /sync/{tenant_id}/product/{ms_product_id}`

Поддерживается синхронизация: названия, цены, себестоимости, единицы измерения, штрихкодов, артикула, описания, НДС товара, типа маркируемого товара для молочной продукции.

Обычная синхронизация товара **не перезаписывает остаток**.

#### Синхронизация остатков

Остатки читаются через `GET /report/stock/all?filter=product={href}`.

Одиночная: `POST /sync/{tenant_id}/stock/{ms_product_id}`

- по `ms_product_id` ищется `evotor_id` в mappings;
- остаток читается из МойСклад;
- в Эвотор обновляется товар с новым `quantity`;
- агрегированный статус `reconcile` не затирается.

Массовая: `POST /sync/{tenant_id}/stock/reconcile`

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

---

### 3. Автоматическая синхронизация остатков: webhook МойСклад

```text
МойСклад создал документ → Webhook → moysklad_webhooks.py → позиции документа → /report/stock/all → Evotor API
```

При изменении остатков в МойСклад остатки автоматически обновляются в Эвотор.

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
- скидки: `discount`, `totalDiscount`, `resultPrice`, `resultSum`, `positionDiscount`
- налоги: `taxPercent`

### Старый формат — `SELL`

Поддерживается для обратной совместимости.

Оба формата нормализуются во внутренний sale-payload и проходят одинаковый pipeline.

---

## Хранилища и таблицы

| Таблица | Назначение |
|---|---|
| `tenants` | Tenant'ы и конфигурация интеграции |
| `event_store` | Очередь событий |
| `processed_events` | Идемпотентность обработки |
| `errors` | Журнал ошибок |
| `mappings` | Связи `evotor_id ↔ ms_id` |
| `stock_sync_status` | Агрегированный статус последней синхронизации остатков |

Все таблицы создаются при запуске `init_db.py`. Скрипт идемпотентен и безопасен для повторного запуска.

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

- `event_id`, `tenant_id`, `error_code`, `message`, `payload_snapshot`, `response_body`

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
│   │   ├── sync.py                 — initial sync, product sync, stock sync, product search
│   │   ├── tenants.py              — tenants и конфигурация MoySklad/Evotor
│   │   └── webhooks.py             — POST /webhooks/evotor/{tenant_id}
│   ├── clients/
│   │   ├── evotor_client.py        — клиент API Эвотор
│   │   └── moysklad_client.py      — клиент API МойСклад (остатки через /report/stock/all)
│   ├── handlers/
│   │   └── sale_handler.py
│   ├── mappers/
│   │   └── sale_mapper.py
│   ├── scripts/
│   │   └── init_db.py
│   ├── services/
│   │   ├── counterparty_resolver.py — резолвинг контрагента по email/phone
│   │   ├── error_logic.py
│   │   └── event_dispatcher.py
│   ├── stores/
│   │   ├── error_store.py
│   │   └── mapping_store.py
│   ├── workers/
│   │   └── worker.py               — event loop с optimistic locking и try/finally
│   ├── db.py
│   ├── logger.py
│   └── main.py
├── data/
│   └── app.db
├── docs/
│   └── PAYLOAD_CONTRACTS.md
├── tests/
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

`init_db` идемпотентен и безопасен для повторного запуска. Создаёт все таблицы включая `stock_sync_status`.

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

3. Подключить callback токена Эвотор:

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

### Поиск товаров МойСклад

```bash
curl "http://127.0.0.1:8000/sync/{tenant_id}/moysklad/products?search=МОЛОКО"
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
| PUT | `/sync/{tenant_id}/product/{ms_product_id}` | Upsert товара МойСклад → Эвотор |
| GET | `/sync/{tenant_id}/moysklad/products` | Поиск товаров МойСклад с API/UI id |
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

- `api`, `api.sync`, `api.webhooks`, `api.webhooks.moysklad`
- `worker`, `dispatcher`
- `sale_handler`, `sale_mapper`
- `moysklad`, `evotor_client`
- `counterparty_resolver`

---

## Что важно помнить

- Продажи обрабатываются через `event_store + worker`.
- Остатки читаются через `/report/stock/all` с фильтром `product={href}` — не через `/entity/assortment`.
- Остатки обновляются автоматически через webhook МойСклад → `moysklad_webhooks.py`.
- Ручные вызовы `/sync/...` обрабатываются напрямую в API-процессе — лог появляется в `uvicorn`, не в `worker`.
- Для `/sync/{tenant_id}/stock/{ms_product_id}` нужен существующий mapping товара.
- Для `/sync/{tenant_id}/stock/reconcile` используются все product mappings tenant'а.
- Одиночная синхронизация остатка не затирает агрегированный статус от `reconcile`.
- Для sale-pipeline источник истины по скидкам и НДС — **сам чек Эвотор**.
- Для карточки товара источник истины по НДС — **МойСклад**.
- Webhooks МойСклад настраиваются через API, не через UI.

---

## Дальнейшее развитие

- Dockerization + деплой на VPS
- Аутентификация на admin API
- Расширенный `/health` — статус воркера, БД, последнее событие
- Алерты на `FAILED` события в Telegram / email
- Дашборд мониторинга — события, ошибки, latency
- Проактивный мониторинг изменений API Эвотор и МойСклад
- Документация для оператора
- Приёмочное тестирование по сценарию
- Фискализация: документ из МойСклад → Эвотор
- Маркировка товара — код и статус из МойСклад
- Верификация подписи webhook Эвотор
