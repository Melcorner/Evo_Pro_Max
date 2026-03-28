# Evotor ↔ MoySklad Integration Bus

Интеграционная шина между **Эвотор** и **МойСклад**.

Проект решает четыре основные задачи:

1. Приём продаж из Эвотор и формирование документов в МойСклад
2. Синхронизация товаров и остатков между МойСклад и Эвотор
3. Автоматическое обновление остатков по webhook от МойСклад
4. Фискализация документа **Отгрузка** из МойСклад через **Универсальный фискализатор**

---

## Что реализовано

### 1. Продажи Эвотор → МойСклад

- приём webhook событий от Эвотор
- поддержка форматов:
  - `SELL`
  - `ReceiptCreated`
- нормализация payload продажи
- сохранение события в `event_store`
- обработка через `worker`
- маппинг товаров `evotor_id -> ms_id`
- создание документа **Отгрузка** в МойСклад

#### Покупатель

Поддерживается перенос данных покупателя из webhook Эвотор:

- поиск контрагента по `email`
- поиск по `phone`
- создание нового контрагента
- fallback имени: `email → phone → "Покупатель"`
- fallback на `default agent`, если buyer data отсутствует или резолв завершился ошибкой

#### Скидки

Поддерживаются:

- production-поля `discount`, `totalDiscount`
- enriched/test-поля `resultPrice`, `resultSum`, `positionDiscount.discountPercent`

Скидка передаётся в МойСклад отдельным процентным полем `discount`, а базовая цена позиции не искажается.

#### НДС продажи

НДС переносится из фактического чека Эвотор:

- `taxPercent = 0` → `vat = 0`, `vatEnabled = false`
- `taxPercent = 10` → `vat = 10`, `vatEnabled = true`
- `taxPercent = 20` → `vat = 20`, `vatEnabled = true`

---

### 2. Товары и остатки МойСклад ↔ Эвотор

#### Initial sync

`POST /sync/{tenant_id}/initial`

Первичная синхронизация:

- получает товары из Эвотор
- создаёт товары в МойСклад
- сохраняет `mappings`
- переводит tenant в режим `МойСклад → Эвотор`

#### Синхронизация одного товара

`POST /sync/{tenant_id}/product/{ms_product_id}`

Если mapping уже есть:

- обновляется карточка товара в Эвотор

Если mapping отсутствует:

- товар создаётся в Эвотор
- затем сохраняется mapping

Синхронизируются:

- название
- цена
- себестоимость
- единица измерения
- штрихкоды
- описание
- артикул
- НДС
- тип маркируемого товара

#### Синхронизация остатков

- одиночная:
  - `POST /sync/{tenant_id}/stock/{ms_product_id}`
- массовая:
  - `POST /sync/{tenant_id}/stock/reconcile`
- статус:
  - `GET /sync/{tenant_id}/stock/status`

Остатки читаются из МойСклад через `/report/stock/all`.

Для статуса используется таблица `stock_sync_status`.

---

### 3. Автоматическая синхронизация остатков

Webhook от МойСклад обновляет остатки в Эвотор при изменении документов:

- `demand`
- `supply`
- `inventory`
- `loss`
- `enter`

Сценарий:

1. МойСклад отправляет webhook
2. система определяет затронутые товары
3. получает актуальные остатки через `/report/stock/all`
4. обновляет остатки в Эвотор

---

### 4. Фискализация документа МойСклад

Реализован отдельный контур:

**МойСклад demand → fiscalization24 → касса Эвотор**

#### Новый клиент `fiscalization_client.py`

Поддерживает:

- `get_clients()` — список клиентов, магазинов и касс интегратора
- `create_check(payload)` — отправка чека на фискализацию
- `get_check_state(uid)` — получение статуса чека

Авторизация:

- `X-Datetime: <unix timestamp UTC>`
- `Authorization: SHA1(X-Datetime + token)`

#### Контур фискализации

Сценарий:

1. `GET /sync/{tenant_id}/demands`
2. `POST /sync/{tenant_id}/fiscalize/{ms_demand_id}`
3. `_map_demand_to_fiscal_check()`
4. `FiscalizationClient.create_check()`
5. сохранение в `fiscalization_checks`
6. `GET /sync/{tenant_id}/fiscalization/{uid}`

#### Конфигурация tenant

Для фискализации нужны:

- `fiscal_token`
- `fiscal_client_uid`
- `fiscal_device_uid`

Настройка:

```http
PATCH /tenants/{tenant_id}/fiscal
```

#### Работа с demand

Получить последние отгрузки:

```http
GET /sync/{tenant_id}/demands
```

Получить клиентов и кассы fiscalization24:

```http
GET /sync/{tenant_id}/fiscal/clients
```

Отправить demand на фискализацию:

```http
POST /sync/{tenant_id}/fiscalize/{ms_demand_id}
```

Проверить статус чека:

```http
GET /sync/{tenant_id}/fiscalization/{uid}
```

#### Статусы чека

- `1` — новый
- `2` — отправлен на кассу
- `5` — принят кассой
- `9` — ошибка
- `10` — успешно фискализирован

#### Текущее ограничение MVP

Текущая версия фискализации работает в упрощённом сценарии:

- `paymentType = 1`
- `payCashSumma = сумма чека`
- `payCardSumma = 0`

То есть чек сейчас уходит как сценарий **наличной оплаты**.

---

## Безопасность

### Верификация webhook Эвотор

Для webhook от Эвотор реализована проверка заголовка:

```http
Authorization: Bearer <token>
```

Секрет берётся из переменной окружения:

```env
EVOTOR_WEBHOOK_SECRET=your_secret
```

Логика:

- если `EVOTOR_WEBHOOK_SECRET` задан, webhook без корректного Bearer-токена получает `401`
- если `EVOTOR_WEBHOOK_SECRET` не задан, проверка пропускается для локальной разработки

### Admin API auth

Для внутренних и административных endpoint'ов реализована **Bearer-auth защита** через `ADMIN_API_TOKEN`.

Переменная окружения:

```env
ADMIN_API_TOKEN=my_admin_token_123
```

Логика:

- если `ADMIN_API_TOKEN` задан, защищённые ручки требуют заголовок:

  ```http
  Authorization: Bearer <ADMIN_API_TOKEN>
  ```

- если токен не задан, защита отключается — это удобно для локальной разработки

#### Какие ручки защищены

Под Bearer-auth находятся:

- `/tenants`
- `/sync`
- `/events`
- `/errors`
- `/mappings`

#### Какие ручки публичные

Без admin auth остаются:

- `/health`
- `/webhooks/evotor/{tenant_id}`
- `/webhooks/moysklad/{tenant_id}`
- `/api/v1/user/token`

#### Использование в Swagger

После включения admin auth:

1. открой `/docs`
2. нажми кнопку **Authorize**
3. введи токен
4. Swagger начнёт автоматически подставлять `Authorization` в защищённые запросы

#### Использование через curl

Пример:

```bash
curl -X GET "http://127.0.0.1:8000/events" \
  -H "Authorization: Bearer my_admin_token_123"
```

Если токен не передан:

- `401 Missing Authorization header`

Если схема неверная:

- `401 Invalid Authorization scheme`

Если токен неверный:

- `401 Invalid admin token`

---

## Архитектура

### Продажи: event-driven pipeline

```text
Эвотор Webhook → Event Store → Worker → Dispatch → Sale Handler → Sale Mapper → МойСклад API
```

### Товары и остатки: API-driven sync

```text
Manual/API Trigger → sync.py → MoySklad API / Evotor API → mappings / stock_sync_status
```

### Автоматическая синхронизация остатков

```text
МойСклад документ → Webhook → moysklad_webhooks.py → позиции документа → /report/stock/all → Evotor API
```

### Фискализация документа

```text
Manual/API Trigger → sync.py → MoySklad demand → mapper → FiscalizationClient → fiscalization24
```

---

## Основные таблицы

| Таблица | Назначение |
|---|---|
| `tenants` | Tenant'ы и конфигурация интеграции |
| `event_store` | Очередь входящих событий |
| `processed_events` | Идемпотентность обработки |
| `errors` | Журнал ошибок |
| `mappings` | Связи `evotor_id ↔ ms_id` |
| `stock_sync_status` | Статус последней синхронизации остатков |
| `fiscalization_checks` | Отправленные чеки и их статусы |
| `service_heartbeats` | Heartbeat фоновых сервисов |

---

## Health-check

`GET /health` — публичный endpoint, не требует авторизации.

Показывает общее состояние сервиса:

- статус API и БД
- heartbeat worker (stale если не отвечал более `WORKER_STALE_AFTER_SEC` секунд)
- количество событий по статусам: `NEW / RETRY / FAILED / PROCESSING`
- время последней успешной обработки события
- количество тенантов с ошибкой синхронизации остатков

Верхний статус:

- `ok` — всё в норме
- `degraded` — worker stale, есть FAILED события или ошибки stock sync
- `error` — не удалось подключиться к БД

Пример ответа (`status: ok`):

```json
{
  "status": "ok",
  "service": "integration-bus",
  "timestamp": 1712000000,
  "checks": {
    "api": { "status": "ok" },
    "db": { "status": "ok" },
    "worker": {
      "status": "ok",
      "last_seen_at": 1712000000,
      "stale_after_sec": 30
    }
  },
  "events": {
    "new": 0,
    "retry": 0,
    "failed": 0,
    "processing": 0,
    "last_processed_at": 1711999990
  },
  "stock_sync": {
    "tenants_with_error": 0,
    "last_sync_at": 1711999800
  }
}
```

Пример ответа (`status: degraded`):

```json
{
  "status": "degraded",
  "service": "integration-bus",
  "timestamp": 1712000000,
  "checks": {
    "api": { "status": "ok" },
    "db": { "status": "ok" },
    "worker": {
      "status": "stale",
      "last_seen_at": 1711999900,
      "stale_after_sec": 30
    }
  },
  "events": {
    "new": 2,
    "retry": 1,
    "failed": 3,
    "processing": 0,
    "last_processed_at": 1711999850
  },
  "stock_sync": {
    "tenants_with_error": 1,
    "last_sync_at": 1711999800
  }
}
```

---

## API endpoint'ы

### Infrastructure

| Метод | URL | Описание |
|---|---|---|
| GET | `/health` | Проверка сервера и фоновых сервисов |

### Tenants

| Метод | URL | Описание |
|---|---|---|
| POST | `/tenants` | Создать tenant |
| GET | `/tenants` | Список tenants |
| PATCH | `/tenants/{tenant_id}/moysklad` | Сохранить конфигурацию tenant |
| PATCH | `/tenants/{tenant_id}/fiscal` | Сохранить конфигурацию фискализации |
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
| GET | `/sync/{tenant_id}/demands` | Последние документы demand из МойСклад |
| GET | `/sync/{tenant_id}/fiscal/clients` | Список клиентов и касс фискализатора |
| POST | `/sync/{tenant_id}/fiscalize/{ms_demand_id}` | Отправить demand на фискализацию |
| GET | `/sync/{tenant_id}/fiscalization/{uid}` | Статус чека фискализации |
| POST | `/sync/{tenant_id}/product/{ms_product_id}` | Синхронизация одного товара МойСклад → Эвотор |
| GET | `/sync/{tenant_id}/moysklad/products` | Поиск товаров МойСклад |
| POST | `/sync/{tenant_id}/stock/{ms_product_id}` | Синхронизация остатка одного товара |
| POST | `/sync/{tenant_id}/stock/reconcile` | Batch-синхронизация остатков |
| GET | `/sync/{tenant_id}/stock/status` | Статус последней синхронизации остатков |

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

## Требования

- Python 3.11+
- Доступ к API Эвотор
- Доступ к API МойСклад
- Доступ к API Универсального фискализатора

---

## Установка

### 1. Создать виртуальное окружение

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

### 2. Установить зависимости

```bash
pip install -r requirements.txt
```

### 3. Настроить `.env`

```env
EVOTOR_WEBHOOK_SECRET=your_secret
ADMIN_API_TOKEN=my_admin_token_123
```

### 4. Инициализировать БД

```bash
python -m app.scripts.init_db
```

---

## Запуск

### API сервер

```bash
uvicorn app.main:app --reload
```

Swagger:
`http://127.0.0.1:8000/docs`

### Worker

```bash
python -m app.workers.worker
```

---

## Базовый сценарий настройки

1. Создать tenant
2. Настроить МойСклад и Эвотор
3. Сохранить токен Эвотор через `/api/v1/user/token`
4. Выполнить `POST /sync/{tenant_id}/initial`
5. Настроить реквизиты фискализации через `PATCH /tenants/{tenant_id}/fiscal`
6. Проверить статус через `GET /sync/{tenant_id}/status`

---

## Примеры curl

### Получить demand

```bash
curl -H "Authorization: Bearer my_admin_token_123" \
  "http://127.0.0.1:8000/sync/{tenant_id}/demands"
```

### Получить клиентов и кассы fiscalization24

```bash
curl -H "Authorization: Bearer my_admin_token_123" \
  "http://127.0.0.1:8000/sync/{tenant_id}/fiscal/clients"
```

### Отправить demand на фискализацию

```bash
curl -X POST \
  -H "Authorization: Bearer my_admin_token_123" \
  "http://127.0.0.1:8000/sync/{tenant_id}/fiscalize/{ms_demand_id}"
```

### Получить статус чека

```bash
curl -H "Authorization: Bearer my_admin_token_123" \
  "http://127.0.0.1:8000/sync/{tenant_id}/fiscalization/{uid}"
```

### Получить события

```bash
curl -H "Authorization: Bearer my_admin_token_123" \
  "http://127.0.0.1:8000/events"
```

---

## Дальнейшее развитие

- поддержка card/mixed payment в фискализации
- Dockerization и деплой
- дашборд мониторинга
- дополнительные E2E-тесты
- алерты по `FAILED` событиям и stale worker
- верификация webhook МойСклад (эндпоинт сейчас публичный)
