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
- поддержка формата:
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
- создаёт товары в МойСклад (с НДС и типом маркировки)
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

#### Клиент `fiscalization_client.py`

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

### 5. Мониторинг integration bus

Реализован базовый backend-дашборд мониторинга для контроля состояния integration bus.

#### JSON snapshot

`GET /monitoring/dashboard`

Возвращает snapshot текущего состояния системы:

- статус сервиса
- состояние worker
- количество событий по статусам:
  - `NEW`
  - `PROCESSING`
  - `DONE`
  - `RETRY`
  - `FAILED`
- последние проблемные события
- последние ошибки
- latency обработки успешных событий

#### HTML dashboard

`GET /dashboard`

Простая server-rendered HTML-страница для мониторинга без отдельного frontend-приложения.

На странице отображаются:

- общий статус integration bus
- время последнего обновления
- время последнего heartbeat worker
- карточки со статусами событий
- статус worker
- `avg / max / last latency`
- таблица проблемных событий
- таблица последних ошибок

#### Latency

Latency рассчитывается для успешных событий (`status = DONE`) по формуле:

```text
updated_at - created_at
```

---

### 6. Alerts: Telegram + email

Реализован отдельный контур автоматических уведомлений для критичных состояний integration bus.

Поддерживаются два канала доставки:

- Telegram
- email

#### Alert worker

`python -m app.workers.alert_worker`

Alert worker работает отдельно от основного `worker` и не вмешивается в обработку событий.

Реализованы четыре типа сигналов:

- `worker stale` или отсутствие heartbeat
- наличие событий со статусом `FAILED`
- наличие событий со статусом `RETRY`
- наличие ошибок синхронизации остатков в `stock_sync_status`

Alert отправляется только при смене состояния (anti-spam).

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
ADMIN_API_TOKEN=token
```

#### Какие ручки защищены

- `/tenants`
- `/sync`
- `/events`
- `/errors`
- `/mappings`
- `/monitoring`
- `/dashboard`

#### Какие ручки публичные

- `/health`
- `/webhooks/evotor/{tenant_id}`
- `/webhooks/moysklad/{tenant_id}`
- `/api/v1/user/token`
- `/onboarding`

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

### Мониторинг

```text
event_store / errors / service_heartbeats → monitoring.py → JSON snapshot / HTML dashboard
```

### Alerts: Telegram + email

```text
service_heartbeats / event_store / stock_sync_status → alert_worker.py → alert_logic.py → telegram_client.py / email_client.py → Telegram Bot API / SMTP
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

---

## API endpoint'ы

### Infrastructure

| Метод | URL | Описание |
|---|---|---|
| GET | `/health` | Проверка сервера и фоновых сервисов |

### Onboarding

| Метод | URL | Описание |
|---|---|---|
| GET | `/onboarding/evotor/connect` | Форма ввода Evotor token |
| POST | `/onboarding/evotor/connect` | Получить магазины по Evotor token |
| GET | `/onboarding/evotor/sessions/{session_id}/stores` | Выбор магазина Эвотор |
| POST | `/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token` | Загрузить данные МойСклад |
| POST | `/onboarding/store-profile` | Создать профиль магазина |

### Tenants

| Метод | URL | Описание |
|---|---|---|
| POST | `/tenants` | Создать tenant |
| GET | `/tenants` | Список tenants |
| PATCH | `/tenants/{tenant_id}/moysklad` | Сохранить конфигурацию tenant |
| PATCH | `/tenants/{tenant_id}/fiscal` | Сохранить конфигурацию фискализации |
| POST | `/tenants/{tenant_id}/complete-sync` | Отметить initial sync как завершённую |
| DELETE | `/tenants/{tenant_id}/complete-sync` | Сбросить initial sync |
| DELETE | `/tenants/{tenant_id}` | Удалить tenant и все связанные данные |

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

### Mappings

| Метод | URL | Описание |
|---|---|---|
| GET | `/mappings` | Список маппингов (фильтр по tenant_id, entity_type) |
| POST | `/mappings/` | Создать или обновить маппинг |
| DELETE | `/mappings/{tenant_id}/{entity_type}/{evotor_id}` | Удалить один маппинг |
| DELETE | `/mappings/{tenant_id}/{entity_type}` | Удалить все маппинги тенанта по типу |
| DELETE | `/mappings/{tenant_id}` | Удалить все маппинги тенанта |

### Диагностика

| Метод | URL | Описание |
|---|---|---|
| GET | `/events` | Последние события |
| GET | `/events/retry` | События в статусе RETRY |
| GET | `/events/failed` | События в статусе FAILED |
| GET | `/events/{id}` | Детали события |
| POST | `/events/{id}/requeue` | Повторная постановка FAILED → NEW |
| GET | `/errors` | Журнал ошибок |

### Monitoring

| Метод | URL | Описание |
|---|---|---|
| GET | `/monitoring/dashboard` | JSON snapshot состояния integration bus |
| GET | `/dashboard` | HTML dashboard мониторинга |

---

## Требования

- Python 3.11+
- PostgreSQL 14+
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
# База данных
DATABASE_URL=postgresql://user:password@localhost:5432/evotor_ms

# Безопасность
EVOTOR_WEBHOOK_SECRET=your_secret
ADMIN_API_TOKEN=token

# Telegram alerts (опционально)
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Email alerts (опционально)
SMTP_HOST=smtp.mail.ru
SMTP_PORT=465
SMTP_USERNAME=your_mail_login@mail.ru
SMTP_PASSWORD=your_external_app_password
SMTP_FROM=your_mail_login@mail.ru
ALERT_EMAIL_TO=recipient@example.com
SMTP_USE_SSL=true
SMTP_USE_TLS=false

# Worker
ALERT_POLL_INTERVAL_SEC=30
WORKER_STALE_AFTER_SEC=30
```

### 4. Создать базу данных PostgreSQL

```bash
sudo -u postgres psql
```

```sql
CREATE USER evotor WITH PASSWORD 'your_password';
CREATE DATABASE evotor_ms OWNER evotor;
GRANT ALL PRIVILEGES ON DATABASE evotor_ms TO evotor;
\q
```

### 5. Инициализировать схему БД

```bash
python -m app.scripts.init_db
```

### 6. Миграция с SQLite (если нужно)

Если ранее использовалась SQLite, перенести данные:

```bash
export SQLITE_PATH=data/app.db
python -m app.scripts.migrate_to_pg
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

### Alert worker

```bash
python -m app.workers.alert_worker
```

Alert worker использует настроенные каналы доставки:

- Telegram, если заданы `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`
- email, если заданы SMTP-параметры

Если настроены оба канала, уведомления отправляются и в Telegram, и на email.

### Dashboard

JSON snapshot:

```bash
GET /monitoring/dashboard
```

HTML dashboard:

```bash
GET /dashboard
```

Оба endpoint'а защищены `ADMIN_API_TOKEN`.

---

## Базовый сценарий настройки

### Через онбординг (рекомендуется)

1. Открыть `/onboarding/evotor/connect`
2. Ввести Evotor token — система получит список магазинов
3. Выбрать магазин
4. Ввести MoySklad token — система автоматически загрузит организации, склады и контрагентов
5. Выбрать организацию, склад и контрагента по умолчанию из списков
6. Создать профиль магазина
7. Выполнить `POST /sync/{tenant_id}/initial`

### Через API вручную

1. Создать tenant: `POST /tenants`
2. Настроить МойСклад: `PATCH /tenants/{tenant_id}/moysklad`
3. Сохранить токен Эвотор: `POST /api/v1/user/token`
4. Выполнить `POST /sync/{tenant_id}/initial`
5. Настроить фискализацию: `PATCH /tenants/{tenant_id}/fiscal`
6. Проверить статус: `GET /sync/{tenant_id}/status`

---

## Примеры curl

### Получить demand

```bash
curl -H "Authorization: Bearer token" \
  "http://127.0.0.1:8000/sync/{tenant_id}/demands"
```

### Получить клиентов и кассы fiscalization24

```bash
curl -H "Authorization: Bearer token" \
  "http://127.0.0.1:8000/sync/{tenant_id}/fiscal/clients"
```

### Отправить demand на фискализацию

```bash
curl -X POST \
  -H "Authorization: Bearer token" \
  "http://127.0.0.1:8000/sync/{tenant_id}/fiscalize/{ms_demand_id}"
```

### Получить статус чека

```bash
curl -H "Authorization: Bearer token" \
  "http://127.0.0.1:8000/sync/{tenant_id}/fiscalization/{uid}"
```

### Получить события

```bash
curl -H "Authorization: Bearer token" \
  "http://127.0.0.1:8000/events"
```

### Получить dashboard snapshot

```bash
curl -H "Authorization: Bearer token" \
  "http://127.0.0.1:8000/monitoring/dashboard"
```

---

## Дальнейшее развитие

- поддержка card/mixed payment в фискализации
- расширение dashboard: фильтры, цветовая индикация, дополнительные метрики
- E2E-тесты на PostgreSQL