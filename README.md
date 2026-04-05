# Evotor ↔ MoySklad Integration Bus

Интеграционная шина между **Эвотор** и **МойСклад**.

Проект решает пять основных задач:

1. Приём продаж из Эвотор и формирование документов в МойСклад
2. Синхронизация товаров и остатков между МойСклад и Эвотор
3. Автоматическое обновление остатков по webhook от МойСклад
4. Фискализация документа **Отгрузка** из МойСклад через **Универсальный фискализатор**
5. Наблюдаемость integration bus: monitoring, metrics, dashboards, logs и alerts

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
- поддержка двух способов маршрутизации webhook:
  - `POST /webhooks/evotor/{tenant_id}` — tenant передан явно
  - `POST /webhooks/evotor` — tenant автоматически резолвится по `evotor_store_id` или `evotor_user_id`

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

#### Fiscal poller

Для проверки статусов чеков реализован отдельный фоновый процесс `fiscal_poller`.

Он:

- выбирает pending-записи из `fiscalization_checks`
- опрашивает fiscalization24 через `get_check_state(uid)`
- обновляет статус чека
- ведёт счётчики polling-результатов и backlog pending-checks

Запуск:

```bash
python -m app.workers.fiscal_poller
```

#### Текущее ограничение MVP

Текущая версия фискализации работает в упрощённом сценарии:

- `paymentType = 1`
- `payCashSumma = сумма чека`
- `payCardSumma = 0`

То есть чек сейчас уходит как сценарий **наличной оплаты**.

---

### 5. Monitoring: JSON snapshot + HTML dashboard

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

#### Problem Events

В блоке **Problem Events** показываются проблемные события из `event_store` со статусами:

- `RETRY`
- `FAILED`

Для каждого события отображаются:

- `Event ID`
- `tenant_id`
- `event_type`
- `event_key`
- `status`
- `retries`
- `last_error_message`
- `updated_at`

#### Recent Errors

В блоке **Recent Errors** показываются последние записи из таблицы `errors`.

Для каждой ошибки отображаются:

- `event_id`
- `tenant_id`
- `error_code`
- `message`
- `created_at`

#### Latency

Latency рассчитывается для успешных событий (`status = DONE`) по формуле:

```text
updated_at - created_at
```

То есть latency — это время прохождения события через integration bus от момента записи в очередь до завершения обработки.

Для dashboard используются:

- `avg_latency_sec` — средняя latency по последним успешным событиям
- `max_latency_sec` — максимальная latency
- `last_latency_sec` — latency последнего успешного события

#### Проверенные сценарии

Dashboard был проверен на следующих сценариях:

- `worker stale`
- восстановление worker (`stale → ok`)
- появление `DONE`
- появление `FAILED`
- появление `RETRY`
- отображение ошибок в `errors`

---

### 6. Prometheus metrics

Реализован отдельный слой метрик для Prometheus.

#### API metrics

`GET /metrics`

API экспортирует Prometheus-метрики через отдельный endpoint `/metrics`.

Базовые метрики API:

- `integration_api_requests_total`
- `integration_api_request_duration_seconds`
- `integration_api_exceptions_total`

Дополнительно перед отдачей `/metrics` обновляются DB-derived gauge-метрики по состоянию integration bus.

#### Worker metrics exporter

Основной `worker` запускает отдельный HTTP exporter для Prometheus.

Базовые метрики worker:

- `integration_worker_cycles_total`
- `integration_worker_idle_cycles_total`
- `integration_worker_events_picked_total`
- `integration_worker_events_processed_total`
- `integration_worker_processing_duration_seconds`
- `integration_worker_last_heartbeat_unixtime`
- `integration_worker_stale_recovered_total`

Exporter worker доступен на отдельном порту, по умолчанию:

```text
http://127.0.0.1:8001/metrics
```

#### Fiscal poller metrics exporter

`fiscal_poller` также экспортирует отдельные метрики.

Базовые метрики fiscal poller:

- `integration_fiscal_poller_cycles_total`
- `integration_fiscal_poller_pending_checks`
- `integration_fiscal_poller_polled_total`
- `integration_fiscal_poller_poll_duration_seconds`

Exporter fiscal poller доступен по умолчанию:

```text
http://127.0.0.1:8002/metrics
```

#### Метрики фискализации

В API-слое добавлены отдельные метрики для ручек фискализации:

- `integration_fiscalization_requests_total`
- `integration_fiscalization_request_duration_seconds`
- `integration_fiscalization_status_checks_total`
- `integration_fiscalization_state_total`

Тем самым контур observability покрывает:

- API
- worker
- fiscal poller
- фискализацию

---

### 7. Structured logging + Loki

Реализовано структурированное логирование в JSON-формате и агрегация логов через Loki.

#### JSON logging

Логирование поддерживает два режима:

- `text`
- `json`

Для observability используется `json`.

В JSON-логах поддерживаются поля:

- `timestamp`
- `level`
- `logger`
- `service`
- `message`

И дополнительные контекстные поля:

- `component`
- `tenant_id`
- `event_id`
- `event_key`
- `uid`
- `operation`
- `status`
- `duration_ms`
- `doc_type`
- `doc_id`
- `exception_type`

#### Логи в файлы

Каждый процесс может писать логи не только в stdout, но и в файл.

Используются переменные:

```env
LOG_TO_FILE=true
LOG_FILE_PATH=logs/api.log
```

Для observability используются три файла:

- `logs/api.log`
- `logs/worker.log`
- `logs/fiscal_poller.log`

#### Loki + Promtail

В docker-compose observability-стека добавлены:

- `loki`
- `promtail`

Promtail читает лог-файлы из `logs/` и отправляет их в Loki.

Для потоков логов используются отдельные labels:

- `app=api`
- `app=worker`
- `app=fiscal_poller`

Тем самым логи можно просматривать в Grafana через datasource `Loki`.

---

### 8. Grafana dashboards

Реализованы отдельные дашборды Grafana, соответствующие контурам системы.

#### Набор дашбордов

Созданы пять отдельных дашбордов:

- `API`
- `Worker`
- `Fiscal poller`
- `Фискализация`
- `Logs`

#### Дашборд API

Показывает:

- общий request rate
- error rate
- p95 latency
- request rate по путям
- latency по путям
- ошибки по status code

#### Дашборд Worker

Показывает:

- heartbeat age
- cycles rate
- idle cycles rate
- picked events rate
- processed results
- processing duration p95
- stale recovered total

#### Дашборд Fiscal poller

Показывает:

- pending checks
- cycles rate
- polling results
- poll duration p95
- results timeline

#### Дашборд Фискализация

Показывает:

- fiscalization requests total
- requests by result
- request duration p95
- status checks total
- status checks by result
- observed states
- state timeline

#### Дашборд Logs

Показывает централизованные логи из Loki. Текущий набор панелей может включать:

- API logs
- API webhook logs
- Worker logs
- Worker failed messages
- Fiscal poller logs
- Fiscal messages
- All errors

Конкретный состав панелей зависит от экспортированного JSON дашборда, но логически Logs dashboard покрывает API, worker, fiscal poller и сводку ошибок.

Дашборды могут быть экспортированы в JSON и сохранены в репозитории как dashboard artifacts.

---

### 9. Alerts: Prometheus rules + Telegram + email

В проекте одновременно реализованы два уровня alerting:

1. Prometheus alert rules
2. отдельный `alert_worker` с доставкой в Telegram и email

#### Базовые alert rules

Для Prometheus добавлены базовые alert rules.

Они покрывают следующие сценарии:

- `IntegrationWorkerHeartbeatStale`
- `IntegrationApiErrorRateHigh`
- `IntegrationApiLatencyP95High`
- `IntegrationFiscalPollerPendingChecksHigh`
- `IntegrationWorkerRetryOrFailedActivity`

Rules подключаются через файл:

```text
observability/prometheus/alerts.yml
```

И оцениваются Prometheus автоматически.

#### Alert worker

`python -m app.workers.alert_worker`

Alert worker работает отдельно от основного `worker` и не вмешивается в обработку событий.

Он периодически читает состояние системы напрямую из БД и отправляет уведомления в Telegram и/или на email в зависимости от конфигурации.

#### Что проверяется alert worker

Реализованы четыре типа сигналов:

- `worker stale` или отсутствие heartbeat
- наличие событий со статусом `FAILED`
- наличие событий со статусом `RETRY`
- наличие ошибок синхронизации остатков в `stock_sync_status`

#### Alert flow

Alert worker:

1. читает heartbeat основного worker из `service_heartbeats`
2. считает количество событий `FAILED` в `event_store`
3. считает количество событий `RETRY` в `event_store`
4. считает количество ошибок синхронизации остатков в `stock_sync_status`
5. строит текущее состояние alert snapshot
6. сравнивает его с предыдущим состоянием
7. отправляет alert или recovery сообщение по доступным каналам доставки только при смене состояния

#### Anti-spam

Чтобы не отправлять одинаковые уведомления на каждой итерации цикла, используется простая защита от спама по состоянию.

Alert отправляется только при переходе:

- `ok → stale`
- `FAILED = 0 → FAILED > 0`
- `RETRY = 0 → RETRY > 0`
- `stock sync errors = 0 → stock sync errors > 0`

Recovery отправляется только при переходе:

- `stale → ok`
- `FAILED > 0 → FAILED = 0`
- `RETRY > 0 → RETRY = 0`
- `stock sync errors > 0 → stock sync errors = 0`

На первом цикле alert worker только фиксирует baseline без отправки сообщений.

#### Каналы доставки

Поддерживаются два канала доставки уведомлений:

- Telegram через Bot API
- email через SMTP

Оба канала могут работать одновременно. Если настроен только один канал, alert worker продолжает работать через него.

#### Alert messages

Поддерживаются следующие типы уведомлений:

- alert по проблеме с heartbeat worker
- recovery по восстановлению worker
- alert по появлению `FAILED` событий
- recovery по очистке `FAILED` событий
- alert по появлению `RETRY` событий
- recovery по очистке `RETRY` событий
- alert по появлению ошибок синхронизации остатков
- recovery по очистке ошибок синхронизации остатков

#### Проверенные сценарии

Alerts были проверены на следующих сценариях:

- остановка основного worker и переход в `stale`
- восстановление worker и возврат в `ok`
- появление `FAILED` события
- очистка `FAILED` событий и recovery
- появление `RETRY` события
- очистка `RETRY` событий и recovery
- появление ошибки синхронизации остатков
- очистка ошибки синхронизации остатков и recovery
- доставка уведомлений одновременно в Telegram и на email
- успешная отправка alert и recovery писем через SMTP

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
- `/monitoring`
- `/dashboard`

#### Какие ручки публичные

Без admin auth остаются:

- `/health`
- `/metrics`
- `/webhooks/evotor`
- `/webhooks/evotor/{tenant_id}`
- `/webhooks/moysklad/{tenant_id}`
- `/api/v1/user/token`
- onboarding-ручки `/onboarding/...`

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
  -H "Authorization: Bearer token"
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

### Fiscal poller

```text
fiscalization_checks → fiscal_poller.py → fiscalization24 get_check_state → update fiscalization_checks
```

### Monitoring + metrics + logs + alerts

```text
event_store / errors / service_heartbeats / fiscalization_checks / stock_sync_status
  → monitoring.py / Prometheus metrics / JSON logs
  → Grafana / Loki / Prometheus rules / alert_worker
```

Контуры monitoring, metrics, logs и alerting не вмешиваются в обработку бизнес-событий и не меняют pipeline integration bus.

Они используют уже существующие данные:

- `event_store`
- `errors`
- `service_heartbeats`
- `fiscalization_checks`
- `stock_sync_status`

Тем самым observability является отдельным слоем наблюдаемости поверх существующей архитектуры.

### Alerts: Telegram + email

```text
service_heartbeats / event_store / stock_sync_status
  → alert_worker.py
  → alert_logic.py
  → telegram_client.py / email_client.py
  → Telegram Bot API / SMTP
```

Контур alerting работает отдельно от основного `worker` и не влияет на обработку продаж, товаров, остатков и фискализации.

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
| GET | `/metrics` | Prometheus-метрики API |

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
| POST | `/webhooks/evotor` | Принять webhook от Эвотор с авто-резолвом tenant |
| POST | `/webhooks/evotor/{tenant_id}` | Принять webhook от Эвотор с явным tenant |
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

### Onboarding

| Метод | URL | Описание |
|---|---|---|
| GET | `/onboarding/evotor/connect` | HTML-форма подключения Эвотор по token |
| POST | `/onboarding/evotor/connect` | Получить список магазинов Эвотор |
| GET | `/onboarding/evotor/sessions/{session_id}/stores` | Выбрать магазин Эвотор |
| GET | `/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token` | Форма ввода token МойСклад |
| POST | `/onboarding/evotor/sessions/{session_id}/stores/{store_id}/ms-token` | Загрузить организации, склады и контрагентов МойСклад |
| GET | `/onboarding/evotor/sessions/{session_id}/stores/{store_id}/configure` | Форма выбора org/store/agent |
| POST | `/onboarding/evotor/sessions/{session_id}/stores/{store_id}/configure` | Создать tenant и сохранить конфигурацию магазина |

### Monitoring

| Метод | URL | Описание |
|---|---|---|
| GET | `/monitoring/dashboard` | JSON snapshot состояния integration bus |
| GET | `/dashboard` | HTML dashboard мониторинга |

---

## Требования

- Python 3.11+
- Docker Desktop
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
ADMIN_API_TOKEN=token

TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

SMTP_HOST=smtp.mail.ru
SMTP_PORT=465
SMTP_USERNAME=your_mail_login@mail.ru
SMTP_PASSWORD=your_external_app_password
SMTP_FROM=your_mail_login@mail.ru
ALERT_EMAIL_TO=recipient@example.com
SMTP_USE_SSL=true
SMTP_USE_TLS=false

ALERT_POLL_INTERVAL_SEC=30
WORKER_STALE_AFTER_SEC=30

WORKER_METRICS_HOST=0.0.0.0
WORKER_METRICS_PORT=8001
FISCAL_POLLER_METRICS_HOST=0.0.0.0
FISCAL_POLLER_METRICS_PORT=8002
```

### 4. Инициализировать БД

```bash
python -m app.scripts.init_db
```

---

## Запуск

### API сервер

Для локальной разработки:

```bash
uvicorn app.main:app --reload
```

Для observability-контура с логированием в файл:

```bash
LOG_TO_FILE=true
LOG_FILE_PATH=logs/api.log
SERVICE_NAME=integration-api
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Swagger:
`http://127.0.0.1:8000/docs`

### Worker

```bash
LOG_TO_FILE=true
LOG_FILE_PATH=logs/worker.log
SERVICE_NAME=integration-worker
python -m app.workers.worker
```

### Fiscal poller

```bash
LOG_TO_FILE=true
LOG_FILE_PATH=logs/fiscal_poller.log
SERVICE_NAME=integration-fiscal-poller
python -m app.workers.fiscal_poller
```

### Alert worker

```bash
python -m app.workers.alert_worker
```

По умолчанию alert worker можно запускать без file logging — тогда его логи останутся в stdout. Если нужно собирать их через Loki, включи `LOG_TO_FILE=true`, задай отдельный `LOG_FILE_PATH` и добавь этот файл в Promtail.

Alert worker использует настроенные каналы доставки:

- Telegram, если заданы `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`
- email, если заданы SMTP-параметры

Если настроены оба канала, уведомления отправляются и в Telegram, и на email.

### Observability stack

```bash
docker compose -f docker-compose.observability.yml up -d
```

Поднимаются:

- `prometheus`
- `grafana`
- `loki`
- `promtail`

Проверка:

```bash
docker compose -f docker-compose.observability.yml ps
```

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

Grafana по умолчанию:

- `http://127.0.0.1:3000`

Prometheus:

- `http://127.0.0.1:9090`

Loki ready endpoint:

- `http://127.0.0.1:3100/ready`

Promtail targets:

- `http://127.0.0.1:9080/targets`

---

## Demo scripts for dashboard

Для локальной проверки dashboard и демонстрации monitoring-сценариев могут использоваться вспомогательные demo-скрипты, которые вставляют тестовые записи в `event_store` и `errors`.

С их помощью можно проверить отображение:

- `DONE`
- `FAILED`
- `RETRY`
- `latency`
- `Problem Events`
- `Recent Errors`

Эти скрипты не являются unit-тестами и используются только для ручной демонстрации dashboard.

---

## Demo scripts for alerts

Для локальной проверки alerts могут использоваться вспомогательные demo-скрипты, которые создают и удаляют тестовые записи в `event_store` и `stock_sync_status`.

С их помощью можно проверить:

- alert и recovery по `FAILED` событиям
- alert и recovery по `RETRY` событиям
- alert и recovery по ошибкам синхронизации остатков

Дополнительно для Prometheus rules можно проверять:

- `/api/v1/rules`
- `/api/v1/alerts`

на стороне Prometheus.

---

## Базовый сценарий настройки

1. Создать tenant вручную через API **или** пройти HTML onboarding `/onboarding/evotor/connect`
2. Настроить МойСклад и Эвотор
3. Сохранить токен Эвотор через `/api/v1/user/token` или завершить onboarding-цепочку до создания tenant
4. Выполнить `POST /sync/{tenant_id}/initial`
5. Настроить реквизиты фискализации через `PATCH /tenants/{tenant_id}/fiscal`
6. Проверить статус через `GET /sync/{tenant_id}/status`
7. Поднять observability stack через `docker compose -f docker-compose.observability.yml up -d`
8. Запустить `worker`, `fiscal_poller` и при необходимости `alert_worker`

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

### Проверить worker metrics

```bash
curl "http://127.0.0.1:8001/metrics"
```

### Проверить fiscal poller metrics

```bash
curl "http://127.0.0.1:8002/metrics"
```

---

## Дальнейшее развитие

- поддержка card/mixed payment в фискализации
- dashboard provisioning из репозитория как dashboard-as-code, если он ещё не включён в локальном окружении
- дополнительные Grafana alert delivery / Alertmanager при необходимости
- расширение dashboard: фильтры, цветовая индикация, дополнительные метрики
- дополнительные E2E-тесты
- верификация webhook МойСклад (эндпоинт сейчас публичный)
