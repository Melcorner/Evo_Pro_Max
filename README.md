# Evotor ↔ MoySklad Integration Bus

Интеграционная шина между **Эвотор** и **МойСклад**. Доступна как решение в каталоге МойСклад.

Проект решает шесть основных задач:

1. Приём продаж из Эвотор и формирование документов в МойСклад
2. Синхронизация товаров и остатков между МойСклад и Эвотор
3. Автоматическое обновление остатков по webhook от МойСклад
4. Фискализация документа **Отгрузка** из МойСклад через **Универсальный фискализатор**
5. Публикация в каталоге решений МойСклад с онбордингом через iframe
6. Наблюдаемость: метрики Prometheus, дашборды Grafana, логи Loki

---

## Что реализовано

### 1. Продажи Эвотор → МойСклад

- приём webhook событий от Эвотор
- поддержка формата `ReceiptCreated`
- нормализация payload продажи
- сохранение события в `event_store`
- обработка через `worker`
- маппинг товаров `evotor_id -> ms_id`
- создание документа **Отгрузка** в МойСклад

#### Покупатель

- поиск контрагента по `email`, затем по `phone`
- создание нового контрагента
- fallback на `default agent`, если buyer data отсутствует

#### Скидки

Поддерживаются production-поля (`discount`, `totalDiscount`) и enriched-поля (`resultPrice`, `resultSum`, `positionDiscount.discountPercent`). Скидка передаётся процентным полем `discount`, базовая цена позиции не искажается.

#### НДС

`taxPercent = 0/10/20` → соответствующие `vat` и `vatEnabled` в МойСклад.

---

### 2. Товары и остатки МойСклад ↔ Эвотор

#### Initial sync

`POST /sync/{tenant_id}/initial` — первичная синхронизация Эвотор → МойСклад.

#### Синхронизация МойСклад → Эвотор

`POST /sync/{tenant_id}/ms-to-evotor` — синхронизирует все товары из МойСклад в Эвотор. Новые создаются, существующие обновляются.

#### Маппинг типов маркировки

Поддерживаются все актуальные типы: MILK, TOBACCO, SHOES, LP_CLOTHES, LP_LINENS, PERFUMERY, ELECTRONICS, TIRES, CAMERA_PHOTO, WATER, OTP, BICYCLE, WHEELCHAIRS, ALCOHOL, MEDICINE, NABEER, NICOTINE, FOOD_SUPPLEMENT, ANTISEPTIC, MEDICAL_DEVICES, SOFT_DRINKS, VETPHARMA, SEAFOOD, VEGETABLE_OIL, ANIMAL_FOOD, MOTOR_OIL, GROCERIES, COSMETICS, FUR, NOT_TRACKED.

#### Синхронизация остатков

- одиночная: `POST /sync/{tenant_id}/stock/{ms_product_id}`
- массовая: `POST /sync/{tenant_id}/stock/reconcile`
- статус: `GET /sync/{tenant_id}/stock/status`

---

### 3. Автоматическая синхронизация остатков

Webhook от МойСклад обновляет остатки в Эвотор при изменении документов `demand`, `supply`, `inventory`, `loss`, `enter`.

---

### 4. Фискализация документа МойСклад

**МойСклад demand → fiscalization24 → касса Эвотор**

Для фискализации нужны: `fiscal_token`, `fiscal_client_uid`, `fiscal_device_uid`. Настраиваются через онбординг или `PATCH /tenants/{tenant_id}/fiscal`.

**Текущее ограничение MVP:** чек уходит как наличная оплата (`payCashSumma = сумма чека`).

---

### 5. Каталог решений МойСклад

#### Vendor API

- `PUT /vendor/api/moysklad/vendor/1.0/apps/{appId}/{accountId}` — activate/suspend/resume
- `DELETE /vendor/api/moysklad/vendor/1.0/apps/{appId}/{accountId}` — delete

При установке МойСклад передаёт токен доступа к JSON API — сохраняется в tenant автоматически.

#### Дескриптор

`descriptor.xml` с namespace `https://apps-api.moysklad.ru/xml/ns/appstore/app/v2`.

#### Онбординг из iframe МойСклад

1. МойСклад открывает iframe, передаёт `contextKey` и `appId`
2. Система находит tenant по `ms_account_id`
3. Если Эвотор не подключён — форма подключения
4. Пользователь вводит только **Evotor token** (МойСклад токен уже есть)
5. Система загружает организации/склады/контрагентов из МойСклад
6. Запускается первичная синхронизация

#### Личный кабинет

`/onboarding/tenants/{tenant_id}` — 4 вкладки: Обзор, Интеграция, Уведомления, Действия.

---

### 6. Alerts: Telegram + email

`alert_worker.py` — tenant-aware уведомления по 4 типам сигналов: worker stale, FAILED события, RETRY события, ошибки stock sync. Отправляется только при смене состояния (anti-spam). Доставка журналируется в `notification_log`.

Telegram подключается через deep link: `https://t.me/<BOT>?start=tglink_<token>`.

---

### 7. Observability: Prometheus + Grafana + Loki

- **Метрики:** `/metrics` (API), `:8001/metrics` (Worker), `:8002/metrics` (Fiscal poller)
- **Grafana:** `/grafana/` — дашборды API, Worker, Fiscal poller, Logs
- **Логи:** systemd drop-in → `logs/*.log` → Promtail → Loki → Grafana

---

## Безопасность

```env
EVOTOR_WEBHOOK_SECRET=    # Bearer токен для webhook Эвотор
MS_WEBHOOK_SECRET=         # HMAC-SHA256 для webhook МойСклад
ADMIN_API_TOKEN=           # Bearer для admin/internal API (опционально)
```

Публичные ручки (без авторизации): `/health`, `/metrics`, `/webhooks/*`, `/onboarding/*`, `/vendor/*`.

---

## Архитектура

```text
# Продажи
Эвотор Webhook → Event Store → Worker → Sale Handler → МойСклад API

# Товары
sync.py → MoySklad API / Evotor API → mappings

# Автоматические остатки
МойСклад Webhook → moysklad_webhooks.py → /report/stock/all → Evotor API

# Каталог МойСклад
Vendor API → tenant (с токеном МС) → iframe → Evotor connect → sync

# Observability
logs/*.log → Promtail → Loki → Grafana
/metrics → Prometheus → Grafana

# Alerts
event_store → alert_worker.py → tenant Telegram/Email → notification_log
```

---

## Основные таблицы

| Таблица | Назначение |
|---|---|
| `tenants` | Tenant'ы и конфигурация интеграции |
| `event_store` | Очередь входящих событий |
| `processed_events` | Идемпотентность обработки |
| `mappings` | Связи `evotor_id ↔ ms_id` |
| `stock_sync_status` | Статус синхронизации остатков |
| `fiscalization_checks` | Отправленные чеки и их статусы |
| `service_heartbeats` | Heartbeat фоновых сервисов |
| `notification_log` | Журнал уведомлений |
| `telegram_link_tokens` | Токены привязки Telegram |
| `evotor_onboarding_sessions` | Сессии ручного онбординга |

---

## Требования

- Python 3.12+
- PostgreSQL 14+
- Docker + Docker Compose (для observability)
- Аккаунт разработчика МойСклад

---

## Установка

```bash
# 1. Виртуальное окружение
python3.12 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. База данных
sudo -u postgres psql -c "CREATE USER evotor WITH PASSWORD 'password';"
sudo -u postgres psql -c "CREATE DATABASE evotor_ms OWNER evotor;"
python -m app.scripts.init_db

# 3. Настройка логирования для Grafana
cp deploy/systemd/evo-api-logging.conf /etc/systemd/system/evo-api.service.d/logging.conf
cp deploy/systemd/evo-worker-logging.conf /etc/systemd/system/evo-worker.service.d/logging.conf
cp deploy/systemd/evo-fiscal-poller-logging.conf /etc/systemd/system/evo-fiscal-poller.service.d/logging.conf
systemctl daemon-reload
```

### .env

```env
DATABASE_URL=postgresql://user:password@localhost:5432/evotor_ms
MS_APP_ID=your_app_id
MS_VENDOR_SECRET_KEY=your_vendor_secret
EVOTOR_WEBHOOK_SECRET=your_secret
MS_WEBHOOK_SECRET=your_secret
ADMIN_API_TOKEN=token
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_BOT_USERNAME=your_bot_username
TELEGRAM_CHAT_ID=your_chat_id
SMTP_HOST=smtp.mail.ru
SMTP_PORT=465
SMTP_USERNAME=your@mail.ru
SMTP_PASSWORD=your_password
SMTP_FROM=your@mail.ru
ALERT_EMAIL_TO=recipient@example.com
SMTP_USE_SSL=true
ALERT_POLL_INTERVAL_SEC=30
```

---

## Запуск

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000   # API
python -m app.workers.worker                        # Worker
python -m app.workers.fiscal_poller                 # Fiscal poller
python -m app.workers.alert_worker                  # Alert worker
docker compose -f docker-compose.observability.yml up -d  # Observability
```

---

## Тесты

```bash
./run_tests.sh                                          # SQLite + PostgreSQL smoke
DATABASE_URL=sqlite:///tmp/test.db python -m pytest tests/ -v  # Только SQLite
RUN_PG_RUNTIME_SMOKE=1 python -m pytest tests/test_pg_runtime_smoke.py -v  # Только PG
```

---

## Онбординг

### Через каталог МойСклад (рекомендуется)

1. Установить решение из каталога МойСклад
2. МойСклад откроет iframe с формой подключения Эвотор
3. Ввести Evotor token — система загрузит магазины и данные МойСклад
4. Выбрать магазин, организацию, склад, контрагента
5. Запустится первичная синхронизация
6. Подключить Telegram в разделе Уведомления

### Вручную

1. Открыть `/onboarding/evotor/connect`
2. Ввести Evotor token → выбрать магазин
3. Ввести MoySklad token → настроить параметры → создать профиль

---

## Дальнейшее развитие

- поддержка card/mixed payment в фискализации
- получение `accountId` из `contextKey` МойСклад (ожидаем endpoint от поддержки МойСклад)
- E2E-тесты на PostgreSQL