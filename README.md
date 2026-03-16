# МойСклад: синхронизация продаж

Интеграционная шина между кассой **Эвотор** и учётной системой **МойСклад**.

Реализует event-driven pipeline:

```
Эвотор Webhook → Event Store → Worker → Dispatch → Handler → Mapper → МойСклад API
```

---

## Возможности

- Приём webhook событий от Эвотор: старый формат (`SELL`) и новый формат (`ReceiptCreated`)
- Хранение событий в `event_store` с идемпотентностью через `processed_events`
- Worker с optimistic locking и exponential backoff retry
- Классификация ошибок `RETRY / FAILED`
- Маппинг Эвотор product_id → МойСклад product_id через `MappingStore`
- Формирование `assortment.meta.href` для МойСклад API
- Создание документа **Отгрузка** (`entity/demand`) в МойСклад при каждой продаже
- Сохранение токена облака Эвотор при установке приложения
- Ручная повторная обработка FAILED событий через `/requeue`
- REST API для диагностики событий и ошибок
- Протестировано на реальных API Эвотор и МойСклад — сквозной сценарий продажи работает

---

## Архитектура

### Ingest Layer

`POST /webhooks/evotor/{tenant_id}` принимает события от Эвотор и сохраняет в `event_store` со статусом `NEW`.

При получении события установки приложения (`userUuid` + `token`) — сохраняет токен облака Эвотор в `tenants`.

### Sync Worker

Фоновый процесс:

1. Выбирает событие `NEW` или `RETRY`
2. Переводит в `PROCESSING` (optimistic lock)
3. Вызывает `dispatch_event(row)`
4. Переводит в `DONE`, `RETRY` или `FAILED`
5. Записывает в `processed_events` (идемпотентность)
6. Записывает в `errors` при неудаче

### Dispatch Layer

`event_dispatcher.py` маршрутизирует событие по `event_type` к нужному handler'у.

| `event_type`       | Поведение                                                         |
| ------------------ | ----------------------------------------------------------------- |
| `sale`             | Передаётся в `handle_sale`                                        |
| `product`, `stock` | Логируется и пропускается (`DONE`, `result_ref=skipped:product`)  |
| Остальное          | `ValueError` → RETRY → FAILED                                     |

### Handler Layer

`sale_handler.py` оркестрирует сценарий продажи: вызывает маппер, отправляет в МойСклад.

Результат — документ **Отгрузка** (`POST /entity/demand`) в МойСклад. После успеха `id` созданного документа сохраняется как `result_ref` в `processed_events`.

### Mapper Layer

`sale_mapper.py` трансформирует payload формата Эвотор в формат МойСклад:

- принимает нативный формат Эвотор (`id`, `body.positions`)
- валидирует обязательные поля
- резолвит `evotor_id → ms_id` через `MappingStore`
- формирует `assortment.meta.href`
- конвертирует цены из рублей в копейки через `round()` (формат МойСклад)
- устанавливает `syncId`: если передан явный `sync_id` — использует его, иначе берёт `id` документа Эвотор

### Idempotency Layer

`processed_events` гарантирует exactly-once обработку по `event_key`.

---

## Формат событий Эвотор

Система поддерживает два формата webhook от Эвотор.

### Новый формат — `ReceiptCreated` (Чеки ver.2)

Актуальный формат, используемый в продакшне. Нормализуется в `webhooks.py` функцией `_normalize_receipt_created()`.

```json
{
  "type": "ReceiptCreated",
  "id": "20260314-...",
  "store_id": "20260314-3BF3-4021-8051-E3A278EE4974",
  "data": {
    "type": "SELL",
    "id": "03990165-9d5f-4841-a99a-083abc659f67",
    "storeId": "20260314-3BF3-4021-8051-E3A278EE4974",
    "totalAmount": 1.0,
    "items": [
      {
        "id": "bbb5b5a8-6e3d-45ff-b16d-18b95926cbc9",
        "name": "GP Alkaline AAx4",
        "quantity": 1,
        "price": 1.0,
        "sumPrice": 1.0
      }
    ]
  }
}
```

Поддерживается только `data.type = SELL`. Остальные подтипы (`PAYBACK` и др.) пропускаются со статусом `skipped`.

### Старый формат — `SELL`

```json
{
  "type": "SELL",
  "id": "03990165-9d5f-4841-a99a-083abc659f67",
  "store_id": "20260314-3BF3-4021-8051-E3A278EE4974",
  "device_id": "20260314-65DA-40F1-80EE-5109AB6E49F6",
  "body": {
    "positions": [
      {
        "product_id": "bbb5b5a8-6e3d-45ff-b16d-18b95926cbc9",
        "product_name": "GP Alkaline AAx4",
        "quantity": 1,
        "price": 1.0,
        "sum": 1.0
      }
    ],
    "sum": 1.0
  }
}
```

Оба формата нормализуются во внутренний формат и проходят одинаковый pipeline.

---

## Обработка ошибок

### Классификация

| Тип ошибки | Решение |
|---|---|
| Timeout / ConnectionError | RETRY |
| HTTP 429 | RETRY |
| HTTP 5xx | RETRY |
| HTTP 400 / 401 / 403 / 422 | FAILED |
| `SalePayloadError` (невалидный payload) | FAILED |
| `MappingNotFoundError` (нет маппинга) | FAILED |
| Остальные неизвестные | RETRY |

### Retry политика

- Exponential backoff: `1m → 2m → 4m → 8m → 16m`
- Максимум 5 попыток, затем → `FAILED`

### Таблица `errors`

При переходе в `FAILED` сохраняется запись с `event_id`, `tenant_id`, `error_code`, `message`, `payload_snapshot`, `response_body`.

---

## Жизненный цикл события

```
NEW → PROCESSING → DONE
               ↘
               RETRY (до 5 раз) → FAILED
                                       ↓
                                  requeue → NEW
```

---

## Структура проекта

```
integration-bus/
├── app/
│   ├── api/
│   │   ├── errors.py           — GET /errors
│   │   ├── evotor.py           — POST /api/v1/user/token и др. эндпоинты Эвотор
│   │   ├── events.py           — GET /events, GET /events/{id}, requeue
│   │   ├── mappings.py         — CRUD /mappings
│   │   ├── tenants.py          — CRUD /tenants
│   │   └── webhooks.py         — POST /webhooks/evotor/{tenant_id}
│   ├── clients/
│   │   └── moysklad_client.py
│   ├── handlers/
│   │   └── sale_handler.py
│   ├── mappers/
│   │   └── sale_mapper.py
│   ├── scripts/
│   │   └── init_db.py          — инициализация БД и миграции
│   ├── services/
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
├── tests/
│   ├── e2e_test.py
│   └── test_sale2.py
├── conftest.py
├── .gitignore
├── README.md
└── requirements.txt
```

---

## Требования

- Python 3.11+
- macOS / Linux / Windows

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

`init_db` идемпотентен — безопасно запускать повторно, применяет недостающие миграции.

---

## Запуск

### Терминал 1 — API сервер

```bash
uvicorn app.main:app --reload
```

Swagger: `http://127.0.0.1:8000/docs`

### Терминал 2 — Worker

macOS / Linux:
```bash
export MS_BASE_URL=https://httpbin.org
python -m app.workers.worker
```

Windows:
```powershell
$env:MS_BASE_URL="https://httpbin.org"
python -m app.workers.worker
```

---

## Тестирование

### E2E тест

```bash
python tests/e2e_test.py
```

Ожидаемый результат:
```
Tenant: <uuid>
Mappings registered: bbb5b5a8 -> ms-product-001, ccc5b5a8 -> ms-product-002
Sending webhook: e2e-<uuid>
✅ E2E OK: DONE + processed_events
```

### Unit тесты

```bash
pytest tests/test_sale2.py -v
```

---

## Интеграция с Эвотор

### Регистрация приложения

1. Зарегистрировать приложение на `dev.evotor.ru`
2. На вкладке **Интеграция** включить **Чеки (ver.2)** и указать URL:
   ```
   https://<your-server>/webhooks/evotor/{tenant_id}
   ```
3. Включить **Токен приложения для доступа к REST API Эвотор** и указать URL:
   ```
   https://<your-server>/api/v1/user/token
   ```
4. Перевести версию в тестирование

### Получение токена Эвотор вручную

1. На вкладке **Интеграция** включить **Создать вкладку Настройки**
2. Добавить текстовое поле со значением `${token}`
3. В Личном кабинете пользователя Эвотор → вкладка **Настройки** → скопировать токен

### Маппинг товаров

Перед началом работы необходимо создать маппинги товаров Эвотор → МойСклад:

```bash
POST /mappings/
{
  "tenant_id": "<tenant_id>",
  "entity_type": "product",
  "evotor_id": "<evotor_product_uuid>",
  "ms_id": "<moysklad_product_uuid>"
}
```

При отсутствии маппинга событие уйдёт в `FAILED` с ошибкой `MappingNotFoundError`.

---

## API эндпоинты

### Инфраструктура

| Метод | URL | Описание |
|---|---|---|
| GET | `/health` | Проверка сервера |

### Tenants

| Метод | URL | Описание |
|---|---|---|
| POST | `/tenants` | Создать tenant |
| GET | `/tenants` | Список tenants |

### Webhook

| Метод | URL | Описание |
|---|---|---|
| POST | `/webhooks/evotor/{tenant_id}` | Принять событие от Эвотор |

### Маппинги

| Метод | URL | Описание |
|---|---|---|
| GET | `/mappings` | Список маппингов |
| POST | `/mappings/` | Создать или обновить маппинг |
| DELETE | `/mappings/` | Удалить маппинг |

### Диагностика

| Метод | URL | Описание |
|---|---|---|
| GET | `/events` | Все события (последние 100) |
| GET | `/events/retry` | События в статусе RETRY |
| GET | `/events/failed` | События в статусе FAILED |
| GET | `/events/{id}` | Детали события |
| POST | `/events/{id}/requeue` | Перевести FAILED → NEW |
| GET | `/errors` | Журнал ошибок |

---

## Логирование

Формат: `timestamp | level | logger | message`

Основные логгеры: `api`, `worker`, `sale_handler`, `sale_mapper`, `moysklad`

---

## Дальнейшее развитие

- Поддержка сценариев `product` и `stock`
- Dockerization
- Метрики и мониторинг
- Dead-letter queue
- Batch processing
- Валидация подписи webhook Эвотор