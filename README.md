# Integration Bus POC

Прототип интеграционной шины между кассой (**Evotor**) и учётной системой (**MoySklad**).

Проект реализует event-driven pipeline:

`Webhook → Event Store → Worker → Dispatch → Handler → Mapper → MoySklad API`

---

## Возможности

- Приём и хранение webhook событий с идемпотентностью
- Worker с optimistic locking и exponential backoff retry
- Классификация ошибок `RETRY / FAILED`
- Маппинг Evotor ID → MoySklad ID через `mapping_store`
- Формирование `assortment.meta` для MoySklad API
- `MappingNotFoundError` при отсутствии маппинга
- Сохранение `response_body` ответа МойСклад при ошибках
- Ручная повторная обработка FAILED событий через `requeue`
- REST API для диагностики событий и ошибок
- E2E тест сценария

---

## Архитектура

### Ingest Layer

Webhook `POST /webhooks/evotor/{tenant_id}` принимает событие и сохраняет его в `event_store` со статусом `NEW`.

### Sync Worker

Фоновый процесс:

- Выбирает событие `NEW` или `RETRY`
- Переводит в `PROCESSING` (optimistic lock)
- Вызывает `dispatch_event(row)`
- Переводит в `DONE`, `RETRY` или `FAILED`
- Записывает в `processed_events` (идемпотентность)
- Записывает в `errors` при неудаче

### Dispatch Layer

`event_dispatcher.py` маршрутизирует событие по `event_type` к нужному handler'у.

### Handler Layer

`sale_handler.py` оркестрирует сценарий продажи: вызывает маппер, отправляет в МойСклад, логирует ошибки маппинга.

### Mapper Layer

`sale_mapper.py` трансформирует payload Evotor в формат МойСклад:

- валидирует обязательные поля (`event_id`, `positions`)
- разрешает `evotor_id → ms_id` через `MappingStore`
- формирует `assortment.meta` с `href` / `type` / `mediaType`
- вычисляет `sum` по позициям
- устанавливает `syncId = event_id`

### Idempotency Layer

`processed_events` гарантирует exactly-once обработку.

---

## Обработка ошибок

### Классификация

| Тип ошибки                              | Решение |
| --------------------------------------- | ------- |
| Timeout / ConnectionError               | RETRY   |
| HTTP 429                                | RETRY   |
| HTTP 5xx                                | RETRY   |
| HTTP 400 / 401 / 403 / 422              | FAILED  |
| `SalePayloadError` (невалидный payload) | FAILED  |
| `MappingNotFoundError` (нет маппинга)   | FAILED  |
| Остальные неизвестные                   | RETRY   |

### Retry политика

- Exponential backoff: `1m → 2m → 4m → 8m → 16m`
- Максимум 5 попыток, затем → `FAILED`

### Таблица `errors`

При переходе в `FAILED` сохраняется запись с:

- `event_id`, `tenant_id`
- `error_code` — HTTP статус (если есть)
- `message` — текст ошибки
- `payload_snapshot` — копия payload на момент ошибки
- `response_body` — тело ответа МойСклад (если есть)

---

## Жизненный цикл события

```text
NEW → PROCESSING → DONE
                ↘
                RETRY (до 5 раз) → FAILED
                                        ↓
                                   requeue → NEW
```

---

## Структура проекта

```text
integration-bus/
├── app/
│   ├── api/
│   │   ├── errors.py       — GET /errors
│   │   ├── events.py       — GET /events, GET /events/{id}, POST /events/{id}/requeue
│   │   ├── mappings.py     — GET/POST/DELETE /mappings
│   │   ├── tenants.py      — CRUD /tenants
│   │   └── webhooks.py     — POST /webhooks/evotor/{tenant_id}
│   ├── clients/
│   │   └── moysklad_client.py
│   ├── handlers/
│   │   └── sale_handler.py
│   ├── mappers/
│   │   └── sale_mapper.py
│   ├── scripts/
│   │   └── init_db.py      — инициализация и миграции БД
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
│   ├── test_db.py
│   ├── test_sale.py
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

Проект запускается в двух терминалах.

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

Windows PowerShell:

```powershell
$env:MS_BASE_URL="https://httpbin.org"
python -m app.workers.worker
```

---

## E2E тест

Перед запуском убедитесь, что API сервер и worker запущены.

```bash
python tests/e2e_test.py
```

Тест автоматически:

1. Создаёт tenant
2. Регистрирует маппинги для `p1` и `p2`
3. Отправляет webhook с двумя позициями
4. Ждёт перехода события в `DONE`
5. Проверяет запись в `processed_events`

Ожидаемый результат:

```text
Tenant: <uuid>
Mappings registered: p1, p2
Sending webhook: e2e-<uuid>
✅ E2E OK: DONE + processed_events
```

---

## Unit тесты

```bash
pytest tests/test_sale.py tests/test_sale2.py -v
```

---

## API эндпоинты

### Инфраструктура

| Метод | URL       | Описание          |
| ----- | --------- | ----------------- |
| GET   | `/health` | Проверка сервера  |

### Tenants

| Метод | URL        | Описание         |
| ----- | ---------- | ---------------- |
| POST  | `/tenants` | Создать tenant   |
| GET   | `/tenants` | Список tenants   |

### Webhook

| Метод | URL                              | Описание                      |
| ----- | -------------------------------- | ----------------------------- |
| POST  | `/webhooks/evotor/{tenant_id}`   | Принять событие от Evotor     |

### Маппинги

| Метод  | URL          | Описание                                                |
| ------ | ------------ | ------------------------------------------------------- |
| GET    | `/mappings`  | Список маппингов (фильтр по `tenant_id`, `entity_type`) |
| POST   | `/mappings/` | Создать или обновить маппинг                            |
| DELETE | `/mappings/` | Удалить маппинг                                         |

### Диагностика событий

| Метод | URL                         | Описание                        |
| ----- | --------------------------- | ------------------------------- |
| GET   | `/events`                   | Все события (последние 100)     |
| GET   | `/events/retry`             | События в статусе RETRY         |
| GET   | `/events/failed`            | События в статусе FAILED        |
| GET   | `/events/{id}`              | Детали события                  |
| POST  | `/events/{id}/requeue`      | Перевести FAILED → NEW          |

### Ошибки

| Метод | URL       | Описание                                             |
| ----- | --------- | ---------------------------------------------------- |
| GET   | `/errors` | Журнал ошибок (параметры: `limit`, `offset`)         |

---

## Логирование

Формат: `timestamp | level | logger | message`

Основные логгеры: `api`, `worker`, `sale_handler`, `sale_mapper`, `moysklad`

---

## Дальнейшее развитие

- Поддержка сценариев `product` и `stock`
- Метрики и мониторинг
- Dockerization
- Dead-letter queue
- Batch processing
