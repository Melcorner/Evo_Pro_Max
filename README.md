# Integration Bus POC

Прототип интеграционной шины между кассой (**Evotor**) и учётной системой (**MoySklad**).

Проект реализует event-driven pipeline:

`Webhook → Event Store → Worker → Dispatch → Handler → Mapper → MoySklad API`

Цель — показать надёжную обработку событий с:
- идемпотентностью
- повторными попытками
- классификацией ошибок
- журналированием ошибок
- базовой интеграцией с MoySklad

---

## 🚀 Возможности проекта

- Приём webhook событий
- Хранение событий в `event_store`
- Exactly-once обработка через `processed_events`
- Worker с optimistic locking
- Retry с exponential backoff
- Перевод в `FAILED` после лимита попыток
- Классификация ошибок `RETRY / FAILED`
- Запись ошибок в таблицу `errors`
- Сохранение `payload_snapshot` для диагностики
- Dispatch событий через `event_dispatcher`
- Базовый client для MoySklad API
- Базовый sale-handler и sale-mapper
- Mapping storage (`mapping_store`)
- E2E тест сценария
- Структурированное логирование

---

## 🧱 Архитектура

### Ingest Layer
Принимает webhook и сохраняет событие в `event_store` со статусом `NEW`.

### Sync Worker
Фоновый процесс, который:
1. Берёт события `NEW` или `RETRY`
2. Переводит их в `PROCESSING`
3. Вызывает `dispatch_event(...)`
4. Помечает событие как `DONE`, `RETRY` или `FAILED`

### Dispatch Layer
`event_dispatcher.py` определяет, какой use-case должен обработать событие по `event_type`.

### Handler Layer
Например, `sale_handler.py` отвечает за orchestration конкретного сценария (`sale`).

### Mapper Layer
`sale_mapper.py` подготавливает payload для MoySklad.  
`mapping_store.py` хранит соответствия между идентификаторами Evotor и MoySklad.

### Idempotency Layer
Таблица `processed_events` гарантирует, что одно и то же событие не будет обработано дважды.

---

## ⚠️ Обработка ошибок и orchestration

### Классификация ошибок

В проект добавлен helper `classify_error(e)`, который разделяет ошибки на два класса.

**RETRY**
- timeout
- connection error
- HTTP 429
- HTTP 5xx

**FAILED**
- HTTP 400
- HTTP 401
- HTTP 403
- HTTP 422

Это позволяет worker принимать корректное решение:
- повторять событие при временной ошибке
- завершать событие как `FAILED` при фатальной ошибке

---

### Таблица `errors`

Добавлена отдельная таблица `errors`, в которую сохраняются ошибки обработки событий.

Для каждой ошибки сохраняются:
- `event_id`
- `tenant_id`
- `error_code`
- `message`
- `payload_snapshot`
- `created_at`

Это позволяет:
- разбирать причины сбоев
- анализировать payload, на котором упала обработка
- хранить историю ошибок отдельно от `event_store`

---

### Dispatch событий

Worker больше не содержит бизнес-логики обработки типов событий.

Теперь worker отвечает только за orchestration:
1. выбор события из `event_store`
2. перевод в `PROCESSING`
3. вызов dispatch
4. перевод в `DONE / RETRY / FAILED`
5. запись в `processed_events`
6. запись в `errors`

Это упрощает расширение системы под новые use-case:
- `sale`
- `product`
- `stock`

---

## 📦 Структура проекта

```text
integration-bus/
├── app/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── events.py
│   │   ├── mappings.py
│   │   ├── tenants.py
│   │   └── webhooks.py
│   ├── clients/
│   │   ├── __init__.py
│   │   └── moysklad_client.py
│   ├── handlers/
│   │   ├── __init__.py
│   │   └── sale_handler.py
│   ├── mappers/
│   │   ├── __init__.py
│   │   └── sale_mapper.py
│   ├── scripts/
│   │   ├── __init__.py
│   │   └── init_db.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── error_logic.py
│   │   └── event_dispatcher.py
│   ├── stores/
│   │   ├── __init__.py
│   │   ├── error_store.py
│   │   └── mapping_store.py
│   ├── workers/
│   │   ├── __init__.py
│   │   └── worker.py
│   ├── __init__.py
│   ├── db.py
│   ├── logger.py
│   └── main.py
├── data/
├── tests/
│   ├── e2e_test.py
│   ├── test_db.py
│   └── test_sale.py
├── venv/
├── .gitignore
├── README.md
└── requirements.txt
```

---

## ⚙️ Требования

- Python 3.11+
- macOS / Linux / Windows

---

## 🔧 Установка и запуск

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

Ожидаемый вывод:

```text
DB initialized
```

---

## ▶️ Запуск проекта

Проект запускается в трёх терминалах.

### Терминал 1 — API сервер

```bash
uvicorn app.main:app --reload
```

Swagger будет доступен по адресу:

```text
http://127.0.0.1:8000/docs
```

### Терминал 2 — Worker

**macOS / Linux**

```bash
export MS_BASE_URL=https://httpbin.org
python -m app.workers.worker
```

**Windows PowerShell**

```powershell
$env:MS_BASE_URL="https://httpbin.org"
python -m app.workers.worker
```

Worker начнёт обрабатывать события.

### Терминал 3 — E2E тест (опционально)

```bash
python tests/e2e_test.py
```

Ожидаемый результат:

```text
✅ E2E OK: DONE + processed_events
```

---

## 🧪 Ручное тестирование webhook

Открыть Swagger:

```text
http://127.0.0.1:8000/docs
```

### Шаги
1. Создать tenant через `POST /tenants`
2. Скопировать `tenant_id`
3. Отправить webhook через `POST /webhooks/evotor/{tenant_id}`

### Пример body

```json
{
  "type": "sale",
  "event_id": "sale-001",
  "amount": 100
}
```

---

## 🔄 Жизненный цикл события

```text
NEW → PROCESSING → DONE
                ↘
                RETRY → FAILED
```

---

## ♻️ Retry политика

- exponential backoff: `1m, 2m, 4m, 8m, 16m`
- максимум `5` попыток
- после лимита событие переводится в `FAILED`

---

## 🧾 Логирование

Формат логов:

```text
timestamp | level | logger | message
```

Логи пишутся в stdout.

Основные логгеры:
- `api`
- `worker`
- `sale_handler`
- `sale_mapper`
- `moysklad`

---

## 🛠️ Полезные эндпоинты

### Проверка сервера

```text
GET /health
```

### Работа с tenant'ами

```text
POST /tenants
GET /tenants
```

### Webhook

```text
POST /webhooks/evotor/{tenant_id}
```

### Диагностика событий

```text
GET /events
GET /events/retry
GET /processed
```

### Mappings

```text
GET /mappings
POST /mappings
DELETE /mappings
```

---

## Тестовый сценарий обработки

Текущий end-to-end сценарий:

```text
1. Создание tenant
2. Отправка webhook
3. Сохранение события в event_store
4. Захват события worker'ом
5. Обработка события через sale_handler
6. Преобразование данных через sale_mapper
7. Отправка данных через moysklad_client
8. Запись результата в processed_events
9. Перевод события в DONE
```

---

## ✅ Текущее состояние

На текущем этапе проект уже умеет:

- принимать webhook
- сохранять события в `event_store`
- обрабатывать события worker'ом
- классифицировать ошибки как `RETRY / FAILED`
- сохранять ошибки в таблицу `errors`
- отправлять базовый sale payload в MoySklad API
- использовать test mode через `httpbin`
- хранить mappings между Evotor и MoySklad

---

## 🔮 Дальнейшее развитие

Планируемые улучшения:

- Sale payload с позициями и `syncId`
- Использование `mapping_store` внутри `sale_handler / sale_mapper`
- Поддержка сценариев `product` и `stock`
- Метрики и мониторинг
- Batch processing
- Dead-letter queue
- Dockerization
- Дополнительные диагностические endpoints (`/events`, `/events/{id}`, `/errors`)
