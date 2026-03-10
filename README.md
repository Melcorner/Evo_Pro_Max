# Integration Bus POC

Прототип интеграционной шины между кассой (Evotor) и учётной системой (MoySklad).

Проект реализует базовый event-driven pipeline:

    Webhook → Event Store → Worker → Retry → Processed Events

Цель — показать надёжную обработку событий с идемпотентностью и повторными попытками.

---

## 🚀 Возможности проекта
 - Приём webhook событий
 - Хранение событий в event_store
 - Exactly-once обработка через processed_events
 - Worker с optimistic locking
 - Retry с exponential backoff
 - Перевод в FAILED после лимита попыток
 - Классификация ошибок RETRY / FAILED
 - Запись ошибок в таблицу errors
 - Сохранение payload_snapshot для диагностики
 - Dispatch событий через event_dispatcher
 - E2E тест сценария
 - Структурированное логирование
---

## 🧱 Архитектура

**Ingest Layer**

Принимает webhook и сохраняет событие в event_store со статусом NEW.

**Sync Worker**

Фоновый процесс, который:
 1. Берёт события NEW или RETRY
 2. Переводит в PROCESSING
 3. Выполняет обработку
 4. Помечает DONE или RETRY/FAILED

**Idempotency Layer**

Таблица processed_events гарантирует, что одно событие не будет обработано дважды.

---
## ⚠️ Обработка ошибок и orchestration

### Классификация ошибок

В проект добавлен helper `classify_error(e)`, который разделяет ошибки на два класса:

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
- завершать событие как FAILED при фатальной ошибке

---

### Таблица errors

Добавлена отдельная таблица `errors`, в которую сохраняются ошибки обработки событий.

Для каждой ошибки сохраняются:
- `event_id`
- `tenant_id`
- `error_code`
- `message`
- `payload_snapshot`
- `created_at`

Это даёт возможность:
- разбирать причины сбоев
- анализировать payload, на котором упала обработка
- хранить историю ошибок отдельно от event_store

---

### Dispatch событий

Worker больше не содержит бизнес-логики обработки типов событий.

Теперь worker отвечает только за orchestration:
1. выбор события из event_store
2. перевод в PROCESSING
3. вызов dispatch
4. перевод в DONE / RETRY / FAILED
5. запись в processed_events
6. запись в errors

Вызов конкретного обработчика вынесен в `event_dispatcher.py`.

Это упрощает расширение системы под новые use-case:
- sale
- product
- stock

---
## 📦 Структура проекта

    integration-bus/
    ├── main.py              # FastAPI приложение (ingest)
    ├── worker.py            # фоновый обработчик событий
    ├── event_dispatcher.py  # dispatch по event_type
    ├── error_logic.py       # классификация ошибок RETRY / FAILED
    ├── error_store.py       # запись ошибок в таблицу errors
    ├── db.py                # подключение к SQLite
    ├── init_db.py           # создание схемы БД
    ├── logger.py            # настройка логирования
    ├── e2e_test.py          # end-to-end тест
    ├── requirements.txt
    ├── app.db               # SQLite база (создаётся автоматически)
    └── README.md

## ⚙️ Требования
 - Python 3.11+
 - macOS / Linux / Windows
 - Homebrew (для macOS, опционально)

---

## 🔧 Установка и запуск

1. Клонировать проект

        git clone <repo-url
        cd integration-bus

2. Создать виртуальное окружение

        python3.11 -m venv venv
        source venv/bin/activate
    Windows:

        venv\Scripts\activate

3. Установить зависимости

        pip install -r requirements.txt

4. Инициализировать базу данных

        python init_db.py

    Ожидаемый вывод:

        DB initialized

## ▶️ Запуск проекта

Проект запускается в трёх терминалах.

---

**Терминал 1 — API сервер**

    uvicorn main:app --reload

API будет доступен:

    http://127.0.0.1:8000/docs

**Терминал 2 — Worker**

    python worker.py
Worker начнёт обрабатывать события.

**Терминал 3 — E2E тест (опционально)**

    python e2e_test.py

Ожидаемый результат:

    ✅ E2E OK: DONE + processed_events

## 🧪 Ручное тестирование webhook
Открыть Swagger:

    http://127.0.0.1:8000/docs

Шаги:
 1. Создать tenant через POST /tenants
 2. Скопировать tenant_id
 3. Отправить webhook через POST /webhooks/evotor/{tenant_id}

Пример body:

    {
    "type": "sale",
    "event_id": "sale-001",
    "amount": 100
    }

## 🔄 Жизненный цикл события
    NEW → PROCESSING → DONE
                    ↘
                    RETRY → FAILED

## ♻️ Retry политика
 - exponential backoff: 1m, 2m, 4m, 8m, 16m
 - максимум 5 попыток
 - после лимита → FAILED

---

## 🧾 Логирование

Формат логов:

    timestamp | level | logger | message

Логи пишутся в stdout.

Основные логгеры:
 - api
 - worker

---

## 🛠️ Полезные эндпоинты

Проверка сервера

    GET /health

Список tenants

    GET /tenants

События на повторной обработке

    GET /events/retry

Обработанные события

    GET /processed

🔮 Дальнейшее развитие

Планируемые улучшения:
 - Mapping Layer (Evotor ↔ MoySklad)
 - Реальный sync в MoySklad API
 - Метрики и мониторинг
 - Batch processing
 - Dead-letter queue
 - Dockerization