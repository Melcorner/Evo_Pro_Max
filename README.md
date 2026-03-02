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

## 📦 Структура проекта

    integration-bus/
    ├── main.py              # FastAPI приложение (ingest)
    ├── worker.py            # фоновый обработчик событий
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

Список событий

    GET /events

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