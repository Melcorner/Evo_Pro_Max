#!/bin/bash
set -e

echo "=== SQLite тесты (быстрые, unit) ==="
DATABASE_URL=sqlite:///tmp/test.db python -m pytest tests/ -v --ignore=tests/test_pg_runtime_smoke.py --tb=short

echo ""
echo "=== PostgreSQL smoke тест ==="
RUN_PG_RUNTIME_SMOKE=1 python -m pytest tests/test_pg_runtime_smoke.py::test_pg_runtime_smoke -v --tb=short

echo ""
echo "=== Все тесты завершены ==="
