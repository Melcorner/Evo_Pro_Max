#!/bin/bash
set -e

TEST_DB_URL="postgresql://evotor_test:test_password@localhost:5432/evotor_ms_test"

echo "=== PostgreSQL smoke тест ==="
RUN_PG_RUNTIME_SMOKE=1 python -m pytest tests/test_pg_runtime_smoke.py::test_pg_runtime_smoke -v --tb=short

echo ""
echo "=== E2E PostgreSQL тесты ==="
RUN_E2E_PG=1 DATABASE_URL=$TEST_DB_URL \
  python -m pytest tests/test_e2e_pg.py -v --tb=short

echo ""
echo "=== Unit тесты (PostgreSQL) ==="
DATABASE_URL=$TEST_DB_URL python -m pytest tests/ -v \
  --ignore=tests/test_pg_runtime_smoke.py \
  --ignore=tests/test_e2e_pg.py \
  --tb=short

echo ""
echo "=== Все тесты завершены ==="
