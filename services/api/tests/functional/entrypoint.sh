#!/bin/bash
set -e
poetry install
poetry run tests/functional/utils/wait_for_es.py
poetry run tests/functional/utils/wait_for_redis.py
poetry run pytest tests/functional/src