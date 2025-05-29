#!/bin/bash
set -e
pip install -r tests/functional/requirements.txt
python tests/functional/utils/wait_for_es.py
python tests/functional/utils/wait_for_redis.py
pytest tests/functional/src