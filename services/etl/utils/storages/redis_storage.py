import json
from typing import Any, Dict

from redis import Redis
from utils.storages.base_storage import BaseStorage  # type: ignore


class RedisStorage(BaseStorage):
    def __init__(self, conn: Redis):
        self.conn = conn

    def save_state(self, state: Dict[str, Any]) -> None:
        self.conn.set("state", json.dumps(state))

    def retrieve_state(self) -> Dict[str, Any]:
        try:
            return json.loads(self.conn.get("state"))
        except TypeError:
            return {}
