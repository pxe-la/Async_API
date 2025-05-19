import json
from typing import Any, Dict

from utils.storages.base_storage import BaseStorage  # type: ignore


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        try:
            with open(self.file_path, "r"):
                pass
        except FileNotFoundError:
            with open(self.file_path, "w") as f:
                json.dump({}, f)

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, "w") as f:
            json.dump(state, f)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        with open(self.file_path, "r") as f:
            return json.load(f)
