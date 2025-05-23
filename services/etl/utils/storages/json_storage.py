import json

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

    def get(self, key: str) -> str:
        with open(self.file_path, "r") as f:
            return json.load(f).get(key)

    def set(self, key: str, value: str) -> None:
        with open(self.file_path, "r") as f:
            state = json.load(f)
        state[key] = value
        with open(self.file_path, "w") as f:
            json.dump(state, f)
