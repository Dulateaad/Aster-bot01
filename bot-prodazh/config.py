import os
from typing import List


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Environment variable '{key}' is required but not set")
    return value


def _parse_id_list(value: str) -> List[int]:
    if not value:
        return []
    return [int(item.strip()) for item in value.split(",") if item.strip()]


MAIN_BOT_TOKEN = _require_env("MAIN_BOT_TOKEN")
ADMIN_IDS = _parse_id_list(os.environ.get("ADMIN_IDS", ""))
MANAGER_IDS = _parse_id_list(os.environ.get("MANAGER_IDS", ""))