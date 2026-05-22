from __future__ import annotations

import json
import os
from threading import RLock
from typing import Any


class WriteAheadLog:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self._lock = RLock()

    def log_pending(self, msg_id: str, key: str, payload_encoded: str) -> None:
        with self._lock:
            with open(self.filepath, "a", encoding="utf-8") as f:
                record = {
                    "action": "PENDING",
                    "msg_id": msg_id,
                    "key": key,
                    "payload": payload_encoded
                }
                f.write(json.dumps(record) + "\n")

    def log_done(self, msg_id: str) -> None:
        with self._lock:
            with open(self.filepath, "a", encoding="utf-8") as f:
                record = {
                    "action": "DONE",
                    "msg_id": msg_id
                }
                f.write(json.dumps(record) + "\n")

    def get_pending_messages(self) -> list[dict[str, Any]]:
        if not os.path.exists(self.filepath):
            return []
            
        pending_msgs = {}
        with self._lock:
            with open(self.filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        msg_id = record.get("msg_id")
                        if not msg_id:
                            continue
                        if record.get("action") == "PENDING":
                            pending_msgs[msg_id] = record
                        elif record.get("action") == "DONE":
                            pending_msgs.pop(msg_id, None)
                    except json.JSONDecodeError:
                        continue
        return list(pending_msgs.values())
