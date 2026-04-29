from __future__ import annotations

import base64
import json
import socket
from typing import Any


def encode_message(message: dict[str, Any]) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


def decode_message(raw: bytes) -> dict[str, Any]:
    decoded = raw.decode("utf-8")
    return json.loads(decoded)


def encode_payload(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def decode_payload(payload: str) -> bytes:
    return base64.b64decode(payload.encode("ascii"))


def send_request(host: str, port: int, message: dict[str, Any], timeout: float = 5.0) -> dict[str, Any]:
    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.settimeout(timeout)
        connection.sendall(encode_message(message))
        try:
            connection.shutdown(socket.SHUT_WR)
        except OSError:
            pass

        with connection.makefile("rb") as reader:
            raw_response = reader.readline()
            if not raw_response:
                return {}
            return decode_message(raw_response)


def parse_peer_spec(value: str) -> tuple[str, str | None, int]:
    node_id: str | None = None
    host_port = value

    if "@" in value:
        node_id, host_port = value.split("@", 1)

    if ":" not in host_port:
        raise ValueError(f"Invalid peer specification: {value!r}")

    host, port_text = host_port.rsplit(":", 1)
    return host, node_id or None, int(port_text)

