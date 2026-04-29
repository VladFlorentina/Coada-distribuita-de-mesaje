from __future__ import annotations

from collections.abc import Callable, Mapping


Command = Callable[[str], str]


def _echo(payload: str) -> str:
    return payload


def _upper(payload: str) -> str:
    return payload.upper()


def _lower(payload: str) -> str:
    return payload.lower()


def _reverse(payload: str) -> str:
    return payload[::-1]


def _length(payload: str) -> str:
    return str(len(payload))


class CommandRegistry:
    def __init__(self, key_to_command: Mapping[str, str] | None = None) -> None:
        self._key_to_command = dict(key_to_command or {})
        self._commands: dict[str, Command] = {
            "echo": _echo,
            "upper": _upper,
            "lower": _lower,
            "reverse": _reverse,
            "length": _length,
        }

    def execute(self, key: str, payload: bytes) -> tuple[str, str]:
        text = payload.decode("utf-8", errors="replace")
        command_name = self._key_to_command.get(key, "echo")
        command = self._commands.get(command_name, _echo)
        return command_name, command(text)

