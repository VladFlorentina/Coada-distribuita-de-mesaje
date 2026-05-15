from __future__ import annotations

from collections.abc import Callable, Mapping


def cmd_echo(text: str) -> str:
    return text


def cmd_upper(text: str) -> str:
    return text.upper()


def cmd_lower(text: str) -> str:
    return text.lower()


def cmd_reverse(text: str) -> str:
    return text[::-1]


def cmd_length(text: str) -> str:
    return str(len(text))


class CommandRegistry:
    def __init__(self, key_to_command: Mapping[str, str] | None = None) -> None:
        self._key_to_command = dict(key_to_command or {})
        self._commands: dict[str, Callable[[str], str]] = {
            "echo": cmd_echo,
            "upper": cmd_upper,
            "lower": cmd_lower,
            "reverse": cmd_reverse,
            "length": cmd_length,
        }

    def execute(self, key: str, payload: bytes) -> tuple[str, str]:
        text = payload.decode("utf-8", errors="replace")

        command_name = str(self._key_to_command.get(key) or "echo")
        command = self._commands.get(command_name)
        if command is None:
            command_name = "echo"
            command = self._commands[command_name]

        result = command(text)
        return command_name, result

