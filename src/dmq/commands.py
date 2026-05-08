from __future__ import annotations

from collections.abc import Mapping

# TODO (Colega): Aici trebuie sa implementezi parserul si functiile pentru comenzi pe baza de chei (switch-case)
# conform cerintelor 2.4 - 2.6 si partea de procesare.
# Exemplu de comenzi de implementat: echo, upper, lower, reverse, length etc. (sub forma de metode/functii).

class CommandRegistry:
    def __init__(self, key_to_command: Mapping[str, str] | None = None) -> None:
        self._key_to_command = dict(key_to_command or {})

    def execute(self, key: str, payload: bytes) -> tuple[str, str]:
        # TODO (Colega): 
        # 1. Extrage string-ul din bytes.
        # 2. Determina numele comenzii dupa mapare (sau o comanda de tip 'echo' implicita).
        # 3. Apeleaza functia specifica comenzii. 
        # 4. Returneaza numele comenzii si rezultatul.
        raise NotImplementedError("TODO Colega: Implementeaza procesarea de comenzi si returneaza formatul necesar")

