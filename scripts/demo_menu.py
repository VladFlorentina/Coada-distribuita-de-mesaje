from __future__ import annotations

import argparse
from collections import defaultdict, deque
import logging
import time

from dmq.node import DistributedNode, configure_logging
from dmq.protocol import encode_payload, send_request


class _RingLogHandler(logging.Handler):
    def __init__(self, *, per_logger_limit: int = 80) -> None:
        super().__init__()
        self._buffers: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=per_logger_limit))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Keep only the most useful demo line(s).
            msg = record.getMessage()
            if "Processed deliver" not in msg:
                return
            self._buffers[record.name].append(msg)
        except Exception:
            return

    def tail(self, logger_prefix: str, *, limit: int = 10) -> list[str]:
        items: list[str] = []
        for logger_name, buf in self._buffers.items():
            if not logger_name.startswith(logger_prefix):
                continue
            items.extend(list(buf))
        if len(items) > limit:
            return items[-limit:]
        return items


def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _print_header(title: str) -> None:
    print("\n" + "=" * 58)
    print(title)
    print("=" * 58)


def _safe_int(text: str, default: int) -> int:
    try:
        return int(text.strip())
    except Exception:
        return default


def _start_cluster(*, base_port: int, key: str) -> tuple[DistributedNode, DistributedNode, DistributedNode]:
    node1 = DistributedNode(
        node_id="node1",
        bind_host="127.0.0.1",
        bind_port=base_port,
        advertise_host="127.0.0.1",
        advertise_port=base_port,
        key_commands={key: "upper"},
    )
    node2 = DistributedNode(
        node_id="node2",
        bind_host="127.0.0.1",
        bind_port=base_port + 1,
        advertise_host="127.0.0.1",
        advertise_port=base_port + 1,
        seeds=[f"node1@127.0.0.1:{base_port}"],
        key_commands={key: "reverse"},
    )
    node3 = DistributedNode(
        node_id="node3",
        bind_host="127.0.0.1",
        bind_port=base_port + 2,
        advertise_host="127.0.0.1",
        advertise_port=base_port + 2,
        seeds=[
            f"node2@127.0.0.1:{base_port + 1}",
            f"node1@127.0.0.1:{base_port}",
        ],
        key_commands={key: "length"},
    )

    node1.start()
    node2.start()
    node3.start()

    assert _wait_until(lambda: node2.store.upstream() == "node1"), "node2 did not connect to node1"
    assert _wait_until(lambda: node3.store.upstream() in {"node2", "node1"}), "node3 did not connect"

    return node1, node2, node3


def _subscribe(target_port: int, key: str, subscriber) -> dict:
    return send_request(
        "127.0.0.1",
        target_port,
        {"type": "SUBSCRIBE", "key": key, "subscriber": subscriber.to_dict()},
    )


def _unsubscribe(target_port: int, key: str, subscriber) -> dict:
    return send_request(
        "127.0.0.1",
        target_port,
        {"type": "UNSUBSCRIBE", "key": key, "subscriber": subscriber.to_dict()},
    )


def _publish(target_port: int, key: str, payload_text: str) -> dict:
    return send_request(
        "127.0.0.1",
        target_port,
        {"type": "PUBLISH", "key": key, "payload": encode_payload(payload_text.encode("utf-8"))},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Interactive DMQ demo menu")
    parser.add_argument("--base-port", type=int, default=5001, help="node1 port (node2=node1+1, node3=node1+2)")
    parser.add_argument("--key", default="demo", help="topic key")
    parser.add_argument("--log-level", default="WARNING", help="Python logging level: DEBUG/INFO/WARNING")
    args = parser.parse_args(argv)

    configure_logging(getattr(logging, str(args.log_level).upper(), logging.WARNING))

    # Keep the console mostly clean for interactive input, but still capture
    # per-node processing results.
    for handler in logging.getLogger().handlers:
        handler.setLevel(logging.WARNING)
    logging.getLogger("dmq.node").setLevel(logging.INFO)
    ring = _RingLogHandler()
    ring.setLevel(logging.INFO)
    logging.getLogger().addHandler(ring)

    key = str(args.key)
    base_port = int(args.base_port)

    node1: DistributedNode | None = None
    node2: DistributedNode | None = None
    node3: DistributedNode | None = None

    def cluster_running() -> bool:
        return node1 is not None and node1.actual_port != 0

    try:
        _print_header("DMQ | Demo interactiv")
        print(f"Cheie: {key}")
        print(f"Porturi: node1={base_port}, node2={base_port + 1}, node3={base_port + 2}")
        print("\nPornesc automat cele 3 noduri...")

        try:
            node1, node2, node3 = _start_cluster(base_port=base_port, key=key)
        except OSError as exc:
            print("Nu pot porni nodurile (port ocupat sau permisiuni).")
            print(f"Eroare: {exc}")
            print("Sugestii: oprește procesele care folosesc porturile sau rulează cu --base-port 5101")
            return 2

        target_port = node1.actual_port

        while True:
            _print_header("MENIU")
            print("1. Subscribe node2 la cheie")
            print("2. Subscribe node3 la cheie")
            print("3. Unsubscribe node2")
            print("4. Unsubscribe node3")
            print("5. Publish mesaj")
            print("6. Simulează crash node2")
            print("7. Status (upstream + subscribers)")
            print("8. Ultimele procesări (Processed deliver)")
            print("0. Ieșire")

            try:
                choice = _safe_int(input("\nAlegerea ta: "), default=-1)
            except KeyboardInterrupt:
                print("\nIes (Ctrl+C)...")
                return 0

            if choice == 0:
                print("Ies...")
                return 0

            if not cluster_running():
                print("Clusterul nu rulează.")
                continue

            if choice == 1:
                if node2 is None:
                    print("node2 e oprit; nu pot face subscribe.")
                else:
                    resp = _subscribe(target_port, key, node2.endpoint)
                    print("subscribe node2 ->", resp)
            elif choice == 2:
                resp = _subscribe(target_port, key, node3.endpoint)
                print("subscribe node3 ->", resp)
            elif choice == 3:
                if node2 is None:
                    print("node2 e oprit; nu pot face unsubscribe.")
                else:
                    resp = _unsubscribe(target_port, key, node2.endpoint)
                    print("unsubscribe node2 ->", resp)
            elif choice == 4:
                resp = _unsubscribe(target_port, key, node3.endpoint)
                print("unsubscribe node3 ->", resp)
            elif choice == 5:
                payload = input("Payload (text): ").strip()
                if not payload:
                    payload = "hello world"
                resp = _publish(target_port, key, payload)
                print("publish ->", resp)
                time.sleep(0.2)
                lines = ring.tail("dmq.node.", limit=6)
                if lines:
                    print("\nUltimele procesări:")
                    for line in lines:
                        print("-", line)
                else:
                    print("(nu am prins încă loguri 'Processed deliver'; încearcă subscribe + publish)")
            elif choice == 6:
                if node2 is None:
                    print("node2 e deja oprit.")
                else:
                    print("Oprire node2...")
                    node2.stop()
                    node2 = None
                    time.sleep(0.2)
                    print("node2 oprit.")
            elif choice == 7:
                assert node1 is not None and node3 is not None
                print("\nUpstream:")
                print("- node1:", node1.store.upstream())
                print("- node2:", None if node2 is None else node2.store.upstream())
                print("- node3:", node3.store.upstream())
                print("\nSubscribers (snapshot node1):")
                print(node1.store.subscription_snapshot())
                print("\nPeers (node1):")
                print([p.identity for p in node1.store.list_peers()])
            elif choice == 8:
                lines = ring.tail("dmq.node.", limit=20)
                if not lines:
                    print("Nu există încă procesări capturate.")
                else:
                    print("\nProcessed deliver (ultimele linii):")
                    for line in lines:
                        print("-", line)
            else:
                print("Opțiune invalidă.")

    finally:
        for n in (node3, node2, node1):
            if n is not None:
                try:
                    n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
