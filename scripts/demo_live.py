from __future__ import annotations

import logging
import time

from dmq.node import DistributedNode, configure_logging
from dmq.protocol import encode_payload, send_request


def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _step(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    configure_logging(logging.INFO)

    node1 = DistributedNode(
        node_id="node1",
        bind_host="127.0.0.1",
        bind_port=0,
        advertise_host="127.0.0.1",
        advertise_port=0,
        key_commands={"demo": "upper"},
    )
    node2 = DistributedNode(
        node_id="node2",
        bind_host="127.0.0.1",
        bind_port=0,
        advertise_host="127.0.0.1",
        advertise_port=0,
        seeds=["node1@127.0.0.1:0"],
        key_commands={"demo": "reverse"},
    )
    node3 = DistributedNode(
        node_id="node3",
        bind_host="127.0.0.1",
        bind_port=0,
        advertise_host="127.0.0.1",
        advertise_port=0,
        seeds=["node2@127.0.0.1:0", "node1@127.0.0.1:0"],
        key_commands={"demo": "length"},
    )

    try:
        _step("Start 3 nodes")
        node1.start()
        node2._seed_specs = [f"node1@127.0.0.1:{node1.actual_port}"]
        node2.start()
        node3._seed_specs = [
            f"node2@127.0.0.1:{node2.actual_port}",
            f"node1@127.0.0.1:{node1.actual_port}",
        ]
        node3.start()

        print(f"node1: 127.0.0.1:{node1.actual_port}")
        print(f"node2: 127.0.0.1:{node2.actual_port}")
        print(f"node3: 127.0.0.1:{node3.actual_port}")

        assert _wait_until(lambda: node2.store.upstream() == "node1"), "node2 did not connect to node1"
        assert _wait_until(lambda: node3.store.upstream() in {"node2", "node1"}), "node3 did not connect"

        _step("Subscribe node2 + node3 to key=demo")
        resp = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "SUBSCRIBE", "key": "demo", "subscriber": node2.endpoint.to_dict()},
        )
        print("subscribe node2 ->", resp)
        resp = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "SUBSCRIBE", "key": "demo", "subscriber": node3.endpoint.to_dict()},
        )
        print("subscribe node3 ->", resp)

        _step("Publish #1 (expect delivered=2)")
        resp = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "PUBLISH", "key": "demo", "payload": encode_payload(b"hello world")},
        )
        print("publish ->", resp)

        time.sleep(0.3)

        _step("Unsubscribe node3")
        resp = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "UNSUBSCRIBE", "key": "demo", "subscriber": node3.endpoint.to_dict()},
        )
        print("unsubscribe node3 ->", resp)

        _step("Publish #2 (expect delivered=1)")
        resp = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "PUBLISH", "key": "demo", "payload": encode_payload(b"hello world")},
        )
        print("publish ->", resp)

        time.sleep(0.3)

        _step("Simulate node2 crash")
        node2.stop()
        time.sleep(0.2)

        _step("Publish #3 after crash (expect delivered=0 + cleanup)")
        resp = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "PUBLISH", "key": "demo", "payload": encode_payload(b"hello world")},
        )
        print("publish ->", resp)

        _step("Done")
        print("Check logs above for Processed deliver ... reverse/length")
        return 0
    finally:
        node3.stop()
        node2.stop()
        node1.stop()


if __name__ == "__main__":
    raise SystemExit(main())
