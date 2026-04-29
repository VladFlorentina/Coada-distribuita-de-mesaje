from __future__ import annotations

import time

from dmq.node import DistributedNode
from dmq.protocol import send_request


def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_two_nodes_join_and_deliver_message() -> None:
    node1 = DistributedNode(node_id="node1", bind_host="127.0.0.1", bind_port=0, key_commands={"demo": "upper"})
    node2 = DistributedNode(
        node_id="node2",
        bind_host="127.0.0.1",
        bind_port=0,
        seeds=["node1@127.0.0.1:0"],
        key_commands={"demo": "reverse"},
    )

    try:
        node1.start()
        node2._seed_specs = [f"node1@127.0.0.1:{node1.actual_port}"]
        node2.start()

        assert _wait_until(lambda: node2.store.upstream() == "node1")
        assert _wait_until(lambda: any(peer.identity == "node2" for peer in node1.store.list_peers()))

        subscribe_response = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "SUBSCRIBE", "key": "demo", "subscriber": node2.endpoint.to_dict()},
        )
        assert subscribe_response["status"] == "OK"
        assert node1.store.has_subscription("demo", "node2")

        publish_response = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "PUBLISH", "key": "demo", "payload": "aGVsbG8="},
        )

        assert publish_response["status"] == "OK"
        assert publish_response["delivered"] == 1

        unsubscribe_response = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "UNSUBSCRIBE", "key": "demo", "subscriber": node2.endpoint.to_dict()},
        )
        assert unsubscribe_response["status"] == "OK"
        assert not node1.store.has_subscription("demo", "node2")

        resubscribe_response = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "SUBSCRIBE", "key": "demo", "subscriber": node2.endpoint.to_dict()},
        )
        assert resubscribe_response["status"] == "OK"
        assert node1.store.has_subscription("demo", "node2")

        node2.stop()

        cleanup_publish_response = send_request(
            "127.0.0.1",
            node1.actual_port,
            {"type": "PUBLISH", "key": "demo", "payload": "aGVsbG8="},
        )

        assert cleanup_publish_response["status"] == "OK"
        assert cleanup_publish_response["delivered"] == 0
        assert node1.store.subscribers_for("demo") == []
    finally:
        node1.stop()
