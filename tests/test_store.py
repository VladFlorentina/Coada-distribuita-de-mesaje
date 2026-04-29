from dmq.store import NodeStateStore, PeerEndpoint


def test_subscription_lifecycle() -> None:
    store = NodeStateStore()
    subscriber = PeerEndpoint(host="127.0.0.1", port=5001, node_id="node1")

    store.add_subscription("demo", subscriber)

    assert store.subscribers_for("demo") == [subscriber]

    store.remove_subscription("demo", subscriber.identity)

    assert store.subscribers_for("demo") == []


def test_peer_registration_and_cleanup() -> None:
    store = NodeStateStore()
    peer = PeerEndpoint(host="127.0.0.1", port=5002, node_id="node2")

    store.register_peer(peer)
    store.add_subscription("demo", peer)
    store.remove_peer(peer.identity)

    assert store.list_peers() == []
    assert store.subscribers_for("demo") == []



