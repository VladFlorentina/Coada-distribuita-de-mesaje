from dmq.node import DistributedNode


def test_ephemeral_port_is_advertised_after_start() -> None:
    node = DistributedNode(node_id="node1", bind_host="127.0.0.1", bind_port=0)

    try:
        node.start()
        assert node.actual_port != 0
        assert node.advertise_port == node.actual_port
        assert node.endpoint.port == node.actual_port
    finally:
        node.stop()
