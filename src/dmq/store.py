from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from threading import RLock


@dataclass(frozen=True, slots=True)
class PeerEndpoint:
    host: str
    port: int
    node_id: str | None = None

    @property
    def identity(self) -> str:
        if self.node_id:
            return self.node_id
        return f'{self.host}:{self.port}'

    def to_dict(self) -> dict[str, object]:
        return {'host': self.host, 'port': self.port, 'node_id': self.node_id}

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> 'PeerEndpoint':
        return cls(
            host=str(data['host']),
            port=int(data['port']),
            node_id=data.get('node_id')
        )


# Thread-safe local state store (req: local storage of peers + subscriptions)
class NodeStateStore:
    def __init__(self) -> None:
        # RLock protects all mutable state against concurrent access from multiple handler threads
        self._mutex = RLock()
        
        # All known peers indexed by their identity (node_id or host:port)
        self.peer_registry: dict[str, PeerEndpoint] = {}
        
        # Maps a topic key to a dict of subscribers: identity -> PeerEndpoint
        self.subscribes_map: dict[str, dict[str, PeerEndpoint]] = defaultdict(dict)
        
        # The upstream node this node connected to at startup
        self.upstream_node: str | None = None

    def register_peer(self, node: PeerEndpoint) -> None:
        # Add or update a peer in the local registry
        with self._mutex:
            self.peer_registry[node.identity] = node

    def list_peers(self) -> list[PeerEndpoint]:
        # Return a snapshot of all known peers (thread-safe copy)
        with self._mutex:
            return list(self.peer_registry.values())

    def remove_peer(self, node_identity: str) -> None:
        # Remove a disconnected peer from the registry and from all subscription lists
        with self._mutex:
            self.peer_registry.pop(node_identity, None)

            for sub_list in self.subscribes_map.values():
                sub_list.pop(node_identity, None)

            if self.upstream_node == node_identity:
                self.upstream_node = None

    def prune_subscriber(self, sub_identity: str) -> None:
        # Remove a peer only from subscription lists, leaving the peer registry intact
        with self._mutex:
            for clients in self.subscribes_map.values():
                clients.pop(sub_identity, None)

            # Clean up keys that now have no subscribers
            dead_keys = [k for k, c in self.subscribes_map.items() if not c]
            for k in dead_keys:
                del self.subscribes_map[k]

    def set_upstream(self, identity: str | None) -> None:
        with self._mutex:
            self.upstream_node = identity

    def upstream(self) -> str | None:
        with self._mutex:
            return self.upstream_node

    def add_subscription(self, topic_key: str, client_peer: PeerEndpoint) -> None:
        # Register a subscriber for the given key (req 2.3)
        with self._mutex:
            self.subscribes_map[topic_key][client_peer.identity] = client_peer

    def remove_subscription(self, topic_key: str, client_identity: str) -> None:
        # Remove a subscriber; clean up the key entry if no subscribers remain
        with self._mutex:
            if topic_key not in self.subscribes_map:
                return

            self.subscribes_map[topic_key].pop(client_identity, None)

            if not self.subscribes_map[topic_key]:
                del self.subscribes_map[topic_key]

    def has_subscription(self, topic_key: str, client_identity: str) -> bool:
        with self._mutex:
            try:
                return client_identity in self.subscribes_map[topic_key]
            except KeyError:
                return False

    def subscribers_for(self, topic_key: str) -> list[PeerEndpoint]:
        # Return all subscribers registered for a given topic key
        with self._mutex:
            return list(self.subscribes_map.get(topic_key, {}).values())

    def subscription_snapshot(self) -> dict[str, list[dict[str, object]]]:
        with self._mutex:
            copy_dict = {}
            for key, clients in self.subscribes_map.items():
                copy_dict[key] = [c.to_dict() for c in clients.values()]
            return copy_dict

