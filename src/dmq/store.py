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
        return self.node_id or f"{self.host}:{self.port}"

    def to_dict(self) -> dict[str, object]:
        return {"host": self.host, "port": self.port, "node_id": self.node_id}

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "PeerEndpoint":
        return cls(
            host=str(data["host"]),
            port=int(data["port"]),
            node_id=data.get("node_id") if data.get("node_id") is not None else None,
        )


# Madalina - functionalitate: Structuri de date locale si thread-safe (Cerintele tehnice privind memorie locala)
class NodeStateStore:
    def __init__(self) -> None:
        self._lock = RLock()
        self._peers: dict[str, PeerEndpoint] = {}
        # Dictionar ce mapeaza CHEIE -> (IDENTITY -> Endpoint) 
        self._subscriptions: dict[str, dict[str, PeerEndpoint]] = defaultdict(dict)
        self._upstream_id: str | None = None

    def register_peer(self, peer: PeerEndpoint) -> None:
        # Madalina - functionalitate: Adaug un nod in memorie (Discovery)
        with self._lock:
            self._peers[peer.identity] = peer

    def list_peers(self) -> list[PeerEndpoint]:
        with self._lock:
            return list(self._peers.values())

    def remove_peer(self, identity: str) -> None:
        with self._lock:
            self._peers.pop(identity, None)
            for subscribers in self._subscriptions.values():
                subscribers.pop(identity, None)
            if self._upstream_id == identity:
                self._upstream_id = None

    def prune_subscriber(self, identity: str) -> None:
        with self._lock:
            for subscribers in self._subscriptions.values():
                subscribers.pop(identity, None)
            empty_keys = [key for key, subscribers in self._subscriptions.items() if not subscribers]
            for key in empty_keys:
                self._subscriptions.pop(key, None)

    def set_upstream(self, identity: str | None) -> None:
        with self._lock:
            self._upstream_id = identity

    def upstream(self) -> str | None:
        with self._lock:
            return self._upstream_id

    def add_subscription(self, key: str, subscriber: PeerEndpoint) -> None:
        with self._lock:
            self._subscriptions[key][subscriber.identity] = subscriber

    def remove_subscription(self, key: str, subscriber_identity: str) -> None:
        with self._lock:
            subscribers = self._subscriptions.get(key)
            if not subscribers:
                return
            subscribers.pop(subscriber_identity, None)
            if not subscribers:
                self._subscriptions.pop(key, None)

    def has_subscription(self, key: str, subscriber_identity: str) -> bool:
        with self._lock:
            subscribers = self._subscriptions.get(key)
            return bool(subscribers and subscriber_identity in subscribers)

    def subscribers_for(self, key: str) -> list[PeerEndpoint]:
        with self._lock:
            return list(self._subscriptions.get(key, {}).values())

    def subscription_snapshot(self) -> dict[str, list[dict[str, object]]]:
        with self._lock:
            return {
                key: [subscriber.to_dict() for subscriber in subscribers.values()]
                for key, subscribers in self._subscriptions.items()
            }

