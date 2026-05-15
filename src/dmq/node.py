from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Iterable
from typing import Any

from .commands import CommandRegistry
from .protocol import decode_payload, encode_message, encode_payload, parse_peer_spec, send_request
from .store import NodeStateStore, PeerEndpoint


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )


class DistributedNode:
    def __init__(
        self,
        *,
        node_id: str,
        bind_host: str,
        bind_port: int,
        advertise_host: str | None = None,
        advertise_port: int | None = None,
        seeds: Iterable[str] | None = None,
        key_commands: dict[str, str] | None = None,
        max_payload_size: int = 64 * 1024,
    ) -> None:
        self.node_id = node_id
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.advertise_host = advertise_host or bind_host
        self.advertise_port = advertise_port or bind_port
        self.max_payload_size = max_payload_size
        self.logger = logging.getLogger(f"dmq.node.{node_id}")
        self.store = NodeStateStore()
        self.commands = CommandRegistry(key_commands)
        self.endpoint = PeerEndpoint(self.advertise_host, self.advertise_port, self.node_id)
        self._seed_specs = list(seeds or [])
        self._shutdown_event = threading.Event()
        self._server_socket: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None

    @property
    def actual_port(self) -> int:
        if self._server_socket is None:
            return self.bind_port
        return int(self._server_socket.getsockname()[1])

    def start(self) -> None:
        self._start_server()
        self.store.register_peer(self.endpoint)
        self._connect_to_seeds()

    def serve_forever(self) -> None:
        self.start()
        self.logger.info("Node started at %s:%s", self.advertise_host, self.advertise_port)
        try:
            while not self._shutdown_event.is_set():
                self._shutdown_event.wait(0.5)
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        finally:
            self.stop()

    def stop(self) -> None:
        self._shutdown_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass

    def _start_server(self) -> None:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.bind_host, self.bind_port))
        server_socket.listen()
        server_socket.settimeout(1.0)
        self._server_socket = server_socket
        self.bind_port = int(server_socket.getsockname()[1])
        if self.advertise_port == 0:
            self.advertise_port = self.bind_port
            self.endpoint = PeerEndpoint(self.advertise_host, self.advertise_port, self.node_id)
        self._accept_thread = threading.Thread(target=self._accept_loop, name=f"accept-{self.node_id}", daemon=True)
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        assert self._server_socket is not None
        while not self._shutdown_event.is_set():
            try:
                connection, address = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            worker = threading.Thread(
                target=self._handle_connection,
                name=f"conn-{self.node_id}-{address[0]}:{address[1]}",
                args=(connection,),
                daemon=True,
            )
            worker.start()

    def _handle_connection(self, connection: socket.socket) -> None:
        with connection:
            connection.settimeout(5.0)
            with connection.makefile("rb") as reader, connection.makefile("wb") as writer:
                raw_message = reader.readline()
                if not raw_message:
                    return
                message = self._read_message(raw_message)
                response = self._dispatch_message(message)
                if response is not None:
                    writer.write(encode_message(response))
                    writer.flush()

    def _read_message(self, raw_message: bytes) -> dict[str, Any]:
        from .protocol import decode_message

        return decode_message(raw_message)

    def _dispatch_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        message_type = message.get("type")
        
        # Madalina - functionalitate: Topologie, Discovery si mecanism Pub/Sub
        if message_type == "HELLO":
            return self._handle_hello(message)
        if message_type == "PEER_ANNOUNCE":
            return self._handle_peer_announce(message)
        if message_type == "SUBSCRIBE":
            return self._handle_subscription(message, subscribe=True)
        if message_type == "UNSUBSCRIBE":
            return self._handle_subscription(message, subscribe=False)
            
        # Partea Colegei (Publish / DELIVER / Procesare)
        if message_type == "PUBLISH":
            return self._handle_publish(message)
        if message_type == "DELIVER":
            return self._handle_deliver(message)
            
        return {"type": "STATUS", "status": "ERROR", "message": f"Unsupported message type: {message_type}"}

    def _handle_hello(self, message: dict[str, Any]) -> dict[str, Any]:
        # Madalina - functionalitate: Gestionare conectare nod nou si anuntare port callback (Cerinta 2.2)
        node_info = message.get("node", {})
        new_peer = PeerEndpoint.from_dict(node_info)
        
        # Daca nu sunt eu, il adaug in lista mea de peers
        if new_peer.identity != self.node_id:
            self.store.register_peer(new_peer)
            # Daca e prima conexiune facuta, el devine upstream-ul meu
            if not self.store.upstream():
                self.store.set_upstream(new_peer.identity)
                
        # Mai departe, anunt toata reteaua ca a aparut un nod nou
        self._broadcast_peer_announce(new_peer.identity, new_peer)
        
        # Raspund cu lista de noduri cunoscute de mine
        known_peers = self.store.list_peers()
        peer_list_dict = []
        for p in known_peers:
            peer_list_dict.append(p.to_dict())
            
        return {
            "type": "HELLO_ACK",
            "status": "OK",
            "node": self.endpoint.to_dict(),
            "peers": peer_list_dict,
        }

    def _handle_peer_announce(self, message: dict[str, Any]) -> dict[str, Any]:
        # Madalina - functionalitate: Propagare nod nou in retea si evidenta locala
        peer_data = message.get("peer", {})
        announced_peer = PeerEndpoint.from_dict(peer_data)
        
        # Prevenim flood-ul: verificam daca mesajul a trecut deja pe la noi
        nodes_visited = set(message.get("visited", []))
        if self.node_id in nodes_visited:
            return {"type": "STATUS", "status": "IGNORED"}
            
        # Altfel, inregistram nodul si dam forward
        self.store.register_peer(announced_peer)
        self._forward_control_message("PEER_ANNOUNCE", message, exclude={announced_peer.identity})
        
        return {"type": "STATUS", "status": "OK"}

    def _handle_subscription(self, message: dict[str, Any], *, subscribe: bool) -> dict[str, Any]:
        # Madalina - functionalitate: Subscriere, dezabonare si prevenire flood in propagare (Cerinta 2.3)
        target_key = str(message.get("key", ""))
        sub_info = message.get("subscriber", {})
        client_node = PeerEndpoint.from_dict(sub_info)
        
        # Logica pentru gasirea buclelor
        already_visited = set(message.get("visited", []))
        if self.node_id in already_visited:
            return {"type": "STATUS", "status": "IGNORED"}

        # Aplicam actiunea pe stocarea locala
        if subscribe is True:
            self.store.add_subscription(target_key, client_node)
            operation_result = "SUBSCRIBED"
            msg_type = "SUBSCRIBE"
        else:
            self.store.remove_subscription(target_key, client_node.identity)
            operation_result = "UNSUBSCRIBED"
            msg_type = "UNSUBSCRIBE"

        # Propagam actiunea in tot sistemul (mai putin catre cel care a initiat-o)
        self._forward_control_message(msg_type, message, exclude={client_node.identity})
        
        return {
            "type": "STATUS", 
            "status": "OK", 
            "operation": operation_result, 
            "key": target_key
        }

    def _handle_publish(self, message: dict[str, Any]) -> dict[str, Any]:
        key = str(message.get("key", ""))
        payload_encoded = message.get("payload")
        if not isinstance(payload_encoded, str):
            return {"type": "STATUS", "status": "ERROR", "message": "Missing payload"}

        try:
            payload_raw = decode_payload(payload_encoded)
        except Exception as exc:
            return {"type": "STATUS", "status": "ERROR", "message": f"Invalid payload encoding: {exc}"}

        if len(payload_raw) > self.max_payload_size:
            return {
                "type": "STATUS",
                "status": "ERROR",
                "message": f"Payload too large ({len(payload_raw)} > {self.max_payload_size})",
            }

        delivered = 0
        subscribers = self.store.subscribers_for(key)
        for subscriber in subscribers:
            if self._is_local_endpoint(subscriber):
                self._process_payload(key, payload_raw, subscriber)
                delivered += 1
                continue

            deliver_message = {
                "type": "DELIVER",
                "key": key,
                "payload": encode_payload(payload_raw),
                "target": subscriber.to_dict(),
                "publisher": self.endpoint.to_dict(),
            }

            try:
                response = send_request(subscriber.host, subscriber.port, deliver_message)
            except OSError:
                self.store.remove_peer(subscriber.identity)
                continue

            if response.get("status") == "OK":
                delivered += 1

        return {"type": "STATUS", "status": "OK", "key": key, "delivered": delivered}

    def _handle_deliver(self, message: dict[str, Any]) -> dict[str, Any]:
        key = str(message.get("key", ""))
        payload_encoded = message.get("payload")
        if not isinstance(payload_encoded, str):
            return {"type": "STATUS", "status": "ERROR", "message": "Missing payload"}

        try:
            payload_raw = decode_payload(payload_encoded)
        except Exception as exc:
            return {"type": "STATUS", "status": "ERROR", "message": f"Invalid payload encoding: {exc}"}

        target_data = message.get("target")
        target = self.endpoint
        if isinstance(target_data, dict):
            try:
                target = PeerEndpoint.from_dict(target_data)
            except Exception:
                target = self.endpoint

        if target.identity != self.node_id and not self._is_local_endpoint(target):
            return {"type": "STATUS", "status": "IGNORED"}

        self._process_payload(key, payload_raw, target)
        return {"type": "STATUS", "status": "OK"}

    def _process_payload(self, key: str, payload: bytes, target: PeerEndpoint) -> None:
        command_name, result = self.commands.execute(key, payload)
        self.logger.info(
            "Processed deliver for key=%s target=%s command=%s result=%s",
            key,
            target.identity,
            command_name,
            result,
        )

    def _connect_to_seeds(self) -> None:
        for seed_spec in self._seed_specs:
            host, node_id, port = parse_peer_spec(seed_spec)
            peer = PeerEndpoint(host=host, port=port, node_id=node_id)
            try:
                response = send_request(
                    host,
                    port,
                    {"type": "HELLO", "node": self.endpoint.to_dict()},
                )
            except OSError as exc:
                self.logger.info("Seed %s unavailable: %s", seed_spec, exc)
                continue

            self.store.register_peer(peer)
            self.store.set_upstream(peer.identity)
            if response.get("node"):
                try:
                    self.store.register_peer(PeerEndpoint.from_dict(response["node"]))
                except Exception:
                    pass
            for peer_data in response.get("peers", []):
                try:
                    self.store.register_peer(PeerEndpoint.from_dict(peer_data))
                except Exception:
                    continue
            self.logger.info("Connected to seed %s", seed_spec)
            break

    def _broadcast_peer_announce(self, origin_identity: str, peer: PeerEndpoint) -> None:
        message = {"type": "PEER_ANNOUNCE", "peer": peer.to_dict(), "visited": [self.node_id]}
        self._forward_control_message("PEER_ANNOUNCE", message, exclude={origin_identity})

    def _forward_control_message(self, message_type: str, message: dict[str, Any], *, exclude: set[str]) -> None:
        visited = set(message.get("visited", []))
        visited.add(self.node_id)
        forwarded = dict(message)
        forwarded["type"] = message_type
        forwarded["visited"] = sorted(visited)

        for peer in self.store.list_peers():
            if peer.identity == self.node_id or peer.identity in exclude or peer.identity in visited:
                continue
            try:
                send_request(peer.host, peer.port, forwarded)
            except OSError:
                self.store.remove_peer(peer.identity)

    def _is_local_endpoint(self, peer: PeerEndpoint) -> bool:
        return peer.host == self.advertise_host and peer.port == self.advertise_port

