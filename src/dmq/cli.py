from __future__ import annotations

import base64
import argparse
import logging
from pathlib import Path
import shlex
import sys
import time

from .node import DistributedNode, configure_logging
from .protocol import encode_payload, parse_peer_spec, send_request
from .store import PeerEndpoint


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dmq")
    subparsers = parser.add_subparsers(dest="command", required=True)

    node_parser = subparsers.add_parser("node", help="run a distributed node")
    node_parser.add_argument("--node-id", required=True)
    node_parser.add_argument("--bind-host", default="127.0.0.1")
    node_parser.add_argument("--bind-port", type=int, required=True)
    node_parser.add_argument("--advertise-host")
    node_parser.add_argument("--advertise-port", type=int)
    node_parser.add_argument("--seed", action="append", default=[])
    node_parser.add_argument("--key-command", action="append", default=[], help="format: key=command")
    node_parser.add_argument("--log-level", default="INFO")

    subscribe_parser = subparsers.add_parser("subscribe", help="send a subscribe request")
    subscribe_parser.add_argument("--target", required=True, help="format: [node@]host:port")
    subscribe_parser.add_argument("--subscriber", required=True, help="format: node@host:port")
    subscribe_parser.add_argument("--key", required=True)

    unsubscribe_parser = subparsers.add_parser("unsubscribe", help="send an unsubscribe request")
    unsubscribe_parser.add_argument("--target", required=True, help="format: [node@]host:port")
    unsubscribe_parser.add_argument("--subscriber", required=True, help="format: node@host:port")
    unsubscribe_parser.add_argument("--key", required=True)

    publish_parser = subparsers.add_parser("publish", help="send a publish request")
    publish_parser.add_argument("--target", required=True, help="format: [node@]host:port")
    publish_parser.add_argument("--key", required=True)
    publish_payload_group = publish_parser.add_mutually_exclusive_group(required=True)
    publish_payload_group.add_argument("--payload", help="treat payload as UTF-8 text")
    publish_payload_group.add_argument("--payload-hex", help="payload encoded as hex")
    publish_payload_group.add_argument("--payload-base64", help="payload encoded as base64")
    publish_payload_group.add_argument("--payload-file", help="read raw payload bytes from file")

    interactive_parser = subparsers.add_parser("interactive", help="run an interactive node + client menu")
    interactive_parser.add_argument("--node-id", required=True)
    interactive_parser.add_argument("--bind-host", default="127.0.0.1")
    interactive_parser.add_argument("--bind-port", type=int, required=True)
    interactive_parser.add_argument("--advertise-host")
    interactive_parser.add_argument("--advertise-port", type=int)
    interactive_parser.add_argument("--seed", action="append", default=[])
    interactive_parser.add_argument("--key-command", action="append", default=[], help="format: key=command")
    interactive_parser.add_argument("--target", help="default target for requests, format: [node@]host:port")
    interactive_parser.add_argument(
        "--mode",
        choices=["menu", "repl"],
        default="menu",
        help="interaction mode: menu (numbered) or repl (type commands)",
    )
    interactive_parser.add_argument("--log-level", default="INFO")

    return parser


def _parse_key_commands(values: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid key-command mapping: {item!r}")
        key, command = item.split("=", 1)
        mapping[key.strip()] = command.strip()
    return mapping


def _parse_endpoint(spec: str) -> PeerEndpoint:
    host, node_id, port = parse_peer_spec(spec)
    return PeerEndpoint(host=host, port=port, node_id=node_id)


def _read_payload_bytes(args: argparse.Namespace) -> bytes:
    if args.payload is not None:
        return args.payload.encode("utf-8")
    if args.payload_hex is not None:
        return bytes.fromhex(args.payload_hex)
    if args.payload_base64 is not None:
        return base64.b64decode(args.payload_base64.encode("ascii"))
    if args.payload_file is not None:
        return Path(args.payload_file).read_bytes()
    raise ValueError("No payload provided")


def _prompt(text: str) -> str:
    try:
        return input(text)
    except KeyboardInterrupt:
        raise


def _safe_int(text: str, default: int = -1) -> int:
    try:
        return int(text.strip())
    except Exception:
        return default


def _print_menu(node: DistributedNode, *, target: PeerEndpoint | None) -> None:
    print("\n" + "=" * 60)
    print(f"DMQ | Conectat ca: {node.endpoint.identity} ({node.endpoint.host}:{node.endpoint.port})")
    if target is None:
        print("Target: (nesetat)")
    else:
        print(f"Target: {target.identity} ({target.host}:{target.port})")
    print("=" * 60)
    print("1. Seteaza target")
    print("2. Subscribe la cheie")
    print("3. Unsubscribe de la cheie")
    print("4. Publish mesaj")
    print("5. Status (peers + subscriptions)")
    print("0. Iesire")


def _require_target(target: PeerEndpoint | None) -> PeerEndpoint:
    if target is None:
        raise ValueError("Target nesetat. Alege optiunea 1 si seteaza un target.")
    return target


def _interactive_loop(node: DistributedNode, *, initial_target: PeerEndpoint | None) -> int:
    target = initial_target
    while True:
        _print_menu(node, target=target)

        try:
            choice = _safe_int(_prompt("\n\033[94mAlegerea ta:\033[0m "), default=-1)
        except KeyboardInterrupt:
            print("\nIes (Ctrl+C)...")
            return 0

        if choice == 0:
            print("Ies...")
            return 0

        try:
            if choice == 1:
                spec = _prompt("Target ([node@]host:port): ").strip()
                if not spec:
                    print("Target nemodificat.")
                    continue
                target = _parse_endpoint(spec)
                print("OK: target setat.")
                continue

            if choice == 2:
                target_ep = _require_target(target)
                key = _prompt("Cheie: ").strip() or "demo"
                resp = send_request(
                    target_ep.host,
                    target_ep.port,
                    {"type": "SUBSCRIBE", "key": key, "subscriber": node.endpoint.to_dict()},
                )
                print(resp)
                continue

            if choice == 3:
                target_ep = _require_target(target)
                key = _prompt("Cheie: ").strip() or "demo"
                resp = send_request(
                    target_ep.host,
                    target_ep.port,
                    {"type": "UNSUBSCRIBE", "key": key, "subscriber": node.endpoint.to_dict()},
                )
                print(resp)
                continue

            if choice == 4:
                target_ep = _require_target(target)
                key = _prompt("Cheie: ").strip() or "demo"
                payload = _prompt("Payload (text): ").strip() or "hello world"
                resp = send_request(
                    target_ep.host,
                    target_ep.port,
                    {"type": "PUBLISH", "key": key, "payload": encode_payload(payload.encode("utf-8"))},
                )
                print(resp)
                # Give some time for DELIVER logs to show in other terminals.
                time.sleep(0.1)
                continue

            if choice == 5:
                print("\nUpstream:", node.store.upstream())
                print("Peers:", [p.identity for p in node.store.list_peers()])
                print("Subscriptions:", node.store.subscription_snapshot())
                continue

            print("Optiune invalida.")
        except ValueError as exc:
            print(f"\033[91mEroare: {exc}\033[0m")
        except OSError as exc:
            print(f"\033[91mEroare retea: {exc}\033[0m")


def _print_repl_help() -> None:
    print("\nComenzi disponibile:")
    print("  help")
    print("  target [node@]host:port")
    print("  subscribe <cheie>")
    print("  unsubscribe <cheie>")
    print("  publish <cheie> <text...>")
    print("  status")
    print("  exit | quit")


def _interactive_repl(node: DistributedNode, *, initial_target: PeerEndpoint | None) -> int:
    target = initial_target
    print("\nDMQ REPL. Scrie 'help' pentru comenzi.")
    print(f"Conectat ca: {node.endpoint.identity} ({node.endpoint.host}:{node.endpoint.port})")
    if target is not None:
        print(f"Target implicit: {target.identity} ({target.host}:{target.port})")

    while True:
        try:
            line = input("\033[94mdmq>\033[0m ")
        except (EOFError, KeyboardInterrupt):
            print("\nIes...")
            return 0

        line = line.strip()
        if not line:
            continue

        try:
            parts = shlex.split(line)
        except ValueError as exc:
            print("Eroare:", exc)
            continue

        cmd = parts[0].lower()
        args = parts[1:]

        try:
            if cmd in {"exit", "quit"}:
                print("Ies...")
                return 0

            if cmd == "help":
                _print_repl_help()
                continue

            if cmd == "target":
                if not args:
                    raise ValueError("Folosire: target [node@]host:port")
                target = _parse_endpoint(args[0])
                print("OK: target setat.")
                continue

            if cmd == "subscribe":
                target_ep = _require_target(target)
                key = (args[0] if args else "demo").strip() or "demo"
                resp = send_request(
                    target_ep.host,
                    target_ep.port,
                    {"type": "SUBSCRIBE", "key": key, "subscriber": node.endpoint.to_dict()},
                )
                print(resp)
                continue

            if cmd == "unsubscribe":
                target_ep = _require_target(target)
                key = (args[0] if args else "demo").strip() or "demo"
                resp = send_request(
                    target_ep.host,
                    target_ep.port,
                    {"type": "UNSUBSCRIBE", "key": key, "subscriber": node.endpoint.to_dict()},
                )
                print(resp)
                continue

            if cmd == "publish":
                target_ep = _require_target(target)
                if len(args) < 1:
                    raise ValueError("Folosire: publish <cheie> <text...>")
                key = args[0]
                payload = " ".join(args[1:]).strip() or "hello world"
                resp = send_request(
                    target_ep.host,
                    target_ep.port,
                    {"type": "PUBLISH", "key": key, "payload": encode_payload(payload.encode("utf-8"))},
                )
                print(resp)
                time.sleep(0.1)
                continue

            if cmd == "status":
                print("Upstream:", node.store.upstream())
                print("Peers:", [p.identity for p in node.store.list_peers()])
                print("Subscriptions:", node.store.subscription_snapshot())
                continue

            print("Comanda necunoscuta. Scrie 'help'.")
        except ValueError as exc:
            print(f"\033[91mEroare: {exc}\033[0m")
        except OSError as exc:
            print(f"\033[91mEroare retea: {exc}\033[0m")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "node":
        configure_logging(getattr(logging, str(args.log_level).upper(), logging.INFO))
        key_commands = _parse_key_commands(args.key_command)
        node = DistributedNode(
            node_id=args.node_id,
            bind_host=args.bind_host,
            bind_port=args.bind_port,
            advertise_host=args.advertise_host,
            advertise_port=args.advertise_port,
            seeds=args.seed,
            key_commands=key_commands,
        )
        node.serve_forever()
        return 0

    if args.command == "interactive":
        configure_logging(getattr(logging, str(args.log_level).upper(), logging.INFO))
        key_commands = _parse_key_commands(args.key_command)
        node = DistributedNode(
            node_id=args.node_id,
            bind_host=args.bind_host,
            bind_port=args.bind_port,
            advertise_host=args.advertise_host,
            advertise_port=args.advertise_port,
            seeds=args.seed,
            key_commands=key_commands,
        )

        initial_target: PeerEndpoint | None = None
        if args.target:
            initial_target = _parse_endpoint(args.target)
        elif args.seed:
            # Default to the first seed host:port
            host, node_id, port = parse_peer_spec(args.seed[0])
            initial_target = PeerEndpoint(host=host, port=port, node_id=node_id)

        try:
            node.start()
            if args.mode == "repl":
                return _interactive_repl(node, initial_target=initial_target)
            return _interactive_loop(node, initial_target=initial_target)
        finally:
            node.stop()

    if args.command in {"subscribe", "unsubscribe", "publish"}:
        target = _parse_endpoint(args.target)

        if args.command == "publish":
            payload_bytes = _read_payload_bytes(args)
            response = send_request(
                target.host,
                target.port,
                {
                    "type": "PUBLISH",
                    "key": args.key,
                    "payload": encode_payload(payload_bytes),
                },
            )
            print(response)
            return 0

        subscriber = _parse_endpoint(args.subscriber)
        response = send_request(
            target.host,
            target.port,
            {
                "type": "SUBSCRIBE" if args.command == "subscribe" else "UNSUBSCRIBE",
                "key": args.key,
                "subscriber": subscriber.to_dict(),
            },
        )
        print(response)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
