from __future__ import annotations

import base64
import argparse
import logging
from pathlib import Path
import sys

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
