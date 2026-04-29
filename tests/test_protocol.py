import argparse

from dmq.commands import CommandRegistry
from dmq.cli import _read_payload_bytes
from dmq.protocol import decode_message, encode_message, encode_payload, decode_payload, parse_peer_spec


def test_message_roundtrip() -> None:
    message = {"type": "PUBLISH", "key": "demo", "payload": encode_payload(b"hello")}
    raw = encode_message(message)

    assert decode_message(raw) == message


def test_payload_roundtrip() -> None:
    payload = b"sample"
    assert decode_payload(encode_payload(payload)) == payload


def test_peer_spec_parser_with_node_id() -> None:
    host, node_id, port = parse_peer_spec("node1@127.0.0.1:5001")

    assert host == "127.0.0.1"
    assert node_id == "node1"
    assert port == 5001


def test_command_registry_executes_key_specific_commands() -> None:
    registry = CommandRegistry({"demo": "reverse", "other": "upper"})

    command_name, result = registry.execute("demo", b"hello")

    assert command_name == "reverse"
    assert result == "olleh"


def test_read_payload_bytes_supports_text_hex_base64_and_file(tmp_path) -> None:
    payload_file = tmp_path / "payload.bin"
    payload_file.write_bytes(b"\x00\xffdata")

    text_args = argparse.Namespace(payload="hello", payload_hex=None, payload_base64=None, payload_file=None)
    hex_args = argparse.Namespace(payload=None, payload_hex="68656c6c6f", payload_base64=None, payload_file=None)
    base64_args = argparse.Namespace(payload=None, payload_hex=None, payload_base64="aGVsbG8=", payload_file=None)
    file_args = argparse.Namespace(payload=None, payload_hex=None, payload_base64=None, payload_file=str(payload_file))

    assert _read_payload_bytes(text_args) == b"hello"
    assert _read_payload_bytes(hex_args) == b"hello"
    assert _read_payload_bytes(base64_args) == b"hello"
    assert _read_payload_bytes(file_args) == b"\x00\xffdata"

