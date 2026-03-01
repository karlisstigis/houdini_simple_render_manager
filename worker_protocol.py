from __future__ import annotations

import json
import sys
from typing import Any

from PySide6 import QtCore


def build_message(message_type: str, request_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": str(message_type or "").strip(),
        "request_id": str(request_id or "").strip(),
        "payload": dict(payload or {}),
    }


def encode_message(message_type: str, request_id: str, payload: dict[str, Any] | None = None) -> bytes:
    return (json.dumps(build_message(message_type, request_id, payload), ensure_ascii=True) + "\n").encode("utf-8")


def decode_message(raw_line: str) -> dict[str, Any] | None:
    text = str(raw_line or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    payload = data.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    return {
        "type": str(data.get("type", "") or "").strip(),
        "request_id": str(data.get("request_id", "") or "").strip(),
        "payload": payload,
    }


class MessageBuffer:
    def __init__(self) -> None:
        self._buffer = ""

    def push_bytes(self, data: bytes) -> list[dict[str, Any]]:
        if not data:
            return []
        self._buffer += bytes(data).decode("utf-8", errors="replace")
        messages: list[dict[str, Any]] = []
        while True:
            newline_index = self._buffer.find("\n")
            if newline_index < 0:
                break
            raw_line = self._buffer[:newline_index]
            self._buffer = self._buffer[newline_index + 1 :]
            message = decode_message(raw_line)
            if message is not None:
                messages.append(message)
        return messages


class StdinReader(QtCore.QThread):
    chunk_read = QtCore.Signal(bytes)
    closed = QtCore.Signal()

    def run(self) -> None:
        while True:
            try:
                line = sys.stdin.buffer.readline()
            except Exception:
                line = b""
            if not line:
                self.closed.emit()
                return
            self.chunk_read.emit(line)
