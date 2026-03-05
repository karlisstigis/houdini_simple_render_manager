from __future__ import annotations

import unittest

from queue_model_text import queue_model_display_text


class _IndexStub:
    def __init__(self, valid: bool, value: object) -> None:
        self._valid = valid
        self._value = value

    def isValid(self) -> bool:
        return self._valid

    def data(self, _role: object) -> object:
        return self._value


class _ModelStub:
    def __init__(self, index_obj: _IndexStub) -> None:
        self._index_obj = index_obj

    def index(self, _row: int, _column: int) -> _IndexStub:
        return self._index_obj


class QueueModelTextTests(unittest.TestCase):
    def test_queue_model_display_text(self) -> None:
        self.assertEqual(queue_model_display_text(None, 0, 0, display_role=0), "")
        invalid_model = _ModelStub(_IndexStub(False, "x"))
        self.assertEqual(queue_model_display_text(invalid_model, 0, 0, display_role=0), "")
        valid_model = _ModelStub(_IndexStub(True, "  hello  "))
        self.assertEqual(queue_model_display_text(valid_model, 0, 0, display_role=0), "hello")
        none_model = _ModelStub(_IndexStub(True, None))
        self.assertEqual(queue_model_display_text(none_model, 0, 0, display_role=0), "")


if __name__ == "__main__":
    unittest.main()
