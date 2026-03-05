from __future__ import annotations


def normalized_notification(message: str, severity: str) -> tuple[str, str] | None:
    text = str(message or "").strip()
    if not text:
        return None
    sev = str(severity or "info").lower()
    return text, sev


def notification_signature(message: str, severity: str) -> tuple[str, str] | None:
    normalized = normalized_notification(message, severity)
    if normalized is None:
        return None
    return normalized


def should_add_notification(
    *,
    signature: tuple[str, str] | None,
    last_signature: tuple[str, str] | None,
    dedupe_consecutive: bool,
) -> bool:
    if signature is None:
        return False
    if dedupe_consecutive and signature == last_signature:
        return False
    return True


def trim_notification_count(*, count: int, max_items: int) -> int:
    if max_items < 0:
        max_items = 0
    extra = int(count) - int(max_items)
    return extra if extra > 0 else 0


def notification_color_hex(severity: str) -> str:
    sev = str(severity or "info").lower()
    if sev == "error":
        return "#d96b6b"
    if sev == "warning":
        return "#d4ad4a"
    return "#d8d8d8"
