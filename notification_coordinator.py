from __future__ import annotations

from typing import Iterable

from notification_list_state import (
    normalized_notification,
    notification_signature,
    should_add_notification,
)
from notification_rules import notification_messages_for_log


NotificationEntry = tuple[str, str]
NotificationSignature = tuple[str, str] | None


def appendable_notifications(
    *,
    candidates: Iterable[tuple[str, str]],
    last_signature: NotificationSignature,
    dedupe_consecutive: bool,
) -> tuple[list[NotificationEntry], NotificationSignature]:
    entries: list[NotificationEntry] = []
    current_signature = last_signature
    for message, severity in candidates:
        normalized = normalized_notification(message, severity)
        signature = notification_signature(message, severity)
        if not should_add_notification(
            signature=signature,
            last_signature=current_signature,
            dedupe_consecutive=dedupe_consecutive,
        ):
            continue
        assert normalized is not None
        entries.append(normalized)
        current_signature = signature
    return entries, current_signature


def appendable_notifications_for_log(
    *,
    source: str,
    text: str,
    last_signature: NotificationSignature,
    dedupe_consecutive: bool,
) -> tuple[list[NotificationEntry], NotificationSignature]:
    candidates = notification_messages_for_log(source, text)
    return appendable_notifications(
        candidates=candidates,
        last_signature=last_signature,
        dedupe_consecutive=dedupe_consecutive,
    )
