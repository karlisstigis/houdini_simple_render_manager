from __future__ import annotations

import re
from pathlib import Path


def frame_sequence_path_for_frame(sample_path: str, frame: int) -> Path | None:
    text = str(sample_path or "").strip()
    p = Path(text)
    if not p.name or text.lower() == "ip":
        return None
    frame_abs = abs(int(frame))
    frame_sign = "-" if int(frame) < 0 else ""

    def _pad(width: int | None) -> str:
        size = max(1, int(width or 1))
        return f"{frame_sign}{frame_abs:0{size}d}"

    name = p.name
    token_patterns: list[tuple[str, re.Pattern[str]]] = [
        ("angle", re.compile(r"<F(\d*)>", re.IGNORECASE)),
        ("brace", re.compile(r"\$\{F(\d*)\}", re.IGNORECASE)),
        ("dollar", re.compile(r"\$F(\d*)", re.IGNORECASE)),
    ]
    for _label, pattern in token_patterns:
        if pattern.search(name):
            replaced = pattern.sub(lambda m: _pad(int(m.group(1)) if m.group(1) else None), name)
            return p.with_name(replaced)

    if any(token in name for token in ("$F", "${F", "<F", "%0")):
        return None

    stem = p.stem
    match = re.search(r"(-?\d+)(?!.*\d)", stem)
    if not match:
        return None
    token = match.group(1)
    negative = token.startswith("-")
    width = len(token) - (1 if negative else 0)
    prefix = stem[: match.start(1)]
    suffix = stem[match.end(1) :]
    if frame < 0:
        body = f"-{abs(frame):0{max(1, width)}d}"
    else:
        body = f"{frame:0{max(1, width)}d}"
    filename = f"{prefix}{body}{suffix}{p.suffix}"
    return p.with_name(filename)


def normalize_output_display_path(path_text: str) -> str:
    text = str(path_text or "").strip()
    if not text:
        return ""
    if text.lower() == "ip":
        return "ip"
    path = Path(text)
    return str(path.parent if path.suffix else path)


def output_folder_from_value(path_text: str) -> Path | None:
    text = str(path_text or "").strip()
    if not text or text.lower() == "ip":
        return None
    path = Path(text)
    return path.parent if path.suffix else path
