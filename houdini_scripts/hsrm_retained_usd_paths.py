import os
from pathlib import Path


def _as_text(value):
    try:
        return str(value or "").strip()
    except Exception:
        return ""


def _iter_setting_values(root):
    if isinstance(root, dict):
        for value in root.values():
            yield value
            for nested in _iter_setting_values(value):
                yield nested
    elif isinstance(root, (list, tuple, set)):
        for value in root:
            yield value
            for nested in _iter_setting_values(value):
                yield nested


def _is_absolute_path(value):
    text = _as_text(value)
    return bool(text) and (text.startswith("/") or (len(text) > 2 and text[1] == ":" and text[2] in ("\\", "/")))


def _normalized_path(value):
    return _as_text(value).replace("\\", "/")


def matches_configured_template(candidate, configured_output_path):
    candidate_text = _normalized_path(candidate)
    configured_text = _normalized_path(configured_output_path)
    if not candidate_text or not configured_text:
        return False
    if "$RENDERID" not in configured_text:
        return candidate_text.lower() == configured_text.lower()
    prefix, suffix = configured_text.split("$RENDERID", 1)
    candidate_lower = candidate_text.lower()
    return candidate_lower.startswith(prefix.lower()) and candidate_lower.endswith(suffix.lower())


def matches_configured_directory(candidate, configured_output_dir):
    candidate_dir = _normalized_path(Path(candidate).parent if candidate else "")
    configured_dir = _normalized_path(configured_output_dir)
    if not candidate_dir or not configured_dir:
        return False
    if "$RENDERID" not in configured_dir:
        return candidate_dir.lower() == configured_dir.lower()
    prefix, suffix = configured_dir.split("$RENDERID", 1)
    candidate_dir_lower = candidate_dir.lower()
    return candidate_dir_lower.startswith(prefix.lower()) and candidate_dir_lower.endswith(suffix.lower())


def retained_usd_env():
    return (
        _as_text(os.environ.get("HSRM_RETAIN_USD_OUTPUT_PATH")),
        _as_text(os.environ.get("HSRM_RETAIN_USD_OUTPUT_DIR")),
    )


def usd_path_candidates(settings_obj):
    candidates = []
    for value in _iter_setting_values(settings_obj):
        text = _as_text(value)
        if not text:
            continue
        if text.lower().endswith((".usd", ".usda", ".usdc")):
            candidates.append(text)
    return candidates


def choose_retained_usd_path(
    *,
    settings_obj,
    configured_output_path,
    configured_output_dir,
    require_existing=False,
):
    if settings_obj is None:
        return configured_output_path

    candidates = usd_path_candidates(settings_obj)

    if configured_output_path:
        matching_candidates = [candidate for candidate in candidates if matches_configured_template(candidate, configured_output_path)]
        chosen = _prefer_candidate(matching_candidates, require_existing=require_existing)
        if chosen:
            return chosen
        if require_existing:
            try:
                if Path(configured_output_path).exists():
                    return configured_output_path
            except Exception:
                pass
        return configured_output_path

    if configured_output_dir:
        matching_candidates = [candidate for candidate in candidates if matches_configured_directory(candidate, configured_output_dir)]
        return _prefer_candidate(matching_candidates, require_existing=require_existing)

    return _prefer_candidate(candidates, require_existing=require_existing)


def _prefer_candidate(candidates, *, require_existing):
    if not candidates:
        return ""
    if require_existing:
        for candidate in candidates:
            try:
                if _is_absolute_path(candidate) and Path(candidate).exists():
                    return candidate
            except Exception:
                continue
        return ""

    for candidate in candidates:
        try:
            if _is_absolute_path(candidate) and Path(candidate).exists():
                return candidate
        except Exception:
            continue
    for candidate in candidates:
        if _is_absolute_path(candidate):
            return candidate
    for candidate in candidates:
        try:
            if Path(candidate).exists():
                return candidate
        except Exception:
            continue
    return candidates[0]
