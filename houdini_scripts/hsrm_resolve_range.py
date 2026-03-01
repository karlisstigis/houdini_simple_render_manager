import re
import sys
import hou

rop_path = sys.argv[1] if len(sys.argv) > 1 else ""
n = hou.node(rop_path)
if n is None:
    print("__HSRM_RANGE_ERR__|node_not_found")
    raise SystemExit(0)


def _strict_trange_flag(node):
    p = node.parm("trange")
    if p is None:
        return 0
    token = ""
    label = ""
    try:
        token = str(p.evalAsString() or "")
    except Exception:
        token = ""
    try:
        pt = p.parmTemplate()
        labels = list(pt.menuLabels() or [])
        items = list(pt.menuItems() or [])
        idx = int(p.eval())
        if 0 <= idx < len(labels):
            label = str(labels[idx] or "")
        elif 0 <= idx < len(items):
            label = str(items[idx] or "")
    except Exception:
        label = ""
    return 1 if ("strict" in token.lower() or "strict" in label.lower()) else 0

def _evalf(name):
    p = n.parm(name)
    return float(p.eval()) if p is not None else None


def _looks_like_output_path(text):
    s = str(text or "").strip()
    if not s:
        return False
    low = s.lower()
    if low == "ip":
        return True
    if "/" not in s and "\\" not in s:
        return False
    exts = (".exr", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".rat", ".bmp", ".usd", ".usda", ".usdc")
    return any(ext in low for ext in exts) or "$f" in low or "<f" in low


def _iter_candidate_output_parms(node):
    for name in ("outputimage", "picture", "vm_picture", "sopoutput", "lopoutput", "filename1", "filename", "productName"):
        p = node.parm(name)
        if p is None:
            continue
        yield p

    for p in node.parms():
        try:
            pt = p.parmTemplate()
            if pt.type() != hou.parmTemplateType.String:
                continue
            name_l = p.name().lower()
            label_l = str(pt.label() or "").lower()
            if not any(k in name_l or k in label_l for k in ("output", "picture", "file", "image", "filename", "product")):
                continue
        except Exception:
            continue
        yield p


def _parm_output_value(parm):
    try:
        val = parm.evalAsString()
        if _looks_like_output_path(val):
            return str(val)
    except Exception:
        pass
    try:
        ref = parm.getReferencedParm()
    except Exception:
        ref = None
    if ref is not None and ref is not parm:
        try:
            val = ref.evalAsString()
            if _looks_like_output_path(val):
                return str(val)
        except Exception:
            pass
    return None


def _iter_upstream_nodes(node, max_depth=16):
    seen = set()
    queue = [(node, 0)]
    while queue:
        current, depth = queue.pop(0)
        if current is None:
            continue
        try:
            path = current.path()
        except Exception:
            continue
        if not path or path in seen:
            continue
        seen.add(path)
        yield current
        if depth >= max_depth:
            continue
        try:
            inputs = list(current.inputs())
        except Exception:
            inputs = []
        for upstream in inputs:
            queue.append((upstream, depth + 1))


def _find_output_path(node):
    for candidate_node in _iter_upstream_nodes(node):
        for parm in _iter_candidate_output_parms(candidate_node):
            value = _parm_output_value(parm)
            if value:
                return value
    return None


def _format_frame(frame_value, width):
    try:
        iv = int(round(float(frame_value)))
    except Exception:
        iv = 1
    sign = "-" if iv < 0 else ""
    body = str(abs(iv)).zfill(max(1, int(width)))
    return f"{sign}{body}"


def _expand_frame_tokens(text, frame_value):
    s = str(text or "")
    if not s:
        return s

    def _replace_angle(match):
        width_txt = match.group(1) or ""
        width = int(width_txt) if width_txt.isdigit() else 1
        return _format_frame(frame_value, width)

    def _replace_braced(match):
        width_txt = match.group(1) or ""
        width = int(width_txt) if width_txt.isdigit() else 1
        return _format_frame(frame_value, width)

    def _replace_dollar(match):
        width_txt = match.group(1) or ""
        width = int(width_txt) if width_txt.isdigit() else 1
        return _format_frame(frame_value, width)

    s = re.sub(r"<F(\d*)>", _replace_angle, s, flags=re.IGNORECASE)
    s = re.sub(r"\$\{F(\d*)\}", _replace_braced, s, flags=re.IGNORECASE)
    s = re.sub(r"\$F(\d*)", _replace_dollar, s, flags=re.IGNORECASE)
    return s


def _resolve_output_sample_path(raw_path, sample_frame):
    s = str(raw_path or "").strip()
    if not s:
        return s
    if s.lower() == "ip":
        return s
    try:
        # Expands Houdini variables/token expressions where supported.
        s = hou.expandStringAtFrame(s, float(sample_frame))
    except Exception:
        try:
            s = hou.expandString(s)
        except Exception:
            pass
    s = _expand_frame_tokens(s, sample_frame)
    return s


print(f"__HSRM_TRANGE_STRICT__|{_strict_trange_flag(n)}")
f1 = _evalf("f1")
f2 = _evalf("f2")
f3 = _evalf("f3")
f = _evalf("f")
sample_frame = f1 if f1 is not None else (f if f is not None else 1.0)

outp = _find_output_path(n)
if outp:
    outp_resolved = _resolve_output_sample_path(outp, sample_frame)
    print(f"__HSRM_OUT__|{str(outp_resolved).replace('|','/')}")

if f1 is not None and f2 is not None:
    step = f3 if (f3 is not None and f3 != 0) else 1.0
    print(f"__HSRM_RANGE__|{f1}|{f2}|{step}")
elif f is not None:
    print(f"__HSRM_RANGE__|{f}|{f}|1")
else:
    print("__HSRM_RANGE_ERR__|frame_parms_not_found")
