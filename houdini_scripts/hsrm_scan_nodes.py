import sys

import hou


def _safe_text(value):
    return str(value).replace("|", "/")


def _is_strict_frame_range(node):
    p = node.parm("trange")
    if p is None:
        return False

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

    return "strict" in token.lower() or "strict" in label.lower()


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
    return ""


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
    return ""


def _evalf(node, name):
    p = node.parm(name)
    if p is None:
        return None
    try:
        return float(p.eval())
    except Exception:
        return None


def _frame_range_triplet(node):
    f1 = _evalf(node, "f1")
    f2 = _evalf(node, "f2")
    f3 = _evalf(node, "f3")
    f = _evalf(node, "f")
    if f1 is not None and f2 is not None:
        step = f3 if (f3 is not None and f3 != 0) else 1.0
        return f1, f2, step
    if f is not None:
        return f, f, 1.0
    return None, None, None


def main():
    roots = [arg for arg in sys.argv[1:] if isinstance(arg, str) and arg.startswith("/")]
    if not roots:
        roots = ["/out", "/stage"]

    print("__HSRM_SCAN_BEGIN__")
    seen = set()
    for root_path in roots:
        try:
            root = hou.node(root_path)
        except Exception:
            root = None
        if root is None:
            continue
        try:
            children = root.children()
        except Exception:
            children = []
        for node in children:
            try:
                path = node.path()
            except Exception:
                continue
            if not path or path in seen:
                continue
            seen.add(path)
            try:
                nt = node.type()
                type_name = nt.name()
                category = nt.category().name()
            except Exception:
                type_name = ""
                category = ""
            strict_flag = "1" if _is_strict_frame_range(node) else "0"
            out_path = _find_output_path(node)
            rf1, rf2, rf3 = _frame_range_triplet(node)
            print(
                "__HSRM_NODE__|%s|%s|%s|%s|%s|%s|%s|%s"
                % (
                    _safe_text(path),
                    _safe_text(category),
                    _safe_text(type_name),
                    strict_flag,
                    _safe_text(out_path),
                    "" if rf1 is None else _safe_text(rf1),
                    "" if rf2 is None else _safe_text(rf2),
                    "" if rf3 is None else _safe_text(rf3),
                )
            )
    print("__HSRM_SCAN_END__")


if __name__ == "__main__":
    main()
