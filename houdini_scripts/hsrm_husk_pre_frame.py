frame = None
out_path = None
try:
    frame = float(hou.frame())
except Exception:
    try:
        rs = settings.get("RenderSettings", {}) if isinstance(settings, dict) else {}
        if isinstance(rs, dict):
            frame = rs.get("houdini:frame", rs.get("frame"))
    except Exception:
        frame = None

try:
    rps = settings.get("RenderProducts") if isinstance(settings, dict) else None
    if isinstance(rps, (list, tuple)) and rps:
        rp0 = rps[0]
        if isinstance(rp0, dict):
            out_path = rp0.get("expandedProductName")
            if not out_path:
                rpset = rp0.get("settings")
                if isinstance(rpset, dict):
                    pn = rpset.get("productName")
                    if isinstance(pn, str):
                        out_path = pn
                    elif isinstance(pn, (list, tuple)) and pn:
                        out_path = pn[0]
except Exception:
    out_path = None

print(f"__HSRM_FRAME__|start|{frame}") if frame is not None else print("__HSRM_FRAME__|start|")
print(f"__HSRM_OUT__|{out_path}") if out_path else None
