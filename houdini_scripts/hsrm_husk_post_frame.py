frame = None
try:
    frame = float(hou.frame())
except Exception:
    try:
        rs = settings.get("RenderSettings", {}) if isinstance(settings, dict) else {}
        if isinstance(rs, dict):
            frame = rs.get("houdini:frame", rs.get("frame"))
    except Exception:
        frame = None

print(f"__HSRM_FRAME__|end|{frame}") if frame is not None else print("__HSRM_FRAME__|end|")
