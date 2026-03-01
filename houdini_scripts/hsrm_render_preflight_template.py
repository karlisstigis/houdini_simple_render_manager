import re
import hou

n = hou.node(__HSRM_ROP_PATH_REPR__)
if n is None:
    print("[Preflight] Node not found for preflight: __HSRM_ROP_PATH_TEXT__")
else:
    p_all = n.parm("allframesatonce")
    if p_all is not None:
        try:
            print("[Preflight] allframesatonce=%s on %s" % (int(p_all.eval()), n.path()))
        except Exception:
            pass

    p_m = n.parm("husk_mplay")
    if __HSRM_DISABLE_HUSK_MPLAY__ and p_m is not None:
        try:
            p_m.set(0)
            print("[Preflight] husk_mplay=0 (session only) on %s" % n.path())
        except Exception as exc:
            print("[Preflight] husk_mplay disable failed: %s" % exc)

    def _norm(s):
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    def _find_hook_parms(node, key):
        toggle = None
        script = None
        for parm in node.parms():
            try:
                pt = parm.parmTemplate()
                ptype = pt.type()
                name_n = _norm(parm.name())
                label_n = _norm(pt.label())
            except Exception:
                continue

            matched = (key in name_n) or (key in label_n)
            if not matched:
                continue

            if ptype == hou.parmTemplateType.Toggle and toggle is None:
                toggle = parm
            elif ptype == hou.parmTemplateType.String and script is None:
                script = parm
        return toggle, script

    def _inject_hook(node, key, hook_path):
        if not hook_path:
            print("[Preflight] No hook path provided for %s" % key)
            return
        toggle, script = _find_hook_parms(node, key)
        if script is None:
            print("[Preflight] Hook parm not found for %s" % key)
            return

        try:
            existing = script.evalAsString()
        except Exception:
            existing = ""

        try:
            script.set(hook_path)
        except Exception as exc:
            print("[Preflight] Failed to set hook %s path: %s" % (key, exc))
            return

        if toggle is not None:
            try:
                current_toggle = int(toggle.eval())
            except Exception:
                current_toggle = 0
            try:
                if current_toggle == 0:
                    toggle.set(1)
            except Exception as exc:
                print("[Preflight] Failed to enable hook %s: %s" % (key, exc))
                return

        if existing and existing != hook_path:
            print("[Preflight] Overrode hook %s path for session only: %s" % (key, existing))
        print("[Preflight] Injected hook %s -> %s on %s" % (key, hook_path, node.path()))

    _inject_hook(n, "huskprerender", __HSRM_HOOK_PRE_RENDER_REPR__)
    _inject_hook(n, "huskpreframe", __HSRM_HOOK_PRE_FRAME_REPR__)
    _inject_hook(n, "huskpostframe", __HSRM_HOOK_POST_FRAME_REPR__)
    _inject_hook(n, "huskpostrender", __HSRM_HOOK_POST_RENDER_REPR__)
