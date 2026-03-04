import os
import re
from pathlib import Path
import hou

n = hou.node(__HSRM_ROP_PATH_REPR__)
if n is None:
    print("[Preflight] Node not found for preflight: __HSRM_ROP_PATH_TEXT__")
else:
    p_all = n.parm("allframesatonce")
    requested_allframes = str(os.environ.get("HSRM_RENDER_ALL_FRAMES_SINGLE_PROCESS", "") or "").strip()
    if p_all is not None:
        try:
            if requested_allframes in {"0", "1"}:
                p_all.set(1 if requested_allframes == "1" else 0)
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

    def _find_parm(node, label_keys=(), name_keys=(), parm_types=None):
        normalized_label_keys = {_norm(key) for key in label_keys}
        normalized_name_keys = {_norm(key) for key in name_keys}
        for parm in node.parms():
            try:
                pt = parm.parmTemplate()
                ptype = pt.type()
                label_n = _norm(pt.label())
                name_n = _norm(parm.name())
            except Exception:
                continue
            if parm_types and ptype not in parm_types:
                continue
            if label_n in normalized_label_keys or name_n in normalized_name_keys:
                return parm
        return None

    def _find_parms(node, label_fragments=(), name_fragments=(), parm_types=None):
        normalized_label_fragments = tuple(_norm(fragment) for fragment in label_fragments)
        normalized_name_fragments = tuple(_norm(fragment) for fragment in name_fragments)
        matches = []
        for parm in node.parms():
            try:
                pt = parm.parmTemplate()
                ptype = pt.type()
                label_n = _norm(pt.label())
                name_n = _norm(parm.name())
            except Exception:
                continue
            if parm_types and ptype not in parm_types:
                continue
            if any(fragment in label_n for fragment in normalized_label_fragments) or any(fragment in name_n for fragment in normalized_name_fragments):
                matches.append(parm)
        return matches

    def _set_toggle(parm, value, label):
        if parm is None:
            print("[Preflight][RetainUSD] Parm not found for %s" % label)
            return False
        try:
            parm.set(1 if value else 0)
            print("[Preflight][RetainUSD] %s -> %s via %s" % (label, int(bool(value)), parm.name()))
            return True
        except Exception as exc:
            print("[Preflight][RetainUSD] Failed to set %s on %s: %s" % (label, parm.name(), exc))
            return False

    def _set_menu_choice(parm, desired_labels, label):
        if parm is None:
            print("[Preflight][RetainUSD] Parm not found for %s" % label)
            return False
        desired_norm = [_norm(v) for v in desired_labels]
        try:
            pt = parm.parmTemplate()
            labels = list(pt.menuLabels() or [])
            items = list(pt.menuItems() or [])
        except Exception:
            labels = []
            items = []
        for idx, raw_label in enumerate(labels):
            label_n = _norm(raw_label)
            item_n = _norm(items[idx]) if idx < len(items) else ""
            if label_n in desired_norm or item_n in desired_norm:
                try:
                    parm.set(items[idx] if idx < len(items) else idx)
                    print("[Preflight][RetainUSD] %s -> %s via %s" % (label, raw_label, parm.name()))
                    return True
                except Exception as exc:
                    print("[Preflight][RetainUSD] Failed to set %s on %s: %s" % (label, parm.name(), exc))
                    return False
        print("[Preflight][RetainUSD] No matching menu choice for %s on %s. Available labels: %s" % (label, parm.name(), labels))
        return False

    def _set_string(parm, value, label):
        if parm is None:
            print("[Preflight][RetainUSD] Parm not found for %s" % label)
            return False
        try:
            parm.set(value)
            print("[Preflight][RetainUSD] %s -> %s via %s" % (label, value, parm.name()))
            return True
        except Exception as exc:
            print("[Preflight][RetainUSD] Failed to set %s on %s: %s" % (label, parm.name(), exc))
            return False

    def _report_string_value(parm, label):
        if parm is None:
            return ""
        try:
            value = str(parm.evalAsString() or "").strip()
        except Exception:
            try:
                value = str(parm.unexpandedString() or "").strip()
            except Exception:
                value = ""
        if value:
            print("[Preflight][RetainUSD] %s -> %s" % (label, value.replace("\\", "/")))
        return value

    def _normalize_log_path(value):
        return str(value or "").strip().replace("\\", "/")

    def _expanded_string_variants(parm):
        values = []
        if parm is None:
            return values
        for getter_name in ("evalAsString", "unexpandedString"):
            try:
                raw = str(getattr(parm, getter_name)() or "").strip()
            except Exception:
                raw = ""
            if not raw:
                continue
            values.append(raw)
            try:
                expanded = str(hou.text.expandString(raw) or "").strip()
            except Exception:
                try:
                    expanded = str(hou.expandString(raw) or "").strip()
                except Exception:
                    expanded = raw
            if expanded and expanded not in values:
                values.append(expanded)
        return values

    def _looks_absolute_path(value):
        if not value:
            return False
        txt = str(value).strip()
        return txt.startswith("/") or bool(re.match(r"^[A-Za-z]:[\\/]", txt))

    def _best_resolved_usd_output(node, primary_parm, extra_parms):
        candidates = []
        seen = set()

        def _add_candidate(value, source):
            normalized = str(value or "").strip()
            if not normalized:
                return
            key = normalized.lower()
            if key in seen:
                return
            seen.add(key)
            candidates.append((normalized, source))

        for value in _expanded_string_variants(primary_parm):
            _add_candidate(value, getattr(primary_parm, "name", lambda: "output")())
        for parm in list(extra_parms or []):
            for value in _expanded_string_variants(parm):
                lower = value.lower()
                if lower.endswith((".usd", ".usda", ".usdc")):
                    _add_candidate(value, parm.name())
        for parm in node.parms():
            try:
                pt = parm.parmTemplate()
                if pt.type() != hou.parmTemplateType.String:
                    continue
            except Exception:
                continue
            for value in _expanded_string_variants(parm):
                lower = value.lower()
                if lower.endswith((".usd", ".usda", ".usdc")):
                    _add_candidate(value, parm.name())

        absolute_candidates = [(value, source) for value, source in candidates if _looks_absolute_path(value)]
        if absolute_candidates:
            return absolute_candidates[0]
        return candidates[0] if candidates else ("", "")

    retain_enabled = str(os.environ.get("HSRM_RETAIN_USD_ENABLED", "") or "").strip() == "1"
    retain_output_path = str(os.environ.get("HSRM_RETAIN_USD_OUTPUT_PATH", "") or "").strip()
    retain_output_dir = str(os.environ.get("HSRM_RETAIN_USD_OUTPUT_DIR", "") or "").strip()
    reuse_existing = str(os.environ.get("HSRM_REUSE_EXISTING_USD", "") or "").strip() == "1"
    if retain_enabled:
        summary_output = retain_output_path or retain_output_dir
        print("[Preflight][RetainUSD] enabled=1 reuse=%s output=%s" % (int(reuse_existing), summary_output))
        p_output_file = _find_parm(
            n,
            label_keys=("outputfile",),
            name_keys=("outputfile", "lopoutput", "outfile", "usdoutputfile"),
            parm_types=(hou.parmTemplateType.String,),
        )
        p_delete_files = _find_parm(
            n,
            label_keys=("deletefiles",),
            name_keys=("deletefiles",),
        )
        save_to_directory_enable_candidates = _find_parms(
            n,
            label_fragments=("saveallfilestoaspecificdirectory",),
            name_fragments=("savetodirectory",),
            parm_types=(hou.parmTemplateType.Toggle,),
        )
        p_save_to_directory_enable = save_to_directory_enable_candidates[0] if save_to_directory_enable_candidates else None
        p_save_to_directory_directory = _find_parm(
            n,
            label_keys=("usdoutputdirectory",),
            name_keys=("savetodirectory_directory",),
            parm_types=(hou.parmTemplateType.String,),
        )
        p_render_existing = _find_parm(
            n,
            label_keys=("renderexistingfile",),
            name_keys=("renderexistingfile",),
            parm_types=(hou.parmTemplateType.Toggle,),
        )
        existing_file_candidates = _find_parms(
            n,
            label_fragments=("existingfile", "renderexisting", "usdfilerender"),
            name_fragments=("existingfile", "renderexisting", "usdfilerender"),
            parm_types=(hou.parmTemplateType.String,),
        )
        p_save_before_existing = _find_parm(
            n,
            label_keys=("saveusdbeforerenderingexistingfile",),
            name_keys=("saveusdbeforerenderingexistingfile",),
            parm_types=(hou.parmTemplateType.Toggle,),
        )
        _set_menu_choice(
            p_delete_files,
            ("neverdelete", "never"),
            "Delete Files",
        )
        if retain_output_dir:
            _set_toggle(p_save_to_directory_enable, True, "Save All Files to Specific Directory")
            _set_string(p_save_to_directory_directory, retain_output_dir.replace("\\", "/"), "USD Output Directory")
            _report_string_value(p_output_file, "Output File")
        if reuse_existing:
            if existing_file_candidates:
                labels = []
                for parm in existing_file_candidates:
                    try:
                        labels.append("%s(%s)" % (parm.name(), parm.parmTemplate().label()))
                    except Exception:
                        labels.append(parm.name())
                print("[Preflight][RetainUSD] Existing file candidates: %s" % ", ".join(labels))
                if retain_output_path:
                    _set_string(existing_file_candidates[0], retain_output_path.replace("\\", "/"), "Render Existing File Path")
            else:
                print("[Preflight][RetainUSD] No existing-file path parm candidate found.")
            _set_toggle(p_render_existing, True, "Render Existing File")
            _set_toggle(p_save_before_existing, False, "Save USD Before Rendering Existing File")
        else:
            _set_toggle(p_render_existing, False, "Render Existing File")
        resolved_output_path, resolved_output_source = _best_resolved_usd_output(n, p_output_file, existing_file_candidates)
        if reuse_existing and resolved_output_path:
            if resolved_output_source:
                print("[Preflight][RetainUSD] Resolved Output File -> %s (%s)" % (_normalize_log_path(resolved_output_path), resolved_output_source))
            else:
                print("[Preflight][RetainUSD] Resolved Output File -> %s" % _normalize_log_path(resolved_output_path))
        elif retain_output_dir:
            resolved_dir = _report_string_value(p_save_to_directory_directory, "Resolved Output Directory")
            resolved_name = _report_string_value(p_output_file, "Resolved Output File Name")
            if resolved_dir and resolved_name:
                print(
                    "[Preflight][RetainUSD] Resolved Output File Template -> %s"
                    % _normalize_log_path(Path(resolved_dir) / resolved_name)
                )
