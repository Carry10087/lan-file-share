from __future__ import annotations

import contextlib
import importlib.util
import io
import itertools
import json
import os
import pathlib
import re
import subprocess
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
APP_PATH = next(ROOT.glob("*文件共享服务器.py"))


def load_app():
    module = importlib.util.module_from_spec(
        importlib.util.spec_from_loader("lanfs_homepage_check", loader=None)
    )
    module.__file__ = str(APP_PATH)

    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        source = APP_PATH.read_text(encoding="utf-8-sig")
        code = compile(source, str(APP_PATH), "exec")
        exec(code, module.__dict__)

    app = getattr(module, "app", None)
    if app is None:
        raise RuntimeError("Flask app object was not found on the module")

    app.testing = True
    return module, app


def reset_runtime_state(module) -> None:
    with module.user_lock:
        module.online_users.clear()
    with module.activity_lock:
        module.current_activities.clear()


def render_homepage_scripts(app) -> str:
    client = app.test_client()
    with client.session_transaction() as session_data:
        session_data["username"] = "回归"

    response = client.get("/")
    if response.status_code != 200:
        raise AssertionError(f"Homepage did not return 200: {response.status_code}")

    html = response.get_data(as_text=True)
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.IGNORECASE | re.DOTALL)
    if not scripts:
        raise AssertionError("No inline scripts were rendered on the homepage")
    return "\n\n".join(scripts)


def check_homepage_js_runtime(app) -> None:
    script_text = render_homepage_scripts(app)
    required_markers = [
        "async function checkAdminPermission",
        "function toggleSection",
        "function openFilePreview",
        "function startRealtimeStream",
        "function startRealtimeUpdates",
        "function handleRealtimeSnapshot",
    ]
    for marker in required_markers:
        if marker not in script_text:
            raise AssertionError(f"Homepage script is missing marker: {marker}")

    safe_script_chars = []
    for ch in script_text:
        codepoint = ord(ch)
        if ch in "\r\n\t" or 32 <= codepoint < 127:
            safe_script_chars.append(ch)
        else:
            safe_script_chars.append(f"\\u{codepoint:04x}")

    temp_path = None
    try:
        temp_dir = pathlib.Path(os.environ.get("TEMP", str(ROOT)))
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".js",
            prefix="homepage-runtime-",
            dir=temp_dir,
            delete=False,
            encoding="ascii",
        ) as handle:
            handle.write("".join(safe_script_chars))
            temp_path = pathlib.Path(handle.name)

        subprocess.run(
            ["node", "--check", str(temp_path)],
            cwd=temp_dir,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()


def check_multi_tab_presence(module, app) -> None:
    reset_runtime_state(module)
    client = app.test_client()
    with client.session_transaction() as session_data:
        session_data["username"] = "多页"

    first = client.get("/get_online_users", headers={"X-Page-Session-Id": "page-a"}).get_json()
    second = client.get("/get_online_users", headers={"X-Page-Session-Id": "page-b"}).get_json()
    offline = client.post("/offline", data={"page_session_id": "page-a"}).get_json()
    after = client.get("/get_online_users", headers={"X-Page-Session-Id": "page-b"}).get_json()

    if first["count"] != 1 or second["count"] != 1:
        raise AssertionError(f"Unexpected online count before closing a tab: {first} / {second}")
    if not offline.get("success"):
        raise AssertionError(f"Offline request did not succeed: {offline}")
    if after["count"] != 1 or after["users"][0]["username"] != "多页":
        raise AssertionError(f"Closing one tab removed the whole user: {after}")


def check_realtime_stream_snapshot(module, app) -> None:
    reset_runtime_state(module)
    client = app.test_client()
    with client.session_transaction() as session_data:
        session_data["username"] = "联调"

    response = client.get("/api/realtime_stream?page_session_id=page-stream", buffered=False)
    try:
        text = "".join(
            chunk.decode("utf-8", errors="replace")
            for chunk in itertools.islice(response.response, 2)
        )
    finally:
        response.close()

    if "event: snapshot" not in text:
        raise AssertionError(f"Realtime stream did not emit a snapshot event: {text[:300]}")

    data_line = next((line[6:] for line in text.splitlines() if line.startswith("data: ")), None)
    if not data_line:
        raise AssertionError(f"Realtime stream payload is missing data line: {text[:300]}")

    payload = json.loads(data_line)
    if payload.get("current_username") != "联调":
        raise AssertionError(f"Unexpected realtime username payload: {payload}")
    if "online_users" not in payload or "activities" not in payload or "admin" not in payload:
        raise AssertionError(f"Realtime payload is missing homepage fields: {payload}")


def main() -> None:
    module, app = load_app()
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        check_homepage_js_runtime(app)
        check_multi_tab_presence(module, app)
        check_realtime_stream_snapshot(module, app)
    print("Homepage runtime checks passed.")


if __name__ == "__main__":
    main()
