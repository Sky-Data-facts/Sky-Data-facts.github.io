"""Shared MJPEG proxy with activation timer, cooldown and idle shutdown."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, request
from requests.auth import HTTPBasicAuth

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

CAMERA_URL = "http://192.168.178.47/video/mjpg.cgi"
CAMERA_USER = os.environ.get("CAMERA_USER", "admin")
CAMERA_PASSWORD = os.environ.get("CAMERA_PASSWORD", "")
STATE_FILE = Path(os.environ.get("CAMERA_STATE_FILE", "/var/lib/kamera-proxy/state.json"))
MAX_RUNTIME_SECONDS = 15 * 60
MAX_VIEWERS = 5  # Maximum simultaneous live video connections
COOLDOWN_SECONDS = 2 * 60 * 60
IDLE_SECONDS = 2 * 60

# Browser API calls are accepted only from these known website origins.
ALLOWED_ORIGINS = {
    "https://sky-data-facts.github.io",
    "https://sky-data-facts-github-io.vercel.app",
}

_lock = threading.RLock()
_condition = threading.Condition(_lock)
_state = {
    "active_until": 0.0,
    "cooldown_until": 0.0,
    "viewers": set(),
    "no_viewers_since": None,
    "latest_frame": None,
    "frame_seq": 0,
    "worker": None,
    "stop_event": None,
}


def _save_persistent_state_locked() -> None:
    """Atomically persist only timestamps; never store camera credentials."""
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "active_until": _state["active_until"],
            "cooldown_until": _state["cooldown_until"],
        }), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, STATE_FILE)
    except OSError as exc:
        app.logger.error("Konnte Kamerastatus nicht speichern (%s)", type(exc).__name__)


def _load_persistent_state() -> None:
    try:
        saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        _state["active_until"] = max(0.0, float(saved.get("active_until", 0.0)))
        _state["cooldown_until"] = max(0.0, float(saved.get("cooldown_until", 0.0)))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass

    now = time.time()
    # If the 15-minute window elapsed while the service was down, its cooldown
    # is measured from that original end time and survives a reboot.
    if _state["active_until"] and _state["active_until"] <= now:
        expired_at = _state["active_until"]
        _state["active_until"] = 0.0
        _state["cooldown_until"] = max(
            _state["cooldown_until"], expired_at + COOLDOWN_SECONDS
        )
        _save_persistent_state_locked()
    elif _state["active_until"] > now:
        # Fail safely after a restart: require a viewer within two minutes.
        _state["no_viewers_since"] = now


def _stop_worker_locked() -> None:
    stop_event = _state.get("stop_event")
    if stop_event is not None:
        stop_event.set()
    _state["latest_frame"] = None
    _condition.notify_all()


def _advance_state_locked(now: float | None = None) -> None:
    if now is None:
        now = time.time()

    if _state["active_until"] and now >= _state["active_until"]:
        # Maximum runtime reached: globally lock out new activations for two hours.
        expired_at = _state["active_until"]
        _state["active_until"] = 0.0
        _state["cooldown_until"] = max(
            _state["cooldown_until"], expired_at + COOLDOWN_SECONDS
        )
        _state["no_viewers_since"] = None
        _stop_worker_locked()
        _save_persistent_state_locked()
        app.logger.info("Maximale Kameralaufzeit erreicht; Sperrzeit gestartet.")
        return

    idle_since = _state["no_viewers_since"]
    if (
        _state["active_until"]
        and not _state["viewers"]
        and idle_since is not None
        and now - idle_since >= IDLE_SECONDS
    ):
        # Idle shutdown does not trigger the two-hour maximum-runtime cooldown.
        _state["active_until"] = 0.0
        _state["no_viewers_since"] = None
        _stop_worker_locked()
        _save_persistent_state_locked()
        app.logger.info("Kamera wegen zwei Minuten ohne Zuschauer abgeschaltet.")


def _status_locked(now: float | None = None) -> dict:
    if now is None:
        now = time.time()
    _advance_state_locked(now)
    active = bool(_state["active_until"] and _state["active_until"] > now)
    cooldown_seconds = max(0, int(_state["cooldown_until"] - now + 0.999))
    remaining = max(0, int(_state["active_until"] - now + 0.999)) if active else 0

    if active:
        message = "Kamera ist eingeschaltet."
    elif cooldown_seconds:
        message = "Kamera-Laufzeit überschritten. Die Sperrzeit läuft."
    else:
        message = "Kamera ist ausgeschaltet und kann eingeschaltet werden."

    return {
        "active": active,
        "cooldown": bool(not active and cooldown_seconds),
        "remaining_seconds": remaining,
        "cooldown_seconds": cooldown_seconds,
        "viewers": len(_state["viewers"]),
        "max_viewers": MAX_VIEWERS,
        "viewers_full": bool(active and len(_state["viewers"]) >= MAX_VIEWERS),
        "message": message,
    }


def _ensure_worker_locked() -> None:
    worker = _state.get("worker")
    current_stop = _state.get("stop_event")
    # A previous worker may still be unwinding after an idle/max-runtime stop.
    # It must not prevent a fresh activation from starting its own worker.
    if (
        worker is not None
        and worker.is_alive()
        and current_stop is not None
        and not current_stop.is_set()
    ):
        return
    stop_event = threading.Event()
    worker = threading.Thread(
        target=_camera_worker,
        args=(stop_event,),
        name="kamera-upstream",
        daemon=True,
    )
    _state["stop_event"] = stop_event
    _state["worker"] = worker
    worker.start()


def _camera_worker(stop_event: threading.Event) -> None:
    """Maintain one upstream camera connection and publish complete JPEG frames."""
    current_thread = threading.current_thread()
    try:
        while not stop_event.is_set():
            with _condition:
                _advance_state_locked()
                if not _state["active_until"]:
                    break

            upstream = None
            try:
                if not CAMERA_PASSWORD:
                    app.logger.error("CAMERA_PASSWORD fehlt in der Dienstumgebung.")
                    break

                upstream = requests.get(
                    CAMERA_URL,
                    auth=HTTPBasicAuth(CAMERA_USER, CAMERA_PASSWORD),
                    stream=True,
                    timeout=(5, 15),
                )
                upstream.raise_for_status()
                buffer = bytearray()

                for chunk in upstream.iter_content(chunk_size=16384):
                    if stop_event.is_set():
                        break
                    if not chunk:
                        continue

                    buffer.extend(chunk)
                    # Reassemble complete JPEGs from the multipart byte stream.
                    while True:
                        start = buffer.find(b"\xff\xd8")  # JPEG SOI
                        if start < 0:
                            if len(buffer) > 1:
                                del buffer[:-1]
                            break
                        if start:
                            del buffer[:start]
                        end = buffer.find(b"\xff\xd9", 2)  # JPEG EOI
                        if end < 0:
                            if len(buffer) > 8 * 1024 * 1024:
                                app.logger.warning("Ungültiger Kamerablock verworfen.")
                                buffer.clear()
                            break

                        frame = bytes(buffer[:end + 2])
                        del buffer[:end + 2]
                        with _condition:
                            _advance_state_locked()
                            if not _state["active_until"] or stop_event.is_set():
                                break
                            _state["latest_frame"] = frame
                            _state["frame_seq"] += 1
                            _condition.notify_all()

                    with _condition:
                        _advance_state_locked()
                        if not _state["active_until"]:
                            break

            except requests.RequestException as exc:
                app.logger.warning("Kamera-Verbindung fehlgeschlagen (%s).", type(exc).__name__)
            finally:
                if upstream is not None:
                    upstream.close()

            if not stop_event.is_set():
                with _condition:
                    _advance_state_locked()
                    still_active = bool(_state["active_until"])
                if still_active:
                    stop_event.wait(2)
    finally:
        with _condition:
            if _state.get("worker") is current_thread:
                _state["worker"] = None
                _state["stop_event"] = None
            _condition.notify_all()


def _watchdog() -> None:
    while True:
        with _condition:
            _advance_state_locked()
            _condition.wait(timeout=1)


_load_persistent_state()
threading.Thread(target=_watchdog, name="kamera-watchdog", daemon=True).start()


@app.after_request
def _cors_headers(response):
    origin = request.headers.get("Origin")
    if origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Vary"] = "Origin"
    return response


@app.route("/")
def index():
    return "Kamera-Proxy läuft. Die Kamerasteuerung erfolgt über die Website-API."


@app.route("/api/status", methods=["GET", "OPTIONS"])
def api_status():
    if request.method == "OPTIONS":
        return ("", 204)
    with _condition:
        return jsonify(_status_locked())


@app.route("/api/start", methods=["POST", "OPTIONS"])
def api_start():
    if request.method == "OPTIONS":
        return ("", 204)

    origin = request.headers.get("Origin")
    if origin and origin not in ALLOWED_ORIGINS:
        return jsonify({"error": "Diese Website darf die Kamera nicht einschalten."}), 403

    with _condition:
        now = time.time()
        status = _status_locked(now)
        if status["cooldown"]:
            return jsonify({**status, "started": False}), 429
        if status["active"]:
            return jsonify({
                **status,
                "started": False,
                "message": "Die Kamera läuft bereits. Der Klick verlängert die Laufzeit nicht.",
            }), 200

        _state["active_until"] = now + MAX_RUNTIME_SECONDS
        _state["no_viewers_since"] = now
        _state["latest_frame"] = None
        _state["frame_seq"] += 1
        _save_persistent_state_locked()
        status = _status_locked(now)
        _condition.notify_all()
        return jsonify({**status, "started": True}), 200


@app.route("/video")
def video():
    with _condition:
        status = _status_locked()
        if not status["active"]:
            code = 429 if status["cooldown"] else 409
            return jsonify({"error": status["message"], **status}), code

        # Enforce the limit on the server, not only in browser code. This remains
        # effective even if many viewers connect simultaneously.
        if len(_state["viewers"]) >= MAX_VIEWERS:
            return jsonify({
                **status,
                "error": f"Alle {MAX_VIEWERS} Zuschauerplätze sind belegt. Bitte später erneut versuchen.",
                "viewers_full": True,
            }), 429

        viewer_id = uuid.uuid4().hex
        _state["viewers"].add(viewer_id)
        _state["no_viewers_since"] = None
        _ensure_worker_locked()
        last_seq = _state["frame_seq"]

    def generate():
        nonlocal last_seq
        try:
            while True:
                frame = None
                with _condition:
                    _advance_state_locked()
                    while _state["active_until"] and _state["frame_seq"] == last_seq:
                        _condition.wait(timeout=1)
                        _advance_state_locked()
                    if not _state["active_until"]:
                        break
                    if _state["frame_seq"] != last_seq:
                        last_seq = _state["frame_seq"]
                        frame = _state["latest_frame"]

                if frame:
                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n"
                        + f"Content-Length: {len(frame)}\r\n\r\n".encode("ascii")
                        + frame
                        + b"\r\n"
                    )
        except GeneratorExit:
            raise
        finally:
            with _condition:
                _state["viewers"].discard(viewer_id)
                if _state["active_until"] and not _state["viewers"]:
                    _state["no_viewers_since"] = time.time()
                _condition.notify_all()

    return Response(
        generate(),
        content_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",
        },
        direct_passthrough=True,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8088, threaded=True)
