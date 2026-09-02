"""
central_server_cloud.py
EyeStrain AI — Centralized Data Collection Server (Cloud / Render deployment)

This is the cloud version of the server — backed by a real Postgres
database instead of local files, so it works across the internet from
any device, not just the same Wi-Fi network.

── HOW IT READS SECRETS ──────────────────────────────────────────────────
This file NEVER hardcodes the database URL or API key. Both are read from
environment variables, which you set in Render's dashboard (Environment
tab of your Web Service) — not committed to any file:

    DATABASE_URL   = <your Render Postgres Internal Database URL>
    EYESTRAIN_API_KEY = <a secret string you choose — must match the
                          same value entered in data_sync.py's client code>

── WHAT IT STORES ─────────────────────────────────────────────────────────
One row per received CSV upload: username, filename, the raw CSV text
(same derived-metric rows already written locally on each user's own
machine — no raw video/images), timestamps.
────────────────────────────────────────────────────────────────────────
"""

import os
from datetime import datetime
from urllib.parse import urlparse

from flask import Flask, request, jsonify
import pg8000.dbapi as pg8000

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
API_KEY      = os.environ.get("EYESTRAIN_API_KEY", "")


def _parse_database_url(url: str) -> dict:
    """
    pg8000 takes connection parameters directly rather than a single DSN
    string (unlike psycopg2), so we parse Render's postgres:// URL here.
    """
    parsed = urlparse(url)
    return {
        "host":     parsed.hostname,
        "port":     parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
        "user":     parsed.username,
        "password": parsed.password,
    }


def get_conn():
    """
    CHANGED — was psycopg2.connect(). psycopg2-binary's compiled C
    extension failed to load on Render's Python 3.14 runtime (no
    prebuilt wheel available yet for this very new Python version, and
    the source-built fallback didn't load at runtime). pg8000 is a pure
    Python driver — no compiled extension, so this class of problem
    can't happen regardless of Python version.
    """
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Set it in Render's dashboard under Environment."
        )
    params = _parse_database_url(DATABASE_URL)
    return pg8000.connect(
        host=params["host"],
        port=params["port"],
        database=params["database"],
        user=params["user"],
        password=params["password"],
    )


def init_db():
    """Creates the sessions table if it doesn't already exist."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS session_uploads (
                id                SERIAL PRIMARY KEY,
                username          TEXT NOT NULL,
                filename          TEXT NOT NULL,
                csv_content       TEXT NOT NULL,
                consent_timestamp TEXT,
                received_at       TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE (username, filename)
            );
        """)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _check_auth(req) -> bool:
    if not API_KEY:
        # Fail closed — if no API key configured server-side, reject
        # everything rather than silently accepting unauthenticated data.
        return False
    return req.headers.get("X-API-Key") == API_KEY


@app.route("/api/status", methods=["GET"])
def status():
    """Health check — also verifies DB connectivity."""
    db_ok = True
    try:
        conn = get_conn()
        conn.close()
    except Exception:
        db_ok = False
    return jsonify({
        "status":       "ok",
        "database":     "connected" if db_ok else "unreachable",
        "server_time":  datetime.now().isoformat(),
    })


@app.route("/api/upload", methods=["POST"])
def upload():
    """
    Receives one session CSV's contents as JSON.
    Expected body:
      {
        "username": "user_001",
        "filename": "session_20260901_101500.csv",
        "csv_content": "<full csv text>",
        "consent_timestamp": "2026-09-01T10:20:00"
      }
    """
    if not _check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "missing json body"}), 400

    username    = str(data.get("username", "unknown")).strip() or "unknown"
    filename    = str(data.get("filename", "")).strip()
    csv_content = data.get("csv_content", "")
    consent_ts  = data.get("consent_timestamp", "")

    if not filename.endswith(".csv") or not csv_content:
        return jsonify({"error": "invalid payload"}), 400
    if "/" in filename or "\\" in filename or ".." in filename:
        return jsonify({"error": "invalid filename"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO session_uploads
                (username, filename, csv_content, consent_timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (username, filename) DO NOTHING
            RETURNING id;
        """, (username, filename, csv_content, consent_ts))
        row = cur.fetchone()
        conn.commit()
        cur.close()
    finally:
        conn.close()

    if row is None:
        return jsonify({"status": "already_received"}), 200

    print(f"[CentralServer] Received {filename} from '{username}'")
    return jsonify({"status": "received"}), 200


@app.route("/api/summary", methods=["GET"])
def summary():
    """Quick overview — how many files received per user."""
    if not _check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT username, COUNT(*) AS file_count
            FROM session_uploads
            GROUP BY username
            ORDER BY username;
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    users = {r[0]: r[1] for r in rows}
    return jsonify({"users": users, "total_files": sum(users.values())})


@app.route("/api/export/<username>", methods=["GET"])
def export_user(username):
    """
    Returns every uploaded CSV's raw content for one user, as JSON —
    useful for pulling data down later to feed into TCN retraining
    (data_preprocessor.py / feature_engineer.py / data_loader.py).
    """
    if not _check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT filename, csv_content, received_at
            FROM session_uploads
            WHERE username = %s
            ORDER BY received_at;
        """, (username,))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    files = [
        {
            "filename":    r[0],
            "csv_content": r[1],
            "received_at": r[2].isoformat() if r[2] else None,
        }
        for r in rows
    ]
    return jsonify({"username": username, "files": files})


# Initialize the table on startup (safe to call repeatedly — CREATE TABLE
# IF NOT EXISTS is a no-op if it already exists).
try:
    init_db()
    print("[CentralServer] Database initialized.")
except Exception as e:
    print(f"[CentralServer] WARNING — could not initialize database: {e}")


if __name__ == "__main__":
    # Local testing only — on Render, gunicorn runs this instead (see
    # the deployment instructions).
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)