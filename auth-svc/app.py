import os
import sqlite3
import datetime
import time
import secrets
from flask import Flask, request, jsonify, render_template, make_response, redirect
from dotenv import load_dotenv
import jwt
from passlib.hash import argon2

try:
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
    PROM_AVAILABLE = True
except Exception:
    PROM_AVAILABLE = False

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("AUTH_DB_PATH", os.path.join(BASE_DIR, "sqlite", "auth.db"))

JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET must be set")

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin12345")

app = Flask(__name__)

if PROM_AVAILABLE:
    AUTH_LOGIN_ATTEMPTS = Counter("auth_login_attempts_total", "Total login attempts")
    AUTH_LOGIN_SUCCESS = Counter("auth_login_success_total", "Successful logins")
    AUTH_LOGIN_FAILURE = Counter("auth_login_failure_total", "Failed logins")
    AUTH_REGISTER_SUCCESS = Counter("auth_register_success_total", "Successful registrations")
    AUTH_REGISTER_CONFLICT = Counter("auth_register_conflict_total", "Registration conflicts")
    REQ_LATENCY = Histogram(
        "auth_request_latency_seconds",
        "Latency of auth endpoints",
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
    )


def _latency():
    class _CM:
        def __enter__(self):
            self.t0 = time.perf_counter()

        def __exit__(self, exc_type, exc, tb):
            if PROM_AVAILABLE:
                REQ_LATENCY.observe(max(time.perf_counter() - self.t0, 0))
    return _CM()


# ---------- DB ----------

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('viewer','operator')),
        created_at TEXT NOT NULL,
        last_login TEXT
      )
    """)

    cur.execute("""
      CREATE TABLE IF NOT EXISTS invites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('viewer','operator')),
        created_by INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        used_by INTEGER,
        used_at TEXT,
        expires_at TEXT
      )
    """)

    # таблица профиля нагрузки для loadgen
    cur.execute("""
      CREATE TABLE IF NOT EXISTS load_profile (
        id INTEGER PRIMARY KEY CHECK(id=1),
        profile TEXT NOT NULL CHECK(profile IN ('off','low','med','high')),
        updated_at TEXT NOT NULL,
        updated_by INTEGER
      )
    """)

    conn.commit()

    # дефолтный админ
    cur.execute("SELECT id FROM users WHERE email=?", (ADMIN_EMAIL,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users(email, password_hash, role, created_at) VALUES(?,?,?,?)",
            (ADMIN_EMAIL, argon2.hash(ADMIN_PASSWORD), "operator", datetime.datetime.utcnow().isoformat())
        )
        conn.commit()

    # дефолтный профиль нагрузки: off (при запуске системы нагрузки нет)
    cur.execute("SELECT profile FROM load_profile WHERE id=1")
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO load_profile(id, profile, updated_at, updated_by) VALUES(1, 'off', ?, NULL)",
            (datetime.datetime.utcnow().isoformat(),)
        )
        conn.commit()

    conn.close()


# ---------- JWT ----------

def create_token(payload: dict, hours=8):
    now = datetime.datetime.utcnow()
    payload = dict(payload)
    payload.update({"iat": now, "exp": now + datetime.timedelta(hours=hours)})
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_token(token: str):
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])


def _get_token_from_req():
    h = request.headers.get("Authorization", "")
    if h.lower().startswith("bearer "):
        return h.split(" ", 1)[1].strip()
    c = request.cookies.get("auth_token")
    if c:
        return c
    return None


# ---------- Base ----------

@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"}), 200


if PROM_AVAILABLE:
    @app.get("/metrics")
    def metrics():
        data = generate_latest()
        return app.response_class(data, mimetype=CONTENT_TYPE_LATEST)


@app.get("/login")
def login_page():
    return render_template("login.html")


@app.get("/register")
def register_page():
    return render_template("register.html")


# ---------- Auth ----------

@app.post("/auth/login")
def login():
    with _latency():
        data = request.get_json(silent=True) or request.form or {}
        email, password = data.get("email"), data.get("password")
        if PROM_AVAILABLE:
            AUTH_LOGIN_ATTEMPTS.inc()
        if not email or not password:
            return jsonify({"error": "email and password required"}), 400

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id, password_hash, role FROM users WHERE email=?", (email,))
        row = cur.fetchone()
        if not row or not argon2.verify(password, row[1]):
            conn.close()
            if PROM_AVAILABLE:
                AUTH_LOGIN_FAILURE.inc()
            return jsonify({"error": "invalid credentials"}), 401

        user_id, _, role = row
        cur.execute(
            "UPDATE users SET last_login=? WHERE id=?",
            (datetime.datetime.utcnow().isoformat(), user_id)
        )
        conn.commit()
        conn.close()

        if PROM_AVAILABLE:
            AUTH_LOGIN_SUCCESS.inc()

        token = create_token({"sub": str(user_id), "email": email, "role": role})
        resp = make_response(jsonify({"token": token, "role": role}))
        resp.set_cookie("auth_token", token, httponly=True, samesite="Lax", max_age=8 * 3600)
        return resp


@app.post("/auth/logout")
def logout():
    resp = redirect("/login")
    resp.delete_cookie("auth_token")
    return resp


@app.get("/auth/me")
def me():
    with _latency():
        t = _get_token_from_req()
        if not t:
            return jsonify({"error": "no token"}), 401
        try:
            payload = decode_token(t)
        except Exception as e:
            return jsonify({"error": str(e)}), 401

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT email, role, created_at, last_login FROM users WHERE id=?",
            (payload.get("sub"),)
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({"error": "user not found"}), 404
        return jsonify({
            "email": row[0],
            "role": row[1],
            "created_at": row[2],
            "last_login": row[3],
        })


# ---------- Invites & Register ----------

def require_operator(role: str) -> bool:
    return role == "operator"


@app.post("/auth/invite")
def invite_create():
    t = _get_token_from_req()
    if not t:
        return ("no token", 401)
    try:
        payload = decode_token(t)
    except Exception as e:
        return (str(e), 401)

    if not require_operator(payload.get("role")):
        return ("forbidden", 403)

    data = request.get_json(silent=True) or {}
    role = data.get("role", "viewer")
    if role not in ("viewer", "operator"):
        return jsonify({"error": "invalid role"}), 400

    code = "INV-" + secrets.token_hex(8)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO invites(code, role, created_by, created_at) VALUES(?,?,?,?)",
        (code, role, payload.get("sub"), datetime.datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()
    return jsonify({"code": code}), 201


@app.post("/auth/register")
def register():
    with _latency():
        data = request.get_json(silent=True) or {}
        email, password, code = (
            data.get("email"),
            data.get("password"),
            data.get("invite_code"),
        )
        if not email or not password or not code:
            return jsonify({"error": "email, password and invite_code required"}), 400

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id, role, used_by FROM invites WHERE code=?", (code,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "invalid invite"}), 400
        inv_id, inv_role, used = row
        if used:
            conn.close()
            if PROM_AVAILABLE:
                AUTH_REGISTER_CONFLICT.inc()
            return jsonify({"error": "invite already used"}), 409

        try:
            cur.execute(
                "INSERT INTO users(email, password_hash, role, created_at) VALUES(?,?,?,?)",
                (email, argon2.hash(password), inv_role, datetime.datetime.utcnow().isoformat())
            )
            user_id = cur.lastrowid
            cur.execute(
                "UPDATE invites SET used_by=?, used_at=? WHERE id=?",
                (user_id, datetime.datetime.utcnow().isoformat(), inv_id)
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            if PROM_AVAILABLE:
                AUTH_REGISTER_CONFLICT.inc()
            return jsonify({"error": "email already exists"}), 409

        conn.close()
        if PROM_AVAILABLE:
            AUTH_REGISTER_SUCCESS.inc()
        return jsonify({"status": "ok"}), 201


# ---------- Load profile API ----------

@app.get("/load/status")
def load_status():
    """Вернёт текущий профиль нагрузки + текст на английском для портала."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT profile, updated_at FROM load_profile WHERE id=1")
    row = cur.fetchone()
    conn.close()

    profile = row[0] if row else "off"

    text_map = {
        "off": "Load generator is OFF. No synthetic traffic is sent to MySQL.",
        "low": "Load generator is in LOW mode (about 200 operations per second).",
        "med": "Load generator is in MEDIUM mode (about 2000 operations per second).",
        "high": "Load generator is in HIGH mode (about 10000 operations per second).",
    }

    return jsonify({
        "profile": profile,
        "status_text": text_map.get(profile, "Unknown load profile.")
    })


@app.post("/load/set")
def load_set():
    """Устанавливает профиль нагрузки: off / low / med / high (только оператор)."""
    t = _get_token_from_req()
    if not t:
        return ("no token", 401)
    try:
        payload = decode_token(t)
    except Exception as e:
        return (str(e), 401)

    if not require_operator(payload.get("role")):
        return ("forbidden", 403)

    data = request.get_json(silent=True) or {}
    profile = data.get("profile", "off")

    if profile not in ("off", "low", "med", "high"):
        return jsonify({"error": "invalid profile"}), 400

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE load_profile SET profile=?, updated_at=?, updated_by=? WHERE id=1",
        (profile, datetime.datetime.utcnow().isoformat(), payload.get("sub"))
    )
    conn.commit()
    conn.close()

    return jsonify({"profile": profile})


# ---------- Portal ----------

@app.get("/portal")
def portal():
    t = _get_token_from_req()
    if not t:
        return redirect("/login")
    try:
        payload = decode_token(t)
    except Exception:
        return redirect("/login")

    email = payload.get("email", "")
    role = payload.get("role", "viewer")
    return render_template("portal.html", email=email, role=role)


# ---------- Verify for nginx ----------

@app.get("/auth/verify")
def verify():
    t = _get_token_from_req()
    if not t:
        return ("no token", 401)
    try:
        p = decode_token(t)
    except Exception as e:
        return (str(e), 401)

    resp = make_response("ok")
    resp.headers["X-Web-User"] = p.get("email", "")
    resp.headers["X-Web-Role"] = p.get("role", "viewer")
    return resp, 200


# ---------- Entrypoint ----------

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8080)
