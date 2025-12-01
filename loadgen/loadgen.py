import os
import time
import random
import json
import urllib.request
import threading

import pymysql
from pymysql import err as pymysql_err

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3307"))
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASS = os.getenv("DB_PASS", "app123")
DB_NAME = os.getenv("DB_NAME", "appdb")

LOAD_STATUS_URL = os.getenv("LOAD_STATUS_URL", "http://auth-svc:8080/load/status")

WORKERS = int(os.getenv("LOAD_WORKERS", "5"))

# LOW  ~  200 ops/sec
# MED  ~ 2000 ops/sec
# HIGH ~ 10000 ops/sec
RATES = {"low": 200, "med": 2000, "high": 10000}

_current_profile = "off"
_last_fetch = 0.0
_stop_flag = False
_profile_lock = threading.Lock()


def connect():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def do_insert(c):
    with c.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer_id, amount, status) VALUES (%s,%s,'NEW')",
            (random.randint(1, 3), round(random.uniform(5, 200), 2)),
        )


def do_update(c):
    """
    Обновляем одну строку со статусом не PAID.
    Такой вариант сильно уменьшает шанс дедлоков.
    """
    with c.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
            SET status = 'PAID'
            WHERE status <> 'PAID'
            ORDER BY id ASC
            LIMIT 1
            """
        )


def do_select(c):
    with c.cursor() as cur:
        cur.execute(
            """
            SELECT o.id, o.amount, c.name
            FROM orders o
            JOIN customers c ON c.id = o.customer_id
            ORDER BY o.id DESC
            LIMIT 50
            """
        )
        cur.fetchall()


def do_ping(c):
    """Очень лёгкий запрос, чтобы можно было накрутить тысячи QPS."""
    with c.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()


def fetch_profile():
    """Тянет профиль нагрузки с auth-svc, вызывается из одного потока-пуллера."""
    global _current_profile, _last_fetch
    while not _stop_flag:
        now = time.time()
        try:
            with urllib.request.urlopen(LOAD_STATUS_URL, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                prof = data.get("profile", "off")
                if prof in ("off", "low", "med", "high"):
                    with _profile_lock:
                        _current_profile = prof
        except Exception as e:
            print(f"[loadgen] failed to fetch profile: {e}")
        _last_fetch = now
        time.sleep(2.0)  


def get_profile():
    with _profile_lock:
        return _current_profile


def worker_loop(worker_id: int):
    conn = None
    last_profile = None

    try:
        while not _stop_flag:
            profile = get_profile()

            if profile == "off":
                if last_profile != profile:
                    print(f"[worker {worker_id}] profile=off (idle)")
                    last_profile = profile
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                time.sleep(0.5)
                continue

            total_ops = RATES.get(profile, 200)
            ops_per_sec = max(total_ops // max(WORKERS, 1), 1)

            if last_profile != profile:
                print(f"[worker {worker_id}] profile={profile} ~{ops_per_sec} ops/sec (per worker)")
                last_profile = profile

            if conn is None:
                try:
                    conn = connect()
                    print(f"[worker {worker_id}] connected to DB")
                except pymysql_err.OperationalError as e:
                    print(f"[worker {worker_id}] DB connect failed: {e}. Retrying in 2s...")
                    time.sleep(2)
                    continue

            start = time.time()
            done = 0

            while done < ops_per_sec and not _stop_flag:
                try:
                    r = random.random()

                    if profile == "high":
                        if r < 0.7:
                            do_ping(conn)
                        elif r < 0.85:
                            do_select(conn)
                        elif r < 0.95:
                            do_insert(conn)
                        else:
                            do_update(conn)
                    elif profile == "med":
                        if r < 0.3:
                            do_ping(conn)
                        elif r < 0.6:
                            do_select(conn)
                        elif r < 0.8:
                            do_insert(conn)
                        else:
                            do_update(conn)
                    else:
                        # low
                        if r < 0.4:
                            do_select(conn)
                        elif r < 0.7:
                            do_insert(conn)
                        else:
                            do_update(conn)

                    done += 1

                except (pymysql_err.OperationalError, pymysql_err.InterfaceError) as e:
                    code = e.args[0] if getattr(e, "args", None) else None
                    if isinstance(e, pymysql_err.OperationalError) and code == 1213:

                        time.sleep(0.001)
                        continue

                    print(f"[worker {worker_id}] DB error: {e}. Closing connection and retrying...")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                    time.sleep(0.5)
                    break

                except Exception as e:
                    print(f"[worker {worker_id}] unexpected error: {e}")
                    time.sleep(0.001)

            elapsed = time.time() - start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

    except KeyboardInterrupt:
        print(f"[worker {worker_id}] interrupted")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def main():
    global _stop_flag

    print(f"[loadgen] starting with {WORKERS} workers")
    profile_thread = threading.Thread(target=fetch_profile, daemon=True)
    profile_thread.start()

    workers = []
    for i in range(WORKERS):
        t = threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
        t.start()
        workers.append(t)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("[loadgen] stopping...")
        _stop_flag = True
        for t in workers:
            t.join(timeout=2.0)


if __name__ == "__main__":
    main()
