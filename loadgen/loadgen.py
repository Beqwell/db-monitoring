import os
import time
import random
import pymysql
from pymysql import err as pymysql_err

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3307"))
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASS = os.getenv("DB_PASS", "app123")
DB_NAME = os.getenv("DB_NAME", "appdb")

PROFILE = os.getenv("PROFILE", "low")  # low | med | high
RATES = {"low": 2, "med": 15, "high": 80}


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
    with c.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
            SET status='PAID'
            WHERE id IN (
              SELECT id FROM (
                SELECT id FROM orders ORDER BY id DESC LIMIT 5
              ) x
            )
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


def main():
    ops_per_sec = RATES.get(PROFILE, 2)
    interval = 1.0 / max(ops_per_sec, 1)
    print(f"[loadgen] profile={PROFILE} ~{ops_per_sec} ops/sec. Ctrl+C to stop.")

    conn = None

    try:
        while True:
            if conn is None:
                try:
                    conn = connect()
                    print("[loadgen] connected to DB")
                except pymysql_err.OperationalError as e:
                    print(f"[loadgen] DB connect failed: {e}. Retrying in 2s...")
                    time.sleep(2)
                    continue

            try:
                r = random.random()
                if r < 0.5:
                    do_insert(conn)
                elif r < 0.8:
                    do_select(conn)
                else:
                    do_update(conn)
                time.sleep(interval)
            except (pymysql_err.OperationalError, pymysql_err.InterfaceError) as e:
                print(f"[loadgen] DB error: {e}. Closing connection and retrying...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
                time.sleep(1)
            except Exception as e:
                print(f"[loadgen] unexpected error: {e}")
                time.sleep(1)
    except KeyboardInterrupt:
        print("[loadgen] stopped.")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
