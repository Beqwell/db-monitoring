#!/usr/bin/env bash
echo "[demo] stopping mysql for 70s..."
docker compose stop mysql
sleep 70
echo "[demo] starting mysql..."
docker compose start mysql
