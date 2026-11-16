#!/usr/bin/env bash
# Producer-alone throughput, Java vs Rust, alternating rounds on a fresh Iggy each run (Flink not running).
# Usage: bench/producer_compare.sh [rounds] [data-dir] [csv]      (run from the repository root; wipes Iggy)
set -euo pipefail
ROUNDS=${1:-3}
export DATA_DIR=${2:-./data}
export CSV_FILE=/data/${3:-debs2022-gc-trading-day-08-11-21.csv}

docker compose rm -sf jobmanager taskmanager >/dev/null 2>&1 || true
PRODUCER_DIR=producer docker compose build iggy-setup >/dev/null
PRODUCER_DIR=producer-rs docker compose build iggy-setup >/dev/null

run() {  # $1 = label, $2 = producer dir, $3 = IGGY_TCP_NODELAY
  export PRODUCER_DIR=$2 IGGY_TCP_NODELAY=$3
  docker compose down -v >/dev/null 2>&1
  docker compose up -d iggy >/dev/null 2>&1
  docker compose run --rm --no-deps iggy-setup >/dev/null 2>&1
  local line secs n
  line=$(docker compose run --rm --no-deps replay-producer 2>&1 | grep 'replay done')
  n=$(sed -E 's/.*sent ([0-9]+) messages.*/\1/' <<<"$line")
  secs=$(sed -E 's/.* in ([0-9.]+) s.*/\1/' <<<"$line")
  printf '%s\t%s\t%s\t%.0f\n' "$1" "$n" "$secs" "$(echo "$n / $secs" | bc -l)"
}

printf 'producer\tmessages\tseconds\tmsg_per_s\n'
for r in $(seq 1 "$ROUNDS"); do
  run java  producer    true
  run rust  producer-rs true
  if [[ -n "${WITH_NAGLE:-}" ]]; then run rust-nagle producer-rs false; fi   # TCP_NODELAY off; made no difference
done
