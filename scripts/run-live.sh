#!/usr/bin/env bash
# Runs the whole system live: starts every service (Kafka, TimescaleDB, Flink, Grafana, Prometheus, cAdvisor),
# then replays the CSV files into Kafka while the job processes them, as a real feed would arrive. Waits until
# the job has processed everything and prints a summary. The system keeps running afterwards (dashboards).
#
# Uses its own topic `live` (recreated empty each time) and a fresh consumer group, so the events loaded with
# bench/backlog.py (topic `ticks`) are kept. Results of earlier runs are removed first (bench/backlog.py reset).
#
#   scripts/run-live.sh                                  # whole week from ./data
#   DAYS="08" scripts/run-live.sh                        # one trading day
#   DATA_DIR=./docs FILES=pala.csv scripts/run-live.sh   # the 200-row sample in the repo
#   TASKMANAGERS=8 PARALLELISM=8 scripts/run-live.sh     # any setting from docker-compose.yml
set -euo pipefail
cd "$(dirname "$0")/.."

export DATA_DIR=${DATA_DIR:-./data}
DAYS=${DAYS:-"08 09 10 11 12 13 14"}
FILES=${FILES:-$(for d in $DAYS; do printf 'debs2022-gc-trading-day-%s-11-21.csv ' "$d"; done)}
for f in $FILES; do
  [ -f "$DATA_DIR/$f" ] || { echo "missing $DATA_DIR/$f" >&2; exit 1; }
done
export KAFKA_TOPIC=live
export KAFKA_GROUP="live-$(date -u +%Y%m%dT%H%M%S)"

kafka_tool() { docker compose exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server localhost:9092 "${@:2}"; }
psql_q() { docker compose exec -T timescaledb psql -U postgres -d trading -tA -c "$1"; }

echo "== fresh topic $KAFKA_TOPIC, results removed"
docker compose up -d --wait kafka >/dev/null 2>&1
kafka_tool kafka-topics.sh --delete --if-exists --topic "$KAFKA_TOPIC" >/dev/null
while kafka_tool kafka-topics.sh --list | grep -qx "$KAFKA_TOPIC"; do sleep 1; done
python3 bench/backlog.py reset   # stops Flink, empties tables, advisories and checkpoints; creates `live` (kafka-setup)
python3 bench/advisory_latency.py start   # a trader's system: receives the advisories, for latency

echo "== starting all services"
docker compose up -d --build
until curl -sf localhost:8081/jobs/overview | grep -q '"state":"RUNNING"'; do sleep 2; done
cat <<EOF
   Flink UI    http://localhost:8081
   Grafana     http://localhost:3001   (admin/admin; Trading trends, Correctness, System metrics)
   Prometheus  http://localhost:9090
EOF

echo "== replaying: $FILES"
for f in $FILES; do
  CSV_FILE="/data/$f" docker compose run --rm --no-deps replay-producer 2>&1 | grep "replay done" | sed "s|^|   $f: |"
done

echo "== waiting for the job to process everything"
last=""; stable=0
while [ "$stable" -lt 6 ]; do
  sleep 5
  lag=$(kafka_tool kafka-consumer-groups.sh --describe --group "$KAFKA_GROUP" 2>/dev/null \
        | awk -v t="$KAFKA_TOPIC" '$2 == t { if ($4 == "-") s += $5; else s += $6; n++ } END { print n ? s : -1 }')
  now=$(psql_q "SELECT count(*) || ' ' || (SELECT count(*) FROM data_issue) FROM ema_window")
  if [ "$lag" = "0" ] && [ "$now" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
  last=$now
done

echo "== done"
psql_q "SELECT count(*) || ' ema_window rows, ' || count(*) FILTER (WHERE advisory = 'BUY') || ' BUY, '
        || count(*) FILTER (WHERE advisory = 'SELL') || ' SELL' FROM ema_window" | sed 's/^/   /'
psql_q "SELECT reason || ' ' || events FROM data_issue_summary" | sed 's/^/   issue: /'
kafka_tool kafka-get-offsets.sh --topic advisories | awk -F: '{ s += $3 } END { print "   advisory messages: " s }'
python3 bench/advisory_latency.py stop --label "$KAFKA_GROUP"
echo "   The system keeps running; stop it with: docker compose stop   (or tear down: see README)"
