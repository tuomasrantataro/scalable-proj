#!/usr/bin/env bash
# Runs every measurement of FINAL_MEASUREMENTS.md (A1-A11) without attention: about 10 hours after one confirmation.
#
#   scripts/run-final.sh                # check the host, ask once, then run in the background
#   scripts/run-final.sh --test         # the same steps, shortened, on one day of data (about 1.5 h)
#   scripts/run-final.sh status         # what has been done, what is running   (status --test)
#   scripts/run-final.sh stop           # stop; the next start continues where it stopped
#   scripts/run-final.sh --yes          # do not ask
#
# It runs as a systemd user service with sleep inhibited, so it survives a closed terminal or editor, but not a logout.
# Every step has a time limit and its own log. A failure in A1-A5 stops the run, because later results would be worthless;
# a later step is tried twice, recorded as failed, and the run goes on. Finished steps are remembered: after an interruption,
# start it again. To begin anew, move bench/results/final away.
#
# A2 deletes everything the system has stored (Kafka, database, metrics); ./data and bench/results are kept.
# Left for you afterwards: the Grafana screenshots of A11. The system keeps running for them.
set -uo pipefail
main() {
cd "$(dirname "$0")/.." || exit 1

MODE=full; ACTION=start; YES=0
for arg in "$@"; do
  case $arg in
    --test) MODE=test ;;
    --yes) YES=1 ;;
    start|status|stop|worker) ACTION=$arg ;;
    *) sed -n '2,17p' scripts/run-final.sh | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
done
UNIT=run-final
WEEK_DIGEST=19413a509d53044973e3e6b76c177af1
DAY_DIGEST=a7ed875d2291c71a8151275953a15be4
WEEK_RECORDS=289,116,808
DAY_RECORDS=53,994,554

# --- what the steps use; the test runs the same commands on one day and with fewer points ---
export TASKMANAGERS=4 PARALLELISM=4 PRODUCER_THREADS=8 PYTHONUNBUFFERED=1
if [ $MODE = full ]; then
  P=final OUT=bench/results/final REF=reference/output-nozero DIGEST=$WEEK_DIGEST RECORDS=$WEEK_RECORDS
  REF_FILES="" THROUGH="" DAYS_ARG="" OFFLINE_DAYS="" LIVE_DAYS="08 09 10 11 12 13 14" ZERO="316,382" LATE="5,506"
  MATRIX="--configs 1x1,2x1,4x1,8x1,2x4" UNIFORM="--configs 4x1,8x1,2x4" REPEATS=3
  SWEEP="" SWEEP_REP="--speeds 30,100,300,0" REPS="1 2 3" DELAY="--values 60,5,1"
  PEAK="" SESSION="--scopes all --speeds 100,300,0 --before 390 --after 120"
  SUBS_ALL="" SUBS_SAMPLE="--traders 100,1000"
  DB_REAL="" DB_BURSTS="" DB_SAT="" FAIL_AFTER=30
else
  P=runtest OUT=bench/results/final-test REF=reference/output-runtest DIGEST=$DAY_DIGEST RECORDS=$DAY_RECORDS
  REF_FILES="--files debs2022-gc-trading-day-08-11-21.csv" THROUGH="--through 2021-11-08" DAYS_ARG="--days 08"
  OFFLINE_DAYS="--days 08" LIVE_DAYS="08" ZERO="62,212" LATE="0"
  MATRIX="--configs 2x1,4x1" UNIFORM="--configs 4x1" REPEATS=1
  SWEEP="--speeds 300,0" SWEEP_REP="--speeds 0" REPS="1" DELAY="--values 1 --end 09:12"
  PEAK="--scopes ETR --speeds 100 --days 08" SESSION="--scopes all --speeds 0 --days 08 --before 5 --after 30"
  SUBS_ALL="--traders 1 --day 08 --end 09:30 --speed 100" SUBS_SAMPLE="--traders 100 --day 08 --end 09:30 --speed 100"
  DB_REAL="--peak-speeds 10 --session-speeds 0" DB_BURSTS="--symbols 5000" DB_SAT="--symbols 10000" FAIL_AFTER=18
fi
ROOT=$PWD
LOGS=$OUT/logs DONE=$OUT/progress.txt MAIN_LOG=$OUT/run.log
export P OUT REF DIGEST RECORDS REF_FILES THROUGH DAYS_ARG OFFLINE_DAYS LIVE_DAYS ZERO LATE MATRIX UNIFORM REPEATS SWEEP \
       SWEEP_REP REPS DELAY PEAK SESSION SUBS_ALL SUBS_SAMPLE DB_REAL DB_BURSTS DB_SAT FAIL_AFTER LOGS MODE WEEK_DIGEST

case $ACTION in
  start) start ;;
  status) status ;;
  stop) stop ;;
  worker) worker ;;
esac
}

say() { echo "$(LC_ALL=C date '+%d %b %T') $*"; }
running() { systemctl --user is-active -q $UNIT 2>/dev/null; }

# --- start: checks, one question, then the service ---------------------------------------------------------------------

check_host() {
  local bad=0 free disk
  no() { echo "  NO   $*"; bad=1; }
  ok() { echo "  ok   $*"; }
  echo "Host:"
  local n; n=$(ls data/debs2022-gc-trading-day-{08,09,10,11,12,13,14}-11-21.csv 2>/dev/null | wc -l)
  [ "$n" = 7 ] && ok "7 CSV files in data/" || no "$n of 7 CSV files in data/"
  docker info >/dev/null 2>&1 && ok "Docker without sudo" || no "Docker does not answer (or needs sudo)"
  systemctl --user show-environment >/dev/null 2>&1 && ok "systemd user services" || no "no systemd user session"
  free=$(awk '/MemAvailable/ {print int($2 / 1048576)}' /proc/meminfo)
  [ "$free" -ge 12 ] && ok "$free GB of memory available" || no "$free GB of memory available, 12 needed: close other programs"
  local need=50; [ $MODE = test ] && need=25
  for d in . /var/lib/docker; do
    disk=$(df -BG --output=avail $d 2>/dev/null | tail -1 | tr -dc 0-9)
    [ -z "$disk" ] || { [ "$disk" -ge $need ] && ok "$disk GB free on the disk of $d" || no "$disk GB free on the disk of $d, $need needed"; }
  done
  local mains; mains=$(cat /sys/class/power_supply/*/online 2>/dev/null | sort -r | head -1)
  [ "${mains:-1}" = 1 ] && ok "mains power" || no "not on mains power"
  if [ -z "$(docker compose ps -q 2>/dev/null)" ]; then
    local used; used=$(ss -ltnH | awk '{print $4}' | grep -E ':(3001|5432|8081|9090|9094)$' | tr '\n' ' ')
    [ -z "$used" ] && ok "ports are free" || no "ports in use: $used"
  fi
  local others; others=$(docker ps --format '{{.Names}}' --filter label=com.docker.compose.project | grep -v "^$(basename "$PWD")-" | tr '\n' ' ')
  [ -z "$others" ] && ok "no other Compose projects running" || no "other containers are running: $others"
  if [ -n "$(git status --porcelain -- . ':!bench/results' 2>/dev/null)" ]; then
    [ $MODE = test ] && ok "uncommitted changes (allowed in a test)" || no "uncommitted changes: commit first, so that the report can name the version"
  else ok "version $(git rev-parse --short HEAD), nothing uncommitted"; fi
  return $bad
}

start() {
  running && { echo "already running: scripts/run-final.sh status"; exit 1; }
  check_host || { echo "Not started."; exit 1; }
  if [ ! -x reference/.venv/bin/python ]; then
    echo "Creating the Python environment ..."
    python3 -m venv reference/.venv && reference/.venv/bin/pip install -q -r reference/requirements.txt || exit 1
  fi
  local resume=0
  if [ -s $DONE ]; then
    resume=1
    local was; was=$(cat $OUT/commit.txt 2>/dev/null)
    if [ $MODE = full ] && [ "$was" != "$(git rev-parse HEAD)" ]; then
      echo "The earlier part was measured with version ${was:0:7}. Check that version out to continue, or move $OUT away to begin anew."
      exit 1
    fi
    echo; echo "Continues after: $(tail -1 $DONE | cut -d' ' -f1). Loaded events are kept."
  else
    echo
    echo "THIS DELETES everything the system has stored: loaded events, database, metrics, Grafana data."
    echo "./data and bench/results are kept. Do not edit files or use the host for other work; stay logged in."
  fi
  if [ $YES = 0 ]; then
    read -r -p "Start the $MODE run? Type yes: " answer
    [ "$answer" = yes ] || { echo "Not started."; exit 1; }
  fi
  mkdir -p $LOGS
  [ $resume = 1 ] || { git rev-parse HEAD > $OUT/commit.txt; touch $OUT/started
    stat -c '%s %n' bench/results/{backlog_runs,advisory_latency}.jsonl 2>/dev/null > $OUT/log-sizes.txt; }
  local flag=(); [ $MODE = test ] && flag=(--test)
  systemd-run --user --collect -q --unit=$UNIT -p OOMPolicy=continue -p KillMode=control-group -p TimeoutStopSec=60 \
    --working-directory="$PWD" --setenv=PATH="$PATH" --setenv=HOME="$HOME" \
    systemd-inhibit --what=idle:sleep:handle-lid-switch --who=run-final --why=measurements \
    bash scripts/run-final.sh worker "${flag[@]}" || exit 1
  echo "Started. Progress: scripts/run-final.sh status ${flag[*]}   or   tail -f $MAIN_LOG"
}

status() {
  running && echo "RUNNING" || echo "not running"
  [ -s $MAIN_LOG ] && grep -E '^[0-9]{2} [A-Z][a-z]{2} ' $MAIN_LOG
  return 0
}

stop() {
  running || { echo "not running"; return 0; }
  systemctl --user stop $UNIT
  remove_oneoffs
  echo "Stopped. Start it again to continue with the interrupted step."
}

# Producer and consumer containers that a killed step leaves behind.
remove_oneoffs() {
  docker ps -aq --filter "label=com.docker.compose.project=$(basename "$PWD")" --filter label=com.docker.compose.oneoff=True \
    | xargs -r docker rm -f >/dev/null 2>&1
  docker compose --profile latency rm -sf advisory-latency >/dev/null 2>&1
}

# --- helpers available in the steps ------------------------------------------------------------------------------------

# matrix NAME OPTIONS...: runs a scaling matrix, or the rest of it if this run has started it. All its runs must be right.
matrix() {
  local name=$1 d; shift
  d=$(find bench/results/scaling_matrix -maxdepth 1 -name "????????T??????-$name" -newer $OUT/started 2>/dev/null | sort | tail -1)
  if [ -n "$d" ] && [ -f "$d/info.json" ]; then
    python bench/scaling_matrix.py --resume "$d"
  else
    python bench/scaling_matrix.py --name "$name" --repeats "$REPEATS" "$@"
    d=$(find bench/results/scaling_matrix -maxdepth 1 -name "????????T??????-$name" -newer $OUT/started | sort | tail -1)
  fi
  python scripts/check_matrix.py "$d"
}

# The results in the database and the topic are those of the reference. $1 = prefix of the files.
check_results() {
  python reference/compare.py --reference $REF/windows.parquet $THROUGH | tee "$OUT/$1-compare.txt"
  python bench/check_delivery.py --reference $REF/advisories.csv $THROUGH | tee "$OUT/$1-delivery.txt"
}

check_events() {
  python bench/event_handling.py | tee "$OUT/$1-events.txt"
  grep -q "zero_price | $ZERO " "$OUT/$1-events.txt"
  grep -q "late (window already evaluated) | $LATE " "$OUT/$1-events.txt"
}

# Removes the lines that this run has added to the two append-only result files.
restore_logs() {
  local f size
  for f in bench/results/backlog_runs.jsonl bench/results/advisory_latency.jsonl; do
    size=$(awk -v n="$f" '$2 == n {print $1}' $OUT/log-sizes.txt)
    if [ -n "$size" ]; then truncate -s "$size" "$f"; else rm -f "$f"; fi
  done
}

wait_job() {
  for _ in $(seq 60); do
    python bench/backlog.py status > $LOGS/status.txt
    if grep -q 'job  *running' $LOGS/status.txt; then cat $LOGS/status.txt; return 0; fi
    sleep 3
  done
  echo "the job does not run" >&2
  return 1
}

# --- steps -------------------------------------------------------------------------------------------------------------

# step ID critical|optional LIMIT COMMANDS
step() {
  local id=$1 kind=$2 limit=$3 commands=$4 tries=2 try t0 code
  grep -q "^$id ok" $DONE 2>/dev/null && return 0
  [ "$kind" = critical ] && tries=1
  RAN=1
  for try in $(seq $tries); do
    say "$id: started$([ "$try" -gt 1 ] && echo ' again')"
    touch $OUT/attempt; t0=$SECONDS
    timeout -k 60 "$limit" bash -e -o pipefail -c "$commands" >> "$LOGS/$id.log" 2>&1 < /dev/null
    code=$?
    if [ $code = 0 ]; then
      sed -i "/^$id /d" $DONE 2>/dev/null
      echo "$id ok $(( (SECONDS - t0) / 60 )) min" >> $DONE
      say "$id: ok, $(( (SECONDS - t0) / 60 )) min"
      return 0
    fi
    say "$id: FAILED ($([ $code = 124 ] || [ $code = 137 ] && echo "over the limit of $limit" || echo "code $code")), see $LOGS/$id.log"
    remove_oneoffs
    if [ "$try" -lt $tries ]; then   # keep the failed attempt, but not under the name of a final result
      find bench/results -mindepth 2 -maxdepth 2 -type d -newer $OUT/attempt -name "????????T??????-$P*" \
        -not -path '*/scaling_matrix/*' -not -path '*/archive/*' -exec mv {} {}-FAILED \;
    fi
  done
  sed -i "/^$id /d" $DONE 2>/dev/null
  echo "$id FAILED" >> $DONE
  if [ "$kind" = critical ]; then
    say "stopped: without $id the later results are worthless. Fix the problem and start again."
    finish; exit 1
  fi
}

finish() {
  {
    echo "# Final measurements: $MODE run"
    echo
    echo "Version $(cut -c1-7 $OUT/commit.txt), $(date '+%Y-%m-%d %H:%M')."
    echo
    echo "| Step | Result | Log |"; echo "|---|---|---|"
    awk -v l="$LOGS" '{r = $2; for (i = 3; i <= NF; i++) r = r " " $i; print "| " $1 " | " r " | `" l "/" $1 ".log` |"}' $DONE
  } > $OUT/summary.md
}

worker() {
  export ROOT; export -f matrix check_results check_events wait_job restore_logs
  source reference/.venv/bin/activate
  mkdir -p $LOGS; touch $DONE
  exec >> $MAIN_LOG 2>&1
  say "$MODE run, version $(git rev-parse --short HEAD)"
  if grep -q '^A5-load ok' $DONE; then   # a continued run needs the events that were loaded before
    python bench/backlog.py status > $LOGS/loaded.txt 2>&1
    grep -q "^ticks  *$RECORDS records" $LOGS/loaded.txt || {
      say "topic ticks no longer holds $RECORDS records: loaded again"
      sed -i '/^A5-load /d; /^A7-load /d' $DONE; }
  fi

  step A1-host critical 5m '
    { git rev-parse --short HEAD; lscpu | grep -E "Model name|^CPU\(s\)"; free -g; docker --version; uname -r
      cat /sys/devices/system/clocksource/clocksource0/current_clocksource; } | tee $OUT/host.txt'
  step A2-start critical 45m '
    docker compose --profile producer --profile latency down -v --remove-orphans
    docker compose up -d --build
    wait_job'

  step A3-reference critical 60m '
    python -m pytest reference -q
    jobs=2; [ $(awk "/MemAvailable/ {print int(\$2 / 1048576)}" /proc/meminfo) -ge 20 ] || jobs=1
    python reference/ema_reference.py --exclude-zero-prices --out $REF --jobs $jobs $REF_FILES
    if [ $MODE = full ]; then
      python reference/compare.py --reference-digest > $LOGS/reference-digest.txt
      grep -q $WEEK_DIGEST $LOGS/reference-digest.txt
    fi'
  step A3-dataset critical 60m '
    if [ $MODE = full ]; then python bench/event_types.py; python bench/symbol_distribution.py
    else python bench/event_types.py $OFFLINE_DAYS --out $OUT/event_types
         python bench/symbol_distribution.py $OFFLINE_DAYS --out $OUT/symbol_distribution; fi'
  step A3-alternatives optional 60m '
    jobs=2; [ $(awk "/MemAvailable/ {print int(\$2 / 1048576)}" /proc/meminfo) -ge 20 ] || jobs=1
    if [ $MODE = full ]; then a=reference/output b=reference/output-batch; else a=$REF-zero b=$REF-batch; fi
    python reference/ema_reference.py --out $a --jobs $jobs $REF_FILES
    python reference/ema_reference.py --lateness inf --out $b --jobs $jobs $REF_FILES'

  step A4-smoke critical 60m '
    python bench/backlog.py load --data-dir ./data --days 08
    python bench/backlog.py run | tee $LOGS/smoke-run.txt
    grep -q "digest '$DAY_DIGEST'" $LOGS/smoke-run.txt
    python reference/compare.py --reference $REF/windows.parquet --through 2021-11-08
    python bench/check_delivery.py --reference $REF/advisories.csv --through 2021-11-08
    python bench/event_handling.py
    python bench/scaling_matrix.py --configs 4x1 --repeats 1 --name $P-smoke
    python bench/peak_test.py --scopes ETR --speeds 100 --days 08 --name $P-smoke
    python bench/subscriber_sweep.py --traders 1 --day 08 --start 09:00 --end 09:30 --speed 100 --name $P-smoke | tee $LOGS/smoke-subs.txt
    if grep -q "deliveries differ" $LOGS/smoke-subs.txt; then exit 1; fi
    grep -q " 1 |" bench/results/scaling_matrix/*-$P-smoke/summary.md
    rm -r bench/results/{scaling_matrix,peak_test,subscriber_sweep}/*-$P-smoke
    restore_logs'

  step A5-load critical 60m '
    python bench/backlog.py load --data-dir ./data $DAYS_ARG | tee $LOGS/load.txt
    grep -q "loaded $RECORDS records" $LOGS/load.txt'
  step A5-week critical 60m '
    python bench/backlog.py run | tee $OUT/a5-run.txt
    grep -q "digest $DIGEST" $OUT/a5-run.txt
    check_results a5
    check_events a5'

  step A6-scaling optional 4h 'matrix $P $MATRIX'
  step A7-load optional 60m 'python bench/backlog.py load --data-dir ./data --workload uniform $DAYS_ARG'
  step A7-uniform optional 3h 'matrix $P-uniform --workload uniform $UNIFORM'

  step A8-sweep optional 3h 'python bench/latency_sweep.py --end 09:36 --name $P $SWEEP'
  for i in $REPS; do
    step A8-rep$i optional 60m "python bench/latency_sweep.py --end 09:36 --name \$P-rep$i \$SWEEP_REP"
  done
  step A8-delay optional 2h 'python bench/window_delay.py --name $P $DELAY'

  step A9-peaks optional 4h 'python bench/peak_test.py --name $P $PEAK'
  step A9-session optional 90m '
    python bench/peak_test.py --name $P-session $SESSION
    d=$(find bench/results/peak_test -maxdepth 1 -name "????????T??????-$P-session" | sort | tail -1)
    python bench/stability.py plot $d'

  step A10-all optional 2h '
    python bench/subscriber_sweep.py --name $P-all $SUBS_ALL | tee $LOGS/subs-all.txt
    if grep -q "deliveries differ" $LOGS/subs-all.txt; then exit 1; fi'
  step A10-sample optional 2h '
    python bench/subscriber_sweep.py --mode sample --name $P-sample $SUBS_SAMPLE | tee $LOGS/subs-sample.txt
    if grep -q "deliveries differ" $LOGS/subs-sample.txt; then exit 1; fi'

  step A10b-realistic optional 3h 'python bench/db_limit.py realistic --name $P-realistic $DB_REAL'
  step A10b-bursts optional 2h 'python bench/db_limit.py bursts --name $P-bursts $DB_BURSTS'
  step A10b-saturation optional 90m 'python bench/db_limit.py saturation --name $P-saturation $DB_SAT'

  step A10c-default optional 60m 'matrix $P-default --configs 4x1'
  step A10c-no-multirow optional 60m 'matrix $P-no-multirow --configs 4x1 --set JDBC_URL=jdbc:postgresql://timescaledb:5432/trading'
  step A10c-checkpoint1s optional 60m 'matrix $P-checkpoint1s --configs 4x1 --set "CHECKPOINT_INTERVAL=1 s"'
  step A10c-no-alignment optional 60m '
    python bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0 | tee $OUT/a10c-no-alignment-1.txt
    python bench/backlog.py run --set WATERMARK_ALIGNMENT_S=0 | tee $OUT/a10c-no-alignment-2.txt'
  step A10c-failover optional 30m '
    python bench/failover.py --after $FAIL_AFTER | tee $OUT/a10c-failover.txt
    grep -q "digest $DIGEST" $OUT/a10c-failover.txt
    check_results a10c-failover'

  # A11 comes last: any other step replaces its results in the database, which the screenshots need.
  [ -z "${RAN:-}" ] || sed -i '/^A11-live /d' $DONE
  step A11-live optional 60m '
    DAYS="$LIVE_DAYS" SUBSCRIPTIONS_FILE=/config/subscriptions-all.txt scripts/run-live.sh | tee $OUT/a11-live.txt
    check_results a11
    check_events a11
    python bench/stability.py record --name $P-live'

  finish
  [ $MODE = full ] || restore_logs
  local failed; failed=$(grep -c ' FAILED' $DONE)
  say "finished: $(grep -c ' ok' $DONE) steps ok, $failed failed; see $OUT/summary.md"
  [ "$failed" = 0 ]
}

main "$@"
exit
