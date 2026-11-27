# Scaling matrix

289,116,808 records (real), 8 partitions, settings {'JDBC_URL': 'jdbc:postgresql://timescaledb:5432/trading'}, host with 16 CPUs. Medians over the valid runs; baseline 4x1, digest `19413a509d53044973e3e6b76c177af1`.

| TaskManagers × slots | Parallelism | Runs | Time (s) | Min–max | Spread | Records/s | Speedup | Efficiency | Busiest task (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka | Memory MB (same) | Skew events / windows / busy | Advisory latency p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 × 1 | 4 | 3 | 135.0 | 131.3–135.6 | 3% | 2,141,844 | 1.00 | 100% | timescaledb_ema:_Writer 902 | 532 / 280 / 109 | 3,704 / 258 / 1,408 | 1.10 / 1.03 / 1.06 | 598 / 2158 |

