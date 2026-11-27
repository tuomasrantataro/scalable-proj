# Scaling matrix

289,116,808 records (real), 8 partitions, settings {'CHECKPOINT_INTERVAL': '1 s'}, host with 16 CPUs. Medians over the valid runs; baseline 4x1, digest `19413a509d53044973e3e6b76c177af1`.

| TaskManagers × slots | Parallelism | Runs | Time (s) | Min–max | Spread | Records/s | Speedup | Efficiency | Busiest task (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka | Memory MB (same) | Skew events / windows / busy | Advisory latency p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 × 1 | 4 | 3 | 88.9 | 86.4–94.6 | 9% | 3,250,695 | 1.00 | 100% | Source:_kafka_source 738 | 805 / 103 / 111 | 3,783 / 269 / 1,292 | 1.10 / 1.03 / 1.02 | 41 / 633 |

