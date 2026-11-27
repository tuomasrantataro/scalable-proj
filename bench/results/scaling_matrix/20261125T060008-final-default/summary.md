# Scaling matrix

289,116,808 records (real), 8 partitions, settings default, host with 16 CPUs. Medians over the valid runs; baseline 4x1, digest `19413a509d53044973e3e6b76c177af1`.

| TaskManagers × slots | Parallelism | Runs | Time (s) | Min–max | Spread | Records/s | Speedup | Efficiency | Busiest task (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka | Memory MB (same) | Skew events / windows / busy | Advisory latency p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 × 1 | 4 | 3 | 91.0 | 87.8–91.2 | 4% | 3,176,026 | 1.00 | 100% | Source:_kafka_source 641 | 740 / 101 / 111 | 3,794 / 266 / 1,283 | 1.10 / 1.03 / 1.03 | 33 / 636 |

