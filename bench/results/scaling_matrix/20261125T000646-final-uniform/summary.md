# Scaling matrix

289,116,808 records (uniform), 8 partitions, settings default, host with 16 CPUs. Medians over the valid runs; baseline 4x1, digest `e02159f44f5ae3ca2863ef5dec6992c4`.

| TaskManagers × slots | Parallelism | Runs | Time (s) | Min–max | Spread | Records/s | Speedup | Efficiency | Busiest task (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka | Memory MB (same) | Skew events / windows / busy | Advisory latency p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 × 1 | 4 | 3 | 89.3 | 85.1–103.1 | 20% | 3,237,191 | 1.00 | 100% | Source:_kafka_source 609 | 740 / 91 / 106 | 3,823 / 197 / 1,294 | 1.05 / 1.05 / 1.04 | – / – |
| 8 × 1 | 8 | 3 | 104.3 | 99.7–106.6 | 7% | 2,772,585 | 0.86 | 43% | Source:_kafka_source 468 | 972 / 98 / 98 | 6,836 / 222 / 1,284 | 1.12 / 1.12 / 1.05 | – / – |
| 2 × 4 | 8 | 3 | 85.0 | 81.7–88.0 | 7% | 3,400,854 | 1.05 | 53% | Source:_kafka_source 536 | 836 / 111 / 108 | 2,179 / 221 / 1,258 | 1.12 / 1.12 / 1.02 | – / – |

