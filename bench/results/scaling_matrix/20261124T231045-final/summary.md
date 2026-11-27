# Scaling matrix

289,116,808 records (real), 8 partitions, settings default, host with 16 CPUs. Medians over the valid runs; baseline 1x1, digest `19413a509d53044973e3e6b76c177af1`.

| TaskManagers × slots | Parallelism | Runs | Time (s) | Min–max | Spread | Records/s | Speedup | Efficiency | Busiest task (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka | Memory MB (same) | Skew events / windows / busy | Advisory latency p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 × 1 | 1 | 3 | 135.7 | 132.4–135.8 | 3% | 2,130,527 | 1.00 | 100% | Source:_kafka_source 751 | 218 / 44 / 89 | 1,044 / 172 / 1,132 | 1.00 / 1.00 / 1.00 | 67 / 527 |
| 2 × 1 | 2 | 3 | 111.3 | 84.3–115.0 | 28% | 2,598,055 | 1.22 | 61% | Source:_kafka_source 566 | 357 / 58 / 95 | 2,028 / 184 / 1,066 | 1.02 / 1.00 / 1.00 | 29 / 480 |
| 4 × 1 | 4 | 3 | 80.9 | 77.8–89.3 | 14% | 3,573,446 | 1.68 | 42% | Source:_kafka_source 698 | 779 / 109 / 105 | 3,818 / 197 / 1,127 | 1.10 / 1.03 / 1.02 | 66 / 690 |
| 8 × 1 | 8 | 3 | 101.8 | 99.3–107.5 | 8% | 2,840,549 | 1.33 | 17% | Source:_kafka_source 453 | 963 / 101 / 94 | 7,113 / 231 / 1,128 | 1.15 / 1.08 / 1.12 | 21 / 396 |
| 2 × 4 | 8 | 3 | 78.5 | 78.0–85.4 | 9% | 3,685,129 | 1.73 | 22% | Source:_kafka_source 820 | 929 / 126 / 106 | 2,192 / 229 / 1,178 | 1.15 / 1.08 / 1.12 | 35 / 206 |

