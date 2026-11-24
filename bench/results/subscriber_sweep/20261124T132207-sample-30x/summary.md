# Subscriber sweep

10-0900-1200-all.csv (22,994,000 rows) at 30×, mode sample, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 5,000 | 4,074 | 4,979 | 621 (5) | 4,358 | 11 / 22 / 68 | 8 / 19 / 67 | 2 / 6 / 8 | 1 / 3 / 3 | 268 / 364 / 365 | Source:_kafka_source 9/0 | 39 / 46 / 0 | 0 / 0 | 98 → 72 → 99 |
| 1000 | 50,000 | 4,074 | 49,735 | 6,184 (15) | 24,063 | 59 / 83 / 100 | 25 / 45 / 98 | 10 / 26 / 28 | 17 / 31 / 31 | 1,058 / 1,638 / 1,651 | Source:_kafka_source 9/0 | 49 / 46 / 0 | 0 / 0 | 95 → 93 → 99 |
