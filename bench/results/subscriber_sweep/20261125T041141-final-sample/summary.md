# Subscriber sweep

10-0900-1200-all.csv (22,994,000 rows) at 30×, mode sample, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 5,000 | 4,074 | 4,979 | 621 (5) | 2,850 | 19 / 24 / 77 | 8 / 16 / 75 | 5 / 11 / 11 | 4 / 7 / 9 | 247 / 384 / 385 | Source:_kafka_source 9/0 | 38 / 46 / 0 | 0 / 0 | 67 → 99 → 99 |
| 1000 | 50,000 | 4,074 | 49,735 | 6,184 (15) | 36,379 | 60 / 87 / 100 | 12 / 42 / 99 | 10 / 34 / 37 | 29 / 43 / 44 | 886 / 1,296 / 1,302 | Source:_kafka_source 9/0 | 50 / 48 / 0 | 0 / 0 | 99 → 98 → 99 |
