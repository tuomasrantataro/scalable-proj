# Subscriber sweep

10-0900-1200-all.csv (22,994,000 rows) at 30×, mode all, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 4,074 | 4,074 | 657 (34) | 3,417 | 13 / 101 / 101 | 5 / 99 / 100 | 1 / 7 / 7 | 1 / 5 / 5 | 231 / 402 / 402 | Source:_kafka_source 9/0 | 38 / 46 / 0 | 0 / 0 | 90 → 96 → 99 |
| 10 | 10 | 4,074 | 40,740 | 6,570 (34) | 20,566 | 16 / 101 / 102 | 8 / 100 / 100 | 1 / 17 / 19 | 1 / 15 / 17 | 860 / 1,366 / 1,379 | Source:_kafka_source 9/0 | 46 / 47 / 0 | 0 / 0 | 95 → 95 → 99 |
| 100 | 100 | 4,074 | 407,400 | 65,700 (34) | 112,768 | 68 / 318 / 327 | 23 / 103 / 106 | 3 / 52 / 56 | 16 / 209 / 214 | 4,687 / 6,253 / 6,286 | ema 14/0 | 49 / 47 / 2 | 0 / 0 | 100 → 89 → 99 |
