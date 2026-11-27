# Subscriber sweep

10-0900-1200-all.csv (22,994,000 rows) at 30×, mode all, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 4,074 | 4,074 | 657 (34) | 3,417 | 11 / 101 / 104 | 4 / 100 / 103 | 1 / 10 / 11 | 1 / 8 / 8 | 233 / 345 / 351 | Source:_kafka_source 9/0 | 40 / 45 / 0 | 0 / 0 | 99 → 99 → 100 |
| 10 | 10 | 4,074 | 40,740 | 6,570 (34) | 32,573 | 14 / 100 / 101 | 7 / 99 / 100 | 1 / 11 / 13 | 1 / 12 / 18 | 803 / 1,172 / 1,176 | Source:_kafka_source 9/0 | 43 / 46 / 0 | 0 / 0 | 98 → 98 → 99 |
| 100 | 100 | 4,074 | 407,400 | 65,700 (34) | 103,490 | 66 / 290 / 294 | 26 / 110 / 119 | 3 / 30 / 36 | 13 / 171 / 173 | 4,529 / 6,095 / 6,134 | ema 14/0 | 53 / 45 / 2 | 0 / 0 | 100 → 99 → 100 |
