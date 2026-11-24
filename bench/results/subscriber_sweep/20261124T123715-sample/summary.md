# Subscriber sweep

10-0900-1730-all.csv (62,750,000 rows) at 100×, mode sample, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 5,000 | 4,285 | 4,977 | 599 (7) | 4,193 | 22 / 58 / 101 | 8 / 57 / 100 | 6 / 15 / 18 | 5 / 8 / 8 | 352 / 575 / 577 | Source:_kafka_source 25/0 | 60 / 54 / 0 | 444 / 0 | 92 → 99 → 99 |
| 1000 | 50,000 | 4,285 | 49,752 | 6,168 (15) | 20,529 | 233 / 294 / 296 | 82 / 191 / 195 | 21 / 100 / 103 | 86 / 157 / 158 | 1,235 / 2,177 / 2,208 | Source:_kafka_source 27/0 | 69 / 53 / 0 | 0 / 0 | 98 → 91 → 75 |
