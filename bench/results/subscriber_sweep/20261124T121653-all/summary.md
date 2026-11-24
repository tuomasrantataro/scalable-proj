# Subscriber sweep

10-0900-1730-all.csv (62,750,000 rows) at 100×, mode all, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 4,285 | 4,285 | 868 (90) | 1,718 | 6 / 96 / 100 | 3 / 95 / 99 | 1 / 4 / 5 | 1 / 3 / 4 | 435 / 740 / 741 | Source:_kafka_source 25/0 | 62 / 54 / 0 | 0 / 0 | 80 → 74 → 91 |
| 10 | 10 | 4,285 | 42,850 | 8,680 (90) | 25,683 | 13 / 100 / 101 | 9 / 99 / 100 | 1 / 43 / 46 | 1 / 18 / 21 | 1,370 / 1,811 / 1,815 | Source:_kafka_source 26/0 | 71 / 55 / 0 | 0 / 0 | 97 → 99 → 96 |
| 100 | 100 | 4,285 | 428,500 | 86,800 (90) spill! | 94,661 | 64 / 4,503 / 4,518 | 33 / 1,978 / 1,983 | 2 / 79 / 97 | 5 / 2,632 / 2,667 | 5,593 / 7,609 / 7,651 | Source:_kafka_source 27/0 | 69 / 56 / 1 | 0 / 0 | 100 → 90 → 90 |
