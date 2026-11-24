# Subscriber sweep

10-0900-1730-all.csv (62,750,000 rows) at 100×, mode all, 4 TaskManagers, parallelism 4. Latency: window fired → received, p50 / p99 / max in ms, without the first window close; stages as p50 / p99 / max.

| Traders | Subscriptions | Advisories | Deliveries | Steady messages (closes) | Max/s | Total latency | fired → emitted | emitted → stored | stored → received | First close (cold) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / Kafka / consumer | Lag max / end (records) | Window p95 per third |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 1,000 | 4,285 | 4,285,000 | 868,000 (90) spill! | 112,616 | 13,582 / 42,351 / 42,681 | 90 / 13,421 / 13,488 | 9 / 463 / 584 | 13,538 / 28,915 / 29,045 | 26,367 / 43,542 / 44,610 | ema 63/1 | 102 / 58 / 14 | 18,713 / 0 | 9,915 → 92 → 98 |
