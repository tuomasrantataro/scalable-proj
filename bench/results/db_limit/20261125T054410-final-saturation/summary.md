# Database capacity: stored backlog with and without the database

Read = until the last record was consumed (Prometheus, 5 s resolution).

| Workload | ema_window | Records | Read (s) | Records/s | Rows | Rows/s (to the last row) | Writer busy | EMA backpressured | DB CPU % | TaskManager CPU % |
|---|---|---|---|---|---|---|---|---|---|---|
| real week | jdbc | 289,116,808 | 85.4 | 3,386,355 | 1,855,645 | 22,432 | 44% | 18% | 111 | 699 |
| real week | jdbc | 289,116,808 | 84.8 | 3,411,327 | 1,855,645 | 22,368 | 42% | 16% | 110 | 667 |
| real week | discard | 289,116,808 | 69.3 | 4,173,285 | – | – | – | 0% | 5 | 712 |
| real week | discard | 289,116,808 | 84.1 | 3,438,837 | – | – | – | 0% | 4 | 580 |
| uniform 10,000 symbols, one day | jdbc | 53,994,554 | 37.5 | 1,441,123 | 1,469,927 | 33,770 | 72% | 56% | 168 | 650 |
| uniform 10,000 symbols, one day | discard | 53,994,554 | 26.5 | 2,036,685 | – | – | – | 0% | 4 | 846 |
| uniform 40,000 symbols, one day | jdbc | 53,994,554 | 86.3 | 625,523 | 5,017,657 | 52,626 | 84% | 72% | 237 | 353 |
| uniform 40,000 symbols, one day | discard | 53,994,554 | 34.2 | 1,576,621 | – | – | – | 0% | 4 | 804 |
