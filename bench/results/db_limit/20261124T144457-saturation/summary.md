# Database capacity: stored backlog with and without the database

Read = until the last record was consumed (Prometheus, 5 s resolution).

| Workload | ema_window | Records | Read (s) | Records/s | Rows | Rows/s (to the last row) | Writer busy | EMA backpressured | DB CPU % | TaskManager CPU % |
|---|---|---|---|---|---|---|---|---|---|---|
| real week | jdbc | 289,116,808 | 89.4 | 3,234,873 | 1,855,645 | 21,676 | 36% | 14% | 97 | 636 |
| real week | jdbc | 289,116,808 | 85.0 | 3,401,014 | 1,855,645 | 22,642 | 41% | 17% | 110 | 647 |
| real week | discard | 289,116,808 | 77.1 | 3,748,873 | – | – | – | 0% | 5 | 706 |
| real week | discard | 289,116,808 | 83.4 | 3,467,668 | – | – | – | 0% | 4 | 619 |
| uniform 10,000 symbols, one day | jdbc | 53,994,554 | 37.7 | 1,433,737 | 1,469,927 | 33,987 | 62% | 48% | 141 | 539 |
| uniform 10,000 symbols, one day | discard | 53,994,554 | 29.5 | 1,829,332 | – | – | – | 0% | 3 | 665 |
| uniform 40,000 symbols, one day | jdbc | 53,994,554 | 86.1 | 626,823 | 5,017,657 | 52,856 | 81% | 71% | 229 | 362 |
| uniform 40,000 symbols, one day | discard | 53,994,554 | 33.2 | 1,627,666 | – | – | – | 0% | 4 | 765 |
