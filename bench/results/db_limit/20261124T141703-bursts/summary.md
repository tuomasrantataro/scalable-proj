# Database load at paced traffic

ms, without the first close. Writer busy and EMA backpressured as a share of time.

| Point | Events/s | Rows per close (p50) | Rows/s (average over the run) | Writer busy avg / max subtask | EMA backpressured | DB CPU % avg | DB CPU % max | fired → EMA p50 / p99 / max | fired → written p50 / p99 / max | Close stored, p50 / max (first fired → last row written) |
|---|---|---|---|---|---|---|---|---|---|---|
| real symbols | 20,761 | 3,136 | 89 | 0.0% / 0.0% | 0.0% | 3 | 5 | 3 / 52 / 52 | 927 / 1,056 / 1,109 | 1,040 / 1,111 |
| 5,000 symbols | 20,761 | 5,000 | 139 | 0.2% / 0.4% | 0.0% | 3 | 6 | 2 / 61 / 99 | 12 / 1,054 / 1,055 | 925 / 1,059 |
| 10,000 symbols | 20,761 | 10,000 | 278 | 0.5% / 0.8% | 0.0% | 3 | 9 | 2 / 71 / 92 | 49 / 1,207 / 1,210 | 573 / 1,223 |
| 20,000 symbols | 20,761 | 20,000 | 555 | 1.3% / 1.5% | 0.8% | 4 | 19 | 5 / 507 / 696 | 100 / 1,694 / 1,711 | 828 / 1,734 |
| 40,000 symbols | 20,761 | 40,000 | 1,110 | 2.2% / 2.3% | 1.6% | 6 | 26 | 115 / 625 / 671 | 227 / 1,264 / 1,686 | 1,351 / 1,788 |
| 80,000 symbols | 20,761 | 79,757 | 2,214 | 3.0% / 3.4% | 2.6% | 10 | 44 | 224 / 765 / 815 | 311 / 904 / 1,217 | 1,586 / 1,653 |
