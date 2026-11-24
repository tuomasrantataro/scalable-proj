# Database load at paced traffic

ms, without the first close. Writer busy and EMA backpressured as a share of time.

| Point | Events/s | Rows per close (p50) | Rows/s (average over the run) | Writer busy avg / max subtask | EMA backpressured | DB CPU % avg | DB CPU % max | fired → EMA p50 / p99 / max | fired → written p50 / p99 / max | Close stored, p50 / max (first fired → last row written) |
|---|---|---|---|---|---|---|---|---|---|---|
| peak half hour 1x | 2,477 | 3,089 | 9 | 0.0% / 0.0% | 0.0% | 2 | 4 | 3 / 99 / 99 | 406 / 1,179 / 1,182 | 629 / 1,187 |
| peak half hour 10x | 24,767 | 3,089 | 86 | 0.0% / 0.1% | 0.0% | 3 | 4 | 3 / 100 / 101 | 508 / 596 / 597 | 533 / 597 |
| session 100x | 205,065 | 3,084 | 1,021 | 0.0% / 0.2% | 0.0% | 6 | 9 | 2 / 100 / 101 | 577 / 1,089 / 1,216 | 831 / 1,232 |
| session 300x | 615,196 | 3,084 | 3,064 | 1.2% / 1.8% | 0.0% | 14 | 22 | 1 / 99 / 101 | 542 / 1,077 / 1,206 | 745 / 1,243 |
| session max | 1,152,857 | 3,084 | 5,742 | 7.1% / 7.5% | 0.0% | 28 | 43 | 2 / 95 / 101 | 406 / 889 / 1,533 | 567 / 1,545 |
