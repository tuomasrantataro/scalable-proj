# Database load at paced traffic

ms, without the first close. Writer busy and EMA backpressured as a share of time.

| Point | Events/s | Rows per close (p50) | Rows/s (average over the run) | Writer busy avg / max subtask | EMA backpressured | DB CPU % avg | DB CPU % max | fired → EMA p50 / p99 / max | fired → written p50 / p99 / max | Close stored, p50 / max (first fired → last row written) |
|---|---|---|---|---|---|---|---|---|---|---|
| peak half hour 1x | 2,477 | 3,089 | 9 | 0.0% / 0.0% | 0.0% | 2 | 5 | 3 / 99 / 99 | 319 / 1,180 / 1,180 | 987 / 1,184 |
| peak half hour 10x | 24,767 | 3,089 | 86 | 0.0% / 0.1% | 0.0% | 2 | 4 | 3 / 97 / 98 | 565 / 1,045 / 1,066 | 729 / 1,069 |
| session 100x | 205,065 | 3,084 | 1,021 | 0.1% / 0.2% | 0.0% | 6 | 10 | 2 / 99 / 100 | 492 / 1,091 / 1,222 | 731 / 1,232 |
| session 300x | 615,196 | 3,084 | 3,064 | 1.0% / 1.3% | 0.0% | 14 | 22 | 1 / 96 / 101 | 501 / 1,067 / 1,141 | 691 / 1,154 |
| session max | 1,139,666 | 3,084 | 5,676 | 7.6% / 8.9% | 0.0% | 29 | 41 | 2 / 98 / 102 | 402 / 844 / 1,190 | 615 / 1,208 |
