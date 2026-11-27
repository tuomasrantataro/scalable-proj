# Database load at paced traffic

ms, without the first close. Writer busy and EMA backpressured as a share of time.

| Point | Events/s | Rows per close (p50) | Rows/s (average over the run) | Writer busy avg / max subtask | EMA backpressured | DB CPU % avg | DB CPU % max | fired → EMA p50 / p99 / max | fired → written p50 / p99 / max | Close stored, p50 / max (first fired → last row written) |
|---|---|---|---|---|---|---|---|---|---|---|
| real symbols | 20,761 | 3,136 | 89 | 0.0% / 0.0% | 0.0% | 2 | 4 | 3 / 91 / 91 | 633 / 729 / 730 | 684 / 730 |
| 5,000 symbols | 20,761 | 5,000 | 139 | 0.2% / 0.3% | 0.0% | 3 | 7 | 2 / 71 / 95 | 13 / 534 / 536 | 465 / 542 |
| 10,000 symbols | 20,761 | 10,000 | 278 | 0.5% / 0.7% | 0.0% | 4 | 11 | 3 / 91 / 95 | 53 / 913 / 914 | 682 / 921 |
| 20,000 symbols | 20,761 | 20,000 | 555 | 1.1% / 1.3% | 0.7% | 4 | 14 | 5 / 355 / 557 | 97 / 1,157 / 1,726 | 1,157 / 1,758 |
| 40,000 symbols | 20,761 | 40,000 | 1,110 | 1.9% / 2.0% | 1.5% | 6 | 39 | 112 / 598 / 647 | 218 / 967 / 1,725 | 964 / 1,780 |
| 80,000 symbols | 20,761 | 79,757 | 2,214 | 3.1% / 3.3% | 2.6% | 10 | 47 | 227 / 715 / 752 | 320 / 910 / 1,418 | 1,694 / 1,819 |
