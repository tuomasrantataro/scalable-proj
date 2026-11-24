# Window delay at real-time pace

ms after the window's end in market time, without the first close.

| OUT_OF_ORDERNESS_S | End → fired, per close | Advisories | End → received p50 / p95 / max | Late events | Clock drift / spread (ms) |
|---|---|---|---|---|---|
| 60 | 60,688 / 60,580 | 501 | 60,706 / 60,791 / 60,792 | 0 | 0.0 / 0.0 |
| 5 | 5,545 / 5,315 | 501 | 5,559 / 5,600 / 5,602 | 0 | 1.0 / 1.0 |
| 1 | 1,633 / 1,472 | 501 | 1,649 / 1,737 / 1,741 | 0 | 1.0 / 1.0 |
| 0.5 (*) | 594 / 357 | 501 | 614 / 699 / 701 | 0 | 0.0 / 2.0 |


(*) not valid: the replay sends a second's events together at its start, so a fractional bound looks up to 1 s too fast.
