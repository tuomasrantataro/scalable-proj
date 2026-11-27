# Window delay at real-time pace

ms after the window's end in market time, without the first close.

| OUT_OF_ORDERNESS_S | End → fired, per close | Advisories | End → received p50 / p95 / max | Late events | Clock drift / spread (ms) |
|---|---|---|---|---|---|
| 60 | 60,378 / 60,689 | 501 | 60,392 / 60,743 / 60,792 | 0 | 0.0 / 1.0 |
| 5 | 5,689 / 5,570 | 501 | 5,704 / 5,795 / 5,798 | 0 | 0.0 / 1.0 |
| 1 | 1,522 / 1,639 | 501 | 1,537 / 1,743 / 1,744 | 0 | 1.0 / 1.0 |


(*) not valid: the replay sends a second's events together at its start, so a fractional bound looks up to 1 s too fast.
