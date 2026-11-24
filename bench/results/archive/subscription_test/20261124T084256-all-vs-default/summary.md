# Subscriptions: default file against every symbol (full week, 4x1)

Latencies in ms, p50 / p95 / max, without the first close.

| Run | Subscriptions | Digest ok | Delivered / missing / unexpected | Time (s) | Fired → received, delivered | Fired → stored, shared advisories | Fired → emitted, all windows | CPU % taskmanager / kafka / timescaledb |
|---|---|---|---|---|---|---|---|---|
| backlog | default | yes | 73 / 0 / 0 | 125.7 | 727 / 1,350 / 1,372 (n 72) | 723 / 1,353 / 1,370 (n 60) | 572 / 1,166 / 2,268 | 509 / 105 / 283 |
| backlog | all | yes | 11,349 / 0 / 0 | 126.3 | 570 / 1,840 / 2,255 (n 11,337) | 584 / 1,808 / 1,838 (n 60) | 521 / 1,203 / 2,521 | 532 / 107 / 284 |
| backlog | all | yes | 11,349 / 0 / 0 | 130.2 | 584 / 2,054 / 2,431 (n 11,337) | 525 / 1,812 / 2,093 (n 60) | 515 / 1,206 / 2,509 | 521 / 107 / 277 |
| backlog | default | yes | 73 / 0 / 0 | 128.3 | 609 / 1,128 / 1,419 (n 72) | 553 / 1,127 / 1,417 (n 60) | 450 / 1,150 / 2,538 | 511 / 103 / 269 |
| backlog | default | yes | 73 / 0 / 0 | 128.1 | 600 / 1,351 / 1,505 (n 72) | 593 / 1,371 / 1,504 (n 60) | 558 / 1,169 / 2,364 | 536 / 104 / 270 |
| backlog | all | yes | 11,349 / 0 / 0 | 130.3 | 511 / 1,212 / 2,220 (n 11,337) | 549 / 1,225 / 1,263 (n 60) | 495 / 1,149 / 2,721 | 545 / 110 / 271 |
| live | default | yes | 73 / 0 / 0 | 257.0 | 32 / 854 / 3,658 (n 72) | 31 / 965 / 3,657 (n 60) | 3 / 564 / 4,055 | 278 / 92 / 164 |
| live | all | yes | 11,349 / 0 / 0 | 254.1 | 38 / 875 / 1,152 (n 11,337) | 57 / 1,026 / 1,145 (n 60) | 3 / 488 / 1,545 | 286 / 96 / 171 |
| live | all | yes | 11,349 / 0 / 0 | 255.9 | 37 / 1,196 / 1,455 (n 11,337) | 53 / 1,144 / 1,204 (n 60) | 3 / 460 / 2,017 | 282 / 95 / 172 |
| live | default | yes | 73 / 0 / 0 | 258.2 | 33 / 1,360 / 1,646 (n 72) | 56 / 1,357 / 1,643 (n 60) | 3 / 566 / 2,056 | 280 / 94 / 173 |
