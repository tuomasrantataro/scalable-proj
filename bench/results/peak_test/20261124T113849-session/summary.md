# Peak test

| Scope | Busiest minute | Rows in it | Real peak (events/s) |
|---|---|---|---|
| all | 10 Nov 15:30 | 275,558 | 4,593 |

Latencies in ms, without the first window close. Lag in seconds of input at the achieved rate.

| Scope | Speed | Events/s (achieved) | Peak minute / second (nominal/s) | Verdict | Lag max / end (s; samples) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka / producer | Window fired → EMA p50 / p99 / max | Advisory fired → received p50 / p99 / max | Advisories (initial) | Window p95 per third of the run |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all | 100× | 205,065 | 459,258 / 2,150,300 | keeps up | 0.0 / 0.0 (61) | Source:_kafka_source 26/0 | 64 / 17 / 53 / 113 | 2 / 99 / 101 | 5 / 100 / 102 | 868 (868) | 77 → 68 → 98 |
| all | 300× | 615,196 | 1,377,775 / 6,450,900 | keeps up | 0.0 / 0.0 (20) | Source:_kafka_source 98/0 | 169 / 53 / 75 / 315 | 2 / 95 / 103 | 13 / 122 / 124 | 868 (868) | 89 → 68 → 68 |
| all | max | 1,086,204 | – / – | keeps up | 0.0 / 0.0 (11) | timescaledb_ema:_Writer 373/0 | 330 / 106 / 90 / 521 | 3 / 97 / 104 | 18 / 117 / 139 | 885 (885) | 87 → 68 → 85 |
