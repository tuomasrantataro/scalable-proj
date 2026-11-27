# Peak test

| Scope | Busiest minute | Rows in it | Real peak (events/s) |
|---|---|---|---|
| all | 10 Nov 15:30 | 275,558 | 4,593 |

Latencies in ms, without the first window close. Lag in seconds of input at the achieved rate.

| Scope | Speed | Events/s (achieved) | Peak minute / second (nominal/s) | Verdict | Lag max / end (s; samples) | Busiest task busy/bp (ms/s) | CPU % TaskManagers / TimescaleDB / Kafka / producer | Window fired → EMA p50 / p99 / max | Advisory fired → received p50 / p99 / max | Advisories (initial) | Window p95 per third of the run |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all | 100× | 205,065 | 459,258 / 2,150,300 | keeps up | 0.0 / 0.0 (61) | Source:_kafka_source 25/0 | 61 / 5 / 51 / 113 | 2 / 93 / 98 | 6 / 92 / 98 | 868 (868) | 82 → 76 → 88 |
| all | 300× | 615,196 | 1,377,775 / 6,450,900 | keeps up | 0.0 / 0.0 (20) | Source:_kafka_source 97/0 | 171 / 13 / 65 / 313 | 2 / 98 / 103 | 13 / 96 / 112 | 868 (868) | 87 → 86 → 91 |
| all | max | 1,126,773 | – / – | keeps up | 0.0 / 0.0 (11) | Source:_kafka_source 234/0 | 294 / 27 / 87 / 636 | 3 / 96 / 102 | 13 / 110 / 117 | 868 (868) | 82 → 88 → 82 |
