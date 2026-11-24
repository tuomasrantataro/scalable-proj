# Notes for the report

Unstructured notes for the report, from different benchmarks, tests and implementation attempts. Add anything worth
mentioning here, in any order and form; say where it comes from (script, result directory, commit or section of
[`MEASUREMENTS.md`](MEASUREMENTS.md)) and the date, so it can be checked later.

## To be stated in the report

From `GAPS-11-23.md`, "Not gaps, but to be stated in the report" (2026-11-23).

- **The reference has the same author and interpretations as the job.** It finds coding errors, not shared
  misreadings. Justify the two decisions that change the results: empty windows are skipped, and zero prices are excluded
  (11,349 advisories instead of 25,834; both variants are computed by the reference).
- **Initialization:** 4,970 of the 11,349 advisories (44%) are the first BUY of a symbol, caused by EMA = 0. They are
  flagged `initial`.
- **Only advisories are delivered per trader.** EMA values are available for every symbol in TimescaleDB and Grafana
  (relaxation 3 of the project description asks for updates for the subscribed symbols).
- **Limits:** one host, one broker, one database. A new job starts with EMA = 0 (no savepoints scripted). No GPU is used.
