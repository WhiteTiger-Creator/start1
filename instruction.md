Act as the platform-observability engineer picking up after a failed metrics-platform rollout. The rollout truncated the authoritative telemetry sample stream at `/app/data/events.json` and left the control-plane reconciler at `/app/workflow/export_report.py` evaluating stale draft rules instead of the governance board's final decisions.

Nothing the reconciler produces can be trusted until that stream is rebuilt. A pre-rollout snapshot and a replay journal of the samples that arrived after it survive alongside the truncated file under `/app/data`. How the two merge, which wins where they overlap, and the order of the result are governance decisions, and the stream must be restored at its expected path first.

Then restore the reconciler. Preserve its `--input` and `--output-dir` options and their defaults; the series topology and the metric policy are always read from their fixed absolute paths under `/app/data`, and `--input` selects the sample stream only.

`/app/docs/report_spec.json` is the output contract: paths, schemas, required-field lists, coercions, container shapes and sort orders. It says nothing about how any value is derived. Reconstruct that from `/app/incident/metrics_governance_log.md`, mostly routine noise recording rules drafted, revised and reversed over several months; where entries conflict, the later dated decision governs.

The shipped dependency topology is large enough that the exposure search has to be engineered rather than enumerated: a run must finish inside the 120-second wall-clock budget the contract states, and the verifier enforces it.

A run writes `/app/output/summary.json`, `/app/output/series_windows.json` and `/app/output/review_queue.jsonl`. Derive every value from the operational inputs: no database or dataframe engine, correct against an alternate sample stream, identical across reruns, and leave the frozen incident snapshot in `/app/workflow` untouched.
