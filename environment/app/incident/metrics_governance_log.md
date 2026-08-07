# Platform Observability Metrics Reconciler — Governance Review Log
Metrics governance archive for the failed observability-platform rollout (2026-Q1 through 2026-Q2).

## Executive Summary
How the metrics reconciler is *meant* to behave — the recovery of a truncated sample stream, canonicalization, deduplication, the window ordering and default frame, framed running aggregates, series ranking, the frame-relative value functions, the exposure metric over the series dependency topology, the per-series pressure ledger, queue admission, tiering and ordering — was settled incrementally by the metrics governance board, and those decisions live in the review entries below, not in any single summary. Several stages deliberately DEVIATE from vanilla SQL window semantics: the default frame, the rank gap rule, the ordering/NULL placement and the exposure metric are governance dialects, so importing a SQL engine and delegating to it produces wrong answers. The February draft proposals were revisited during the 2026-05 governance review and several were reversed; where a draft or interim conflicts with a later decision, the later dated decision governs. `/app/docs/report_spec.json` is the output contract only.

## Governance Review Archive
Routine entries are context only. #OBS-ticketed proposal and decision quotes are the authoritative record for reconciler behaviour.

### Review entry 1000 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1000. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1001 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1001. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1002 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1002. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
> **Recovery draft proposal (2026-02-06 - #OBS-6004)** Anders: the default window frame is SQL's RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so framed aggregates run over every preceding row in the partition *(Superseded — reversed in the 2026-05 governance review.)*
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1003 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1003. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1004 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1004. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1005 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1005. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1006 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1006. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
> **Recovery draft proposal (2026-02-07 - #OBS-6006)** Anders: series ranking uses standard SQL RANK: a tie of g rows shares a rank and the next distinct value's rank advances by the full g *(Superseded — reversed in the 2026-05 governance review.)*
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1007 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1007. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1008 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1008. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1009 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1009. Vendor ticket on collector retries closed; delivery within contractual budget.
> **Recovery draft proposal (2026-02-08 - #OBS-6008)** Rosa: order each partition by ts then value ASC with NULLS FIRST, matching SQLite's default ordering so null-coerced samples sort before real ones *(Superseded — reversed in the 2026-05 governance review.)*
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1010 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1010. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1011 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1011. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1012 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1012. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1013 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1013. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1014 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1014. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
> **Recovery draft proposal (2026-02-09 - #OBS-6010)** Rosa: the lag(value,1) carry returns NULL (serialized 0) when the offset falls outside the partition, exactly like SQL LAG with no default *(Superseded — reversed in the 2026-05 governance review.)*
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1015 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1015. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1016 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1016. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1017 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1017. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1018 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1018. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
> **Recovery draft proposal (2026-02-10 - #OBS-6012)** Anders: window_pressure = (bounded_running_sum // 200) + (frame_peak // 120), with no leader-count term *(Superseded — reversed in the 2026-05 governance review.)*
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1019 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1019. Vendor ticket on collector retries closed; delivery within contractual budget.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1020 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1020. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1021 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1021. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
> **Recovery draft proposal (2026-02-11 - #OBS-6018)** Anders: stability_index = ledger_adjusted_pressure + (frame_mean // 20) + (exposure_score // 7) *(Superseded — reversed in the 2026-05 governance review.)*
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1022 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1022. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1023 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1023. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1024 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1024. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1025 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1025. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
> **Recovery draft proposal (2026-02-12 - #OBS-6020)** Rosa: exposure_score sums, over reachable targets, the greatest edge-weight SUM path to each; edge weights are valid in 1..7 and traversal enumerates simple paths of at most two edges *(Superseded — reversed in the 2026-05 governance review.)*
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1026 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1026. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1027 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1027. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1028 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1028. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
> **Recovery draft proposal (2026-02-13 - #OBS-6040)** Rosa: admit any window whose ledger_adjusted_pressure is at least 3 regardless of metric *(Superseded — reversed in the 2026-05 governance review.)*
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1029 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1029. Vendor ticket on collector retries closed; delivery within contractual budget.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1030 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1030. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
> **Recovery draft proposal (2026-02-16 - #OBS-6002)** Anders: should a rollout ever truncate the sample stream, rebuild it by concatenating the pre-rollout snapshot and the replay journal in file order and let the sample_id deduplication stage settle whatever overlaps; journal bookkeeping fields are inert and may stay on the records *(Superseded — reversed in the 2026-06 governance review.)*
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1031 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1031. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1032 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1032. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
> **Recovery draft proposal (2026-02-14 - #OBS-6044)** Anders: tiers: escalate when stability_index>=12; review when stability_index>=6; else watch *(Superseded — reversed in the 2026-05 governance review.)*
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1033 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1033. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1034 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1034. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1035 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1035. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
> **Governance decision (2026-03-05 - #OBS-6109)** Rosa: deduplicate by sample_id keeping the FIRST-seen row in input order; ts and value do not override that *(Revised — see the 2026-05 governance review.)*
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1036 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1036. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1037 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1037. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1038 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1038. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1039 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1039. Vendor ticket on collector retries closed; delivery within contractual budget.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1040 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1040. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
> **Governance decision (2026-03-06 - #OBS-6115)** Priya: risk ledger interim: idle-gap decay and carry-in credit both FLOOR — carry_in = max(prev.carry_out - (idle_gap // 2), 0), ledger_adjusted_pressure = window_pressure + (carry_in // 4); carry_out = min(carry_in + window_pressure + rank_span*2, carry_out_cap) *(Revised — see the 2026-05 governance review.)*
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1041 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1041. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1042 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1042. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1043 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1043. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
> **Governance decision (2026-03-07 - #OBS-6124)** Priya: exposure_score is the SINGLE greatest retained bottleneck across reachable targets, not the sum of them *(Revised — see the 2026-05 governance review.)*
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1044 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1044. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1045 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1045. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1046 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1046. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1047 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1047. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
> **Governance decision (2026-03-08 - #OBS-6048)** Yusuf: the four max_* score summary fields are maxima over EVERY series-window, admitted or not *(Revised — see the 2026-05 governance review.)*
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1048 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1048. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1049 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1049. Vendor ticket on collector retries closed; delivery within contractual budget.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1050 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1050. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
> **Governance decision (2026-05-02 - #OBS-6101)** Yusuf: canonicalization: series and metric via str(...).strip().lower() (empty -> 'unknown'); note collapses internal whitespace; value coercion is int(str(value).strip()), else int(float(...)), else 0 — a value that coerces to 0 is the NULL sentinel for ordering; ts coerces the same way; the row is KEPT even when value or ts is invalid (supersedes any drop rule); suppressed — booleans unchanged, strings true/1/yes => true, other strings => false, non-string/non-bool via Python bool(value)
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1051 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1051. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
> **Governance decision (2026-03-09 - #OBS-6009)** Priya: stream recovery interim: the replay journal outranks the pre-rollout snapshot on overlap, but a replayed sample is appended to the END of the rebuilt stream rather than taking the snapshot record's position, and a retraction only applies to sample_ids the snapshot never held *(Revised — see the 2026-06 governance review.)*
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1052 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1052. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1053 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1053. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1054 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1054. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
> **Governance decision (2026-05-03 - #OBS-6102)** Yusuf: deduplicate by sample_id (final chain, revising #OBS-6109 which kept first-seen): keep the row with the HIGHEST ts; tie-break by value, then longer normalized note, then lexicographically larger normalized series, then first-seen input order. The direction of the value tie-break is set by #OBS-6142; every other step here is final
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1055 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1055. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1056 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1056. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1057 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1057. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
> **Governance decision (2026-05-14 - #OBS-6142)** Yusuf: duplicate value precedence is REVERSED. Watchdog re-emissions inflate the repeated sample's value before an operator confirms it, so keeping the higher value over-escalated. Where two rows share a sample_id and tie on ts, keep the row with the LOWER value. Only this comparison changes; the rest of the #OBS-6102 chain (then longer note, then larger series, then first-seen) runs unchanged after it
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1058 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1058. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1059 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1059. Vendor ticket on collector retries closed; delivery within contractual budget.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1060 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1060. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1061 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1061. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
> **Governance decision (2026-05-03 - #OBS-6108)** Lena: partition ordering (deviates from SQLite ORDER BY ts / NULLS FIRST): within each (series,metric) partition order by ts ASCENDING; within equal ts, NULL-coerced samples (value == 0) sort LAST; then by value DESCENDING; then sample_id ascending. Suppressed samples are EXCLUDED from window construction and from all framed aggregates, but are still counted in canonical_sample_count and suppressed_excluded_count. This supersedes #OBS-6008
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1062 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1062. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1063 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1063. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1064 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1064. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
> **Governance decision (2026-05-04 - #OBS-6104)** Lena: default window frame (deviates from SQL): the default frame is ROWS BETWEEN 2 PRECEDING AND CURRENT ROW — a bounded trailing frame of at most three rows (the current row and up to two before it in partition order), NOT SQL's RANGE UNBOUNDED PRECEDING AND CURRENT ROW. The window's emitted framed aggregates are taken at the LAST row of the partition over that bounded frame: bounded_running_sum = sum of the frame's values, frame_peak = max of the frame's values, frame_mean = bounded_running_sum // (frame row count) FLOORED. This supersedes #OBS-6004. ROUNDING: frame_mean = FLOOR
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1065 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1065. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1066 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1066. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1067 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1067. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1068 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1068. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1069 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1069. Vendor ticket on collector retries closed; delivery within contractual budget.
> **Governance decision (2026-05-05 - #OBS-6112)** Marek: window_pressure = (bounded_running_sum // 220) + (frame_peak // 130) + max(leader_count - 1, 0). Both divisions FLOOR. This supersedes #OBS-6012. ROUNDING: bounded_running_sum // 220 = FLOOR. ROUNDING: frame_peak // 130 = FLOOR
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1070 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1070. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1071 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1071. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1072 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1072. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
> **Governance decision (2026-05-04 - #OBS-6106)** Lena: series ranking (deviates from SQL RANK and DENSE_RANK): rank rows within the partition by value DESCENDING; equal values share one rank; the next distinct group's rank advances by min(group_size, 2) — capped at two, unlike RANK (advance by group_size) and DENSE_RANK (advance by one). leader_count = number of rows sharing rank 1; rank_span = the greatest rank assigned in the partition. This supersedes #OBS-6006
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1073 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1073. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1074 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1074. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1075 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1075. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1076 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1076. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
> **Governance decision (2026-05-05 - #OBS-6110)** Priya: frame-relative value functions at the LAST row of the partition: frame_first_value = the first_value over the active bounded frame of #OBS-6104, i.e. the value 2 rows before the last (clamped to the partition's first row when the partition is shorter). lag_fill_value = lag(value, 1) at the last row; when the offset falls outside the partition (a single-row partition) the governance default is the partition's FIRST value, NOT SQL NULL or 0. This supersedes #OBS-6010
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1077 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1077. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1078 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1078. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1079 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1079. Vendor ticket on collector retries closed; delivery within contractual budget.
> **Governance decision (2026-05-07 - #OBS-6120)** Lena: series topology edges: normalize source_series and target_series via the #OBS-6101 name normalization; coerce weight to int; discard self-edges and weights outside 1..9 (the 1..9 bound is final and revises the 1..7 draft in #OBS-6020); collapse duplicate directed (source,target) rows by MAXIMUM weight; edges are directed source -> target
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1080 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1080. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1081 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1081. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1082 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1082. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1083 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1083. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
> **Governance decision (2026-05-08 - #OBS-6122)** Priya: exposure metric (widest-path bounded reachability, deviates from SQL and from an edge-weight sum): for a window's series (the origin) enumerate every simple directed path of 1, 2 or 3 edges (bounded reachability; a simple path never repeats a node). A path's BOTTLENECK is the MINIMUM edge weight along it (maximin / widest-path), NOT the sum of its weights. For each reachable target retain the path with the GREATEST bottleneck, ties broken by the lexicographically smallest full node sequence. exposure_score = the SUM over reachable targets of each target's retained bottleneck (final, revising the single-greatest interim #OBS-6124 and the edge-sum draft #OBS-6020). exposure_reachable_series = the retained target names sorted ascending. exposure_strongest_path = among retained paths the one with the greatest bottleneck, then lexicographically smallest full node sequence; [origin] when nothing is reachable
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1084 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1084. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1085 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1085. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1086 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1086. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
> **Governance decision (2026-05-06 - #OBS-6116)** Yusuf: per-series pressure ledger: state is independent per series; process each series' windows in ascending (start_ts, metric) order. First window: idle_gap = 0, carry_in = 0. Later windows: idle_gap = max(current.start_ts - previous.end_ts, 0); carry_in = max(previous.carry_out - decay(idle_gap), 0); ledger_adjusted_pressure = window_pressure + credit(carry_in); carry_out = min(carry_in + window_pressure + backlog(window), carry_out_cap). carry_out_cap is the resolved policy value. The rounding of decay and credit is set by #OBS-6160 and the backlog term by #OBS-6162; finalize each window's carry_out before the next window in the same series. This supersedes #OBS-6115 on structure
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1087 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1087. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1088 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1088. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1089 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1089. Vendor ticket on collector retries closed; delivery within contractual budget.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1090 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1090. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
> **Governance decision (2026-05-28 - #OBS-6160)** Yusuf: ledger carry-chain rounding, final: the idle-gap decay and the carry-in credit BOTH ROUND UP (ceil), revising the floors in #OBS-6115: carry_in = max(previous.carry_out - ceil(idle_gap / 2), 0); ledger_adjusted_pressure = window_pressure + ceil(carry_in / 4). In integer arithmetic ceil(x/n) is -(-x // n). ROUNDING: idle_gap // 2 = CEIL. ROUNDING: carry_in // 4 = CEIL
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1091 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1091. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1092 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1092. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1093 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1093. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
> **Governance decision (2026-05-29 - #OBS-6162)** Yusuf: ledger carry-out backlog term, final: the backlog bonus is ceil(exposure_score / 9), revising the rank_span*2 term in #OBS-6115, so carry_out = min(carry_in + window_pressure + ceil(exposure_score / 9), carry_out_cap). A window on a high blast-radius series therefore carries more pressure forward. ROUNDING: exposure_score // 9 = CEIL
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1094 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1094. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1095 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1095. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1096 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1096. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1097 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1097. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
> **Governance decision (2026-05-08 - #OBS-6118)** Priya: stability_index = ledger_adjusted_pressure + rank_span + (exposure_score // 7), FLOORED on the exposure term. This supersedes the frame_mean draft #OBS-6018. ROUNDING: exposure_score // 7 = FLOOR
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1098 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1098. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1099 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1099. Vendor ticket on collector retries closed; delivery within contractual budget.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1100 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1100. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
> **Governance decision (2026-05-09 - #OBS-6140)** Marek: queue admission: the admitted metrics are exactly {latency_p99, error_rate, saturation}; queue_depth and any other metric are never admitted. A window is admitted iff its metric is admitted AND its ledger_adjusted_pressure is >= the resolved admission_min for that metric (inclusive: equal to the floor admits). This supersedes #OBS-6040
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1101 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1101. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1102 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1102. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1103 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1103. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1104 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1104. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
> **Governance decision (2026-05-16 - #OBS-6144)** Marek: tier assignment (thresholds are resolved policy values): a window is escalate iff peak_value >= escalate_peak_min OR ledger_adjusted_pressure >= escalate_ledger_min OR exposure_score >= escalate_exposure_min OR stability_index >= escalate_stability_min. Otherwise, evaluated only when escalate does not hold, review iff ledger_adjusted_pressure >= review_ledger_min OR leader_count >= 2 OR exposure_score >= review_exposure_min OR frame_peak >= review_frame_peak_min. Otherwise watch. This supersedes #OBS-6044
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1105 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1105. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1106 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1106. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1107 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1107. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
> **Governance decision (2026-05-10 - #OBS-6145)** Yusuf: final queue ordering, strictly in sequence: tier rank escalate > review > watch; then ledger_adjusted_pressure desc; then stability_index desc; then exposure_score desc; then window_pressure desc; then bounded_running_sum desc; then peak_value desc; then series asc; then metric asc; then start_ts asc
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1108 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1108. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1109 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1109. Vendor ticket on collector retries closed; delivery within contractual budget.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1110 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1110. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1111 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1111. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
> **Governance decision (2026-05-24 - #OBS-6146)** Marek: responder capacity cap: at most TWO queue rows per series. The cap is a FINAL pass over the fully ordered queue (not applied during admission and not per series before ordering): admit and prioritise every window, apply the #OBS-6145 ordering, then walk the ordered queue from the top keeping the first two rows of each series and discarding the rest. Which rows survive depends on the global order, so a window ranked third within its series is dropped even if it outranks a retained row from another series. Discarded rows do not contribute to any queue-derived summary field
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1112 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1112. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1113 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1113. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1114 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1114. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
> **Governance decision (2026-05-10 - #OBS-6148)** Yusuf: summary aggregation domains (final, revising #OBS-6048): max_window_pressure, max_ledger_adjusted_pressure, max_stability_index and max_exposure_score are maxima over the FINAL admitted review_queue rows only, using 0 when the queue is empty. Only max_carry_out is taken over EVERY series-window, admitted or not, using 0 when there are no windows
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1115 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1115. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1116 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1116. Replica checksum sync drill completed; sample acknowledgment stayed within the governance SLO.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1117 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1117. Change-board reviewed stale exception approvals; owners pinged before the next reconcile cycle.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1118 — ingest (north) lane
Shift lead logged a routine observation for ingest (north) during review window 1118. Rule-set rollback rehearsal ran clean; no changes to reconciler parameters were approved.
> **Governance decision (2026-05-18 - #OBS-6150)** Priya: metric policy baseline (read from /app/data/metric_policies.json at that fixed absolute path; --input never relocates it). Any field the policy file omits keeps its baseline: admission_min = 6; escalate_ledger_min = 20; escalate_exposure_min = 26; escalate_stability_min = 24; escalate_peak_min = 900; review_ledger_min = 10; review_exposure_min = 16; review_frame_peak_min = 650; carry_out_cap = 850
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1119 — gateway (south) lane
Shift lead logged a routine observation for gateway (south) during review window 1119. Vendor ticket on collector retries closed; delivery within contractual budget.
> **Governance decision (2026-06-02 - #OBS-6170)** Lena: authoritative sample-stream recovery, final — this supersedes the #OBS-6002 draft and revises the #OBS-6009 interim, and it runs BEFORE any reconcile. The rollout truncated `/app/data/events.json`, so that file is no longer authoritative and must be rebuilt in place from the two surviving sources beside it. Begin with every record of `/app/data/events_snapshot_pre_rollout.json` in snapshot file order. Then apply `/app/data/events_replay_journal.json` in ascending journal_seq order, one entry at a time. An entry whose journal_op is `append` carries a sample that arrived after the snapshot: if the stream already holds a record with that sample_id the entry OVERWRITES the FIRST such record IN PLACE, keeping that record's existing position (it is NOT moved to the end, revising #OBS-6009); otherwise the sample is appended to the end of the stream. An entry whose journal_op is `retract` removes EVERY record carrying that sample_id, whether the sample_id came from the snapshot or from an earlier journal entry (also revising #OBS-6009), and contributes no record of its own. The journal always wins on overlap; the snapshot never overrides it. journal_seq, journal_op and reason are journal bookkeeping, not sample fields: a recovered record carries exactly sample_id, series, metric, value, ts, suppressed and note, with the journal's values for a replayed sample. Write the result back to `/app/data/events.json` as a JSON array in exactly the order described. Nothing downstream re-orders it — the #OBS-6102/#OBS-6142 deduplication runs over this stream and its first-seen tie-break follows this order — so a stream rebuilt any other way yields a wrong review queue
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1120 — gateway (edge) lane
Shift lead logged a routine observation for gateway (edge) during review window 1120. Dashboard tiles for sample volume lagged during rule refresh; attributed to cache staleness, not the reconciler.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1121 — checkout (core) lane
Shift lead logged a routine observation for checkout (core) during review window 1121. Topology edge audit sampled cross-account roles; no reconciler-relevant findings for this lane.
> **Governance decision (2026-05-18 - #OBS-6152)** Priya: policy resolution, per metric, in three layers: start from the #OBS-6150 baseline; overlay every field the policy file's `default` object supplies (it need not be complete — an omitted field keeps its baseline); then overlay every field that metric's entry in `metric_overrides` supplies (an override names only the fields it changes and inherits the rest). Coerce every policy value to int. The per-series carry_out_cap of #OBS-6116 is taken from the resolved `default` layer
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

### Review entry 1122 — search (core) lane
Shift lead logged a routine observation for search (core) during review window 1122. Synthetic sample injection verified alert delivery to the on-call rotation for this region.
Historical CSV exports remain archived and non-authoritative for the JSON reconciler acceptance.

### Review entry 1123 — billing (ledger) lane
Shift lead logged a routine observation for billing (ledger) during review window 1123. Noise review: repeated samples traced to a flapping collector, suppressed at the source.
No reconciler semantics changed in this entry; parameters remain as approved by the governance board.

### Review entry 1124 — cache (edge) lane
Shift lead logged a routine observation for cache (edge) during review window 1124. Quarterly access recertification touched this lane; no reconciler-relevant configuration changed.
Reviewers should reconcile behaviour questions against #OBS governance decisions rather than chat excerpts.

### Review entry 1125 — search (west) lane
Shift lead logged a routine observation for search (west) during review window 1125. Capacity review noted rising sample volume; thresholds unchanged outside the governance process.
Thread archived; see the #OBS decision entries for anything affecting reconciler behaviour.

