#!/usr/bin/env python3
"""Platform-observability metrics reconciler (governance dialect).

Converts raw telemetry samples into a ranked operational review report.
Every window-function rule here is the metrics governance board's own dialect
and deliberately deviates from vanilla SQL / SQLite window semantics; it is
reconstructed from /app/incident/metrics_governance_log.md, the operational
data, and /app/docs/report_spec.json (output contract only).

Standard-library only. Delegating window evaluation to sqlite3/pandas/duckdb
would produce wrong answers because the dialect deviates from SQL, and is
rejected by the verifier regardless.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

# Fixed absolute operational-input paths. --input selects the sample stream
# only; the scoped policy and topology files never become relative to it.
DEFAULT_INPUT = "/app/data/events.json"
DEFAULT_OUTPUT_DIR = "/app/output"
TOPOLOGY_PATH = "/app/data/series_topology_edges.json"
METRIC_POLICY_PATH = "/app/data/metric_policies.json"

SCHEMA_VERSION = "obs-window-v1"
TIER_ORDER = ["escalate", "review", "watch"]

# --- Governance constants (final decisions; see log entries in comments) ---
FRAME_LOOKBACK = 2          # #OBS-6104: default frame ROWS 2 PRECEDING..CURRENT
RANK_GAP_CAP = 2            # #OBS-6106: tie advances next rank by min(group,2)
REACH_MAX_EDGES = 3         # #OBS-6120: bounded reachability, <=3 edges
PRESS_RUNSUM_DIV = 220      # #OBS-6112: bounded_running_sum // 220 (floor)
PRESS_PEAK_DIV = 130        # #OBS-6112: frame_peak // 130 (floor)
LEDGER_IDLE_DIV = 2         # #OBS-6116/#OBS-6160: idle decay, CEIL (final)
LEDGER_CARRYIN_DIV = 4      # #OBS-6116/#OBS-6160: carry-in credit, CEIL (final)
LEDGER_EXPOSURE_DIV = 9     # #OBS-6162: carry-out exposure term, CEIL (final)
STAB_EXPOSURE_DIV = 7       # #OBS-6118: exposure_score // 7 (floor)

# Baseline metric policy (#OBS-6150). Any field the policy file omits keeps
# these values; the policy file may override per default and per metric.
POLICY_BASELINE = {
    "admission_min": 6,
    "escalate_ledger_min": 20,
    "escalate_exposure_min": 26,
    "escalate_stability_min": 24,
    "escalate_peak_min": 900,
    "review_ledger_min": 10,
    "review_exposure_min": 16,
    "review_frame_peak_min": 650,
    "carry_out_cap": 850,
}
ADMITTED_METRICS = ("latency_p99", "error_rate", "saturation")  # #OBS-6140
SERIES_CAP = 2  # #OBS-6146: at most 2 queue rows per series, after ordering


def _ceil_div(numer: int, denom: int) -> int:
    """Integer ceil for non-negative numer; ceil(x/n) == -(-x // n)."""
    return -(-numer // denom)


def canon_name(value: object) -> str:
    text = str(value).strip().lower()
    return text if text else "unknown"


def collapse_ws(value: object) -> str:
    return " ".join(str(value).split())


def coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return 0


def coerce_flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


# --------------------------------------------------------------------------
# Stage 1-2: canonicalize + deduplicate by sample_id (#OBS-6101, #OBS-6102/#OBS-6142)
# --------------------------------------------------------------------------
def canonicalize(raw_rows: list[dict]) -> list[dict]:
    canon = []
    for row in raw_rows:
        canon.append(
            {
                "sample_id": collapse_ws(row.get("sample_id", "")),
                "series": canon_name(row.get("series", "")),
                "metric": canon_name(row.get("metric", "")),
                "ts": coerce_int(row.get("ts", 0)),
                "value": coerce_int(row.get("value", 0)),
                "suppressed": coerce_flag(row.get("suppressed", False)),
                "note": collapse_ws(row.get("note", "")),
            }
        )
    return canon


def deduplicate(canon_rows: list[dict]) -> list[dict]:
    # #OBS-6102 chain, with the #OBS-6142 reversal on the value tie-break:
    # keep highest ts; on tie keep the LOWER value; then longer note; then
    # lexicographically larger series; then first-seen input order.
    best: dict[str, tuple] = {}
    order: dict[str, int] = {}
    for idx, row in enumerate(canon_rows):
        sid = row["sample_id"]
        # sort key: larger is better. ts high, value low (negated), note long,
        # series large, earliest input index (negated so smaller index wins).
        key = (row["ts"], -row["value"], len(row["note"]), row["series"], -idx)
        if sid not in best or key > best[sid]:
            best[sid] = key
            order[sid] = idx
    keep_idx = set(order.values())
    return [row for idx, row in enumerate(canon_rows) if idx in keep_idx]


# --------------------------------------------------------------------------
# Stage 3-6: partition, order, framed aggregates, ranking, frame-relative values
# --------------------------------------------------------------------------
def order_partition(rows: list[dict]) -> list[dict]:
    # #OBS-6108 governance ordering (deviates from SQLite ORDER BY ts / NULLS
    # FIRST): ts asc; then null-coerced values (value == 0) sort LAST within
    # equal ts; then value DESC; then sample_id asc.
    return sorted(
        rows,
        key=lambda r: (r["ts"], 1 if r["value"] == 0 else 0, -r["value"], r["sample_id"]),
    )


def governance_rank(values_desc: list[int]) -> tuple[int, int]:
    # #OBS-6106 dense-gap rank over value DESC. Ties share a rank; the next
    # distinct group's rank advances by min(group_size, RANK_GAP_CAP=2).
    # Differs from SQL RANK (advance by group_size) and DENSE_RANK (advance 1).
    ordered = sorted(values_desc, reverse=True)
    rank = 1
    leader_count = 0
    max_rank = 1
    i = 0
    n = len(ordered)
    while i < n:
        j = i
        while j < n and ordered[j] == ordered[i]:
            j += 1
        group = j - i
        if rank == 1:
            leader_count = group
        max_rank = max(max_rank, rank)
        rank += min(group, RANK_GAP_CAP)
        i = j
    return leader_count, max_rank


def build_window(series: str, metric: str, canon_partition: list[dict]) -> dict:
    active = order_partition([r for r in canon_partition if not r["suppressed"]])
    values = [r["value"] for r in active]
    n = len(values)
    start_ts = active[0]["ts"]
    end_ts = active[-1]["ts"]
    last = n - 1
    lo = max(0, last - FRAME_LOOKBACK)   # #OBS-6104 default frame
    frame_vals = values[lo : last + 1]
    bounded_running_sum = sum(frame_vals)
    frame_peak = max(frame_vals)
    frame_mean = bounded_running_sum // len(frame_vals)
    frame_first_value = values[lo]                       # first_value over frame
    lag_fill_value = values[last - 1] if n >= 2 else values[0]  # #OBS-6110 lag default
    peak_value = max(values)
    leader_count, rank_span = governance_rank(values)
    window_pressure = (
        bounded_running_sum // PRESS_RUNSUM_DIV
        + frame_peak // PRESS_PEAK_DIV
        + max(leader_count - 1, 0)
    )
    return {
        "series": series,
        "metric": metric,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "span_ms": max(end_ts - start_ts, 0),
        "sample_count": n,
        "canonical_sample_count": len(canon_partition),
        "peak_value": peak_value,
        "bounded_running_sum": bounded_running_sum,
        "frame_peak": frame_peak,
        "frame_mean": frame_mean,
        "frame_first_value": frame_first_value,
        "lag_fill_value": lag_fill_value,
        "leader_count": leader_count,
        "rank_span": rank_span,
        "window_pressure": window_pressure,
        "source_sample_ids": sorted(r["sample_id"] for r in active),
    }


# --------------------------------------------------------------------------
# Stage 8: exposure over the series dependency graph (#OBS-6120..#OBS-6124)
# Governance widest-path (maximin bottleneck) bounded reachability. NOT a
# standard shortest/longest path or edge-weight sum.
# --------------------------------------------------------------------------
def build_topology(edges: list[dict]) -> dict[str, dict[str, int]]:
    graph: dict[str, dict[str, int]] = {}
    for edge in edges:
        src = canon_name(edge.get("source_env", edge.get("source_series", "")))
        dst = canon_name(edge.get("target_env", edge.get("target_series", "")))
        weight = coerce_int(edge.get("weight", 0))
        if src == dst or not 0 < weight <= 9:   # #OBS-6120 canonicalization
            continue
        graph.setdefault(src, {})
        graph[src][dst] = max(graph[src].get(dst, 0), weight)
    return graph


def exposure(origin: str, graph: dict[str, dict[str, int]]) -> tuple[list[str], int, list[str]]:
    # Enumerate simple directed paths of 1..REACH_MAX_EDGES edges. For each
    # reachable target retain the path with the greatest BOTTLENECK (min edge
    # weight along the path) -- the widest path; ties broken by lexicographically
    # smallest node sequence. exposure_score = sum of retained bottlenecks.
    best: dict[str, tuple[int, list[str]]] = {}

    def visit(node: str, bottleneck: int, path: list[str]) -> None:
        if len(path) - 1 >= REACH_MAX_EDGES:
            return
        for nxt, weight in graph.get(node, {}).items():
            if nxt in path:
                continue
            new_bottleneck = weight if bottleneck == 0 else min(bottleneck, weight)
            new_path = path + [nxt]
            cur = best.get(nxt)
            cand = (new_bottleneck, new_path)
            if cur is None or new_bottleneck > cur[0] or (
                new_bottleneck == cur[0] and new_path < cur[1]
            ):
                best[nxt] = cand
            visit(nxt, new_bottleneck, new_path)

    visit(origin, 0, [origin])
    reachable = sorted(best)
    score = sum(best[t][0] for t in reachable)
    if reachable:
        # strongest path: greatest bottleneck, then lexicographically smallest
        # full node sequence.
        strongest_path = min(
            (best[t] for t in reachable),
            key=lambda bp: (-bp[0], bp[1]),
        )[1]
    else:
        strongest_path = [origin]
    return reachable, score, strongest_path


# --------------------------------------------------------------------------
# Stage 7: per-series stateful pressure ledger (#OBS-6116, #OBS-6160..#OBS-6162)
# --------------------------------------------------------------------------
def apply_ledger(windows: list[dict], cap: int) -> None:
    # windows are one series' windows sorted by start_ts.
    prev_end = None
    prev_carry_out = 0
    for win in windows:
        if prev_end is None:
            idle_gap = 0
            carry_in = 0
        else:
            idle_gap = max(win["start_ts"] - prev_end, 0)
            carry_in = max(prev_carry_out - _ceil_div(idle_gap, LEDGER_IDLE_DIV), 0)
        ledger_adjusted = win["window_pressure"] + _ceil_div(carry_in, LEDGER_CARRYIN_DIV)
        carry_out = min(
            carry_in + win["window_pressure"] + _ceil_div(win["exposure_score"], LEDGER_EXPOSURE_DIV),
            cap,
        )
        win["idle_gap"] = idle_gap
        win["carry_in"] = carry_in
        win["carry_out"] = carry_out
        win["ledger_adjusted_pressure"] = ledger_adjusted
        prev_end = win["end_ts"]
        prev_carry_out = carry_out


# --------------------------------------------------------------------------
# Policy resolution (#OBS-6150, #OBS-6152)
# --------------------------------------------------------------------------
def resolve_policy(metric: str, policy_data: dict) -> dict:
    resolved = dict(POLICY_BASELINE)
    for field, val in policy_data.get("default", {}).items():
        if field in resolved:
            resolved[field] = coerce_int(val)
    override = policy_data.get("metric_overrides", {}).get(metric)
    if isinstance(override, dict):
        for field, val in override.items():
            if field in resolved:
                resolved[field] = coerce_int(val)
    return resolved


# --------------------------------------------------------------------------
# Scoring, admission, tiering (#OBS-6118, #OBS-6140, #OBS-6144)
# --------------------------------------------------------------------------
def score_window(win: dict) -> None:
    win["stability_index"] = (
        win["ledger_adjusted_pressure"]
        + win["rank_span"]
        + win["exposure_score"] // STAB_EXPOSURE_DIV
    )


def assign_tier(win: dict, policy: dict) -> str:
    if (
        win["peak_value"] >= policy["escalate_peak_min"]
        or win["ledger_adjusted_pressure"] >= policy["escalate_ledger_min"]
        or win["exposure_score"] >= policy["escalate_exposure_min"]
        or win["stability_index"] >= policy["escalate_stability_min"]
    ):
        return "escalate"
    if (
        win["ledger_adjusted_pressure"] >= policy["review_ledger_min"]
        or win["leader_count"] >= 2
        or win["exposure_score"] >= policy["review_exposure_min"]
        or win["frame_peak"] >= policy["review_frame_peak_min"]
    ):
        return "review"
    return "watch"


WINDOW_FIELDS = (
    "start_ts",
    "end_ts",
    "span_ms",
    "sample_count",
    "canonical_sample_count",
    "metric",
    "peak_value",
    "bounded_running_sum",
    "frame_peak",
    "frame_mean",
    "frame_first_value",
    "lag_fill_value",
    "leader_count",
    "rank_span",
    "window_pressure",
    "idle_gap",
    "carry_in",
    "carry_out",
    "ledger_adjusted_pressure",
    "stability_index",
    "exposure_reachable_series",
    "exposure_score",
    "exposure_strongest_path",
    "source_sample_ids",
)
QUEUE_FIELDS = (
    "window_id",
    "series",
    *WINDOW_FIELDS,
    "tier",
)


def run(input_path: str, output_dir: str) -> None:
    raw_rows = json.loads(Path(input_path).read_text(encoding="utf-8"))
    topo_edges = json.loads(Path(TOPOLOGY_PATH).read_text(encoding="utf-8"))
    policy_data = json.loads(Path(METRIC_POLICY_PATH).read_text(encoding="utf-8"))

    canon_rows = canonicalize(raw_rows)
    canon_rows = deduplicate(canon_rows)

    suppressed_excluded = sum(1 for r in canon_rows if r["suppressed"])

    graph = build_topology(topo_edges)

    # partition by (series, metric); build windows from non-suppressed rows.
    partitions: dict[tuple[str, str], list[dict]] = {}
    for row in canon_rows:
        partitions.setdefault((row["series"], row["metric"]), []).append(row)

    windows: list[dict] = []
    for (series, metric), part in partitions.items():
        if not any(not r["suppressed"] for r in part):
            continue
        win = build_window(series, metric, part)
        reach, score, path = exposure(series, graph)
        win["exposure_reachable_series"] = reach
        win["exposure_score"] = score
        win["exposure_strongest_path"] = path
        windows.append(win)

    # per-series ledger across each series' windows sorted by start_ts.
    by_series: dict[str, list[dict]] = {}
    for win in windows:
        by_series.setdefault(win["series"], []).append(win)
    cap = resolve_policy("__default__", policy_data)["carry_out_cap"]
    for series_windows_list in by_series.values():
        series_windows_list.sort(key=lambda w: (w["start_ts"], w["metric"]))
        apply_ledger(series_windows_list, cap)

    for win in windows:
        score_window(win)

    # admission + tiering
    queue_rows: list[dict] = []
    for win in windows:
        policy = resolve_policy(win["metric"], policy_data)
        if win["metric"] not in ADMITTED_METRICS:
            continue
        if win["ledger_adjusted_pressure"] < policy["admission_min"]:
            continue
        win["tier"] = assign_tier(win, policy)
        win["window_id"] = f"{win['series']}:{win['metric']}:{win['start_ts']}-{win['end_ts']}"
        queue_rows.append(win)

    tier_rank = {name: len(TIER_ORDER) - i for i, name in enumerate(TIER_ORDER)}
    queue_rows.sort(
        key=lambda w: (
            -tier_rank[w["tier"]],
            -w["ledger_adjusted_pressure"],
            -w["stability_index"],
            -w["exposure_score"],
            -w["window_pressure"],
            -w["bounded_running_sum"],
            -w["peak_value"],
            w["series"],
            w["metric"],
            w["start_ts"],
        )
    )
    # capacity cap: keep first SERIES_CAP rows per series over the global order.
    seen: dict[str, int] = {}
    capped: list[dict] = []
    for win in queue_rows:
        c = seen.get(win["series"], 0)
        if c < SERIES_CAP:
            capped.append(win)
            seen[win["series"]] = c + 1
    queue_rows = capped

    # --- summary aggregates ---
    tier_counts = {tier: 0 for tier in TIER_ORDER}
    for win in queue_rows:
        tier_counts[win["tier"]] += 1

    def qmax(field: str) -> int:
        return max((w[field] for w in queue_rows), default=0)

    summary = {
        "schema_version": SCHEMA_VERSION,
        "raw_sample_count": len(raw_rows),
        "unique_sample_ids": len({collapse_ws(r.get("sample_id", "")) for r in raw_rows}),
        "canonical_sample_count": len(canon_rows),
        "series_count": len({w["series"] for w in windows}),
        "partition_count": len(windows),
        "suppressed_excluded_count": suppressed_excluded,
        "tier_counts": tier_counts,
        "total_span_ms": sum(w["span_ms"] for w in windows),
        "total_bounded_running_sum": sum(w["bounded_running_sum"] for w in windows),
        "total_window_pressure": sum(w["window_pressure"] for w in windows),
        "total_ledger_adjusted_pressure": sum(w["ledger_adjusted_pressure"] for w in windows),
        "longest_window_ms": max((w["span_ms"] for w in windows), default=0),
        "queued_window_count": len(queue_rows),
        "max_window_pressure": qmax("window_pressure"),
        "max_ledger_adjusted_pressure": qmax("ledger_adjusted_pressure"),
        "max_stability_index": qmax("stability_index"),
        "max_exposure_score": qmax("exposure_score"),
        "max_carry_out": max((w["carry_out"] for w in windows), default=0),
    }

    # --- series_windows.json: object keyed by series, list sorted by start_ts ---
    series_windows: dict[str, list[dict]] = {}
    for win in windows:
        series_windows.setdefault(win["series"], []).append(win)
    out_windows: dict[str, list[dict]] = {}
    for series in sorted(series_windows):
        rows = sorted(series_windows[series], key=lambda w: (w["start_ts"], w["metric"]))
        out_windows[series] = [{f: w[f] for f in WINDOW_FIELDS} for w in rows]

    out_queue = [{f: w[f] for f in QUEUE_FIELDS} for w in queue_rows]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (out / "series_windows.json").write_text(json.dumps(out_windows, indent=2) + "\n", encoding="utf-8")
    with (out / "review_queue.jsonl").open("w", encoding="utf-8") as fh:
        for row in out_queue:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Platform-observability metrics reconciler")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    run(args.input, args.output_dir)


if __name__ == "__main__":
    main()
