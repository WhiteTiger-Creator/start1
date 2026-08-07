"""Verifier tests for the platform-observability metrics reconciler task."""

from __future__ import annotations

import ast
import hashlib
import itertools
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

WORKFLOW_PATH = Path("/app/workflow/export_report.py")
ORIGINAL_WORKFLOW_PATH = Path("/app/workflow/.export_report.original")
DEFAULT_INPUT = Path("/app/data/events.json")
SNAPSHOT_PATH = Path("/app/data/events_snapshot_pre_rollout.json")
JOURNAL_PATH = Path("/app/data/events_replay_journal.json")
TOPOLOGY_PATH = Path("/app/data/series_topology_edges.json")
POLICY_PATH = Path("/app/data/metric_policies.json")
SPEC_PATH = Path("/app/docs/report_spec.json")
LOG_PATH = Path("/app/incident/metrics_governance_log.md")
EXPECTED_FIXTURE = Path("/tests/fixtures/expected_report.json")
ALT_INPUT = Path("/tests/fixtures/alt_events.json")

TIER_ORDER = ["escalate", "review", "watch"]
TIER_RANK = {name: len(TIER_ORDER) - idx for idx, name in enumerate(TIER_ORDER)}

FIXTURE = json.loads(EXPECTED_FIXTURE.read_text())
SPEC = json.loads(SPEC_PATH.read_text())

POLICY_FIELDS = (
    "admission_min", "escalate_ledger_min", "escalate_exposure_min", "escalate_stability_min",
    "escalate_peak_min", "review_ledger_min", "review_exposure_min", "review_frame_peak_min",
    "carry_out_cap",
)
BASELINE = {
    "admission_min": 6, "escalate_ledger_min": 20, "escalate_exposure_min": 26,
    "escalate_stability_min": 24, "escalate_peak_min": 900, "review_ledger_min": 10,
    "review_exposure_min": 16, "review_frame_peak_min": 650, "carry_out_cap": 850,
}
ADMITTED_METRICS = {"latency_p99", "error_rate", "saturation"}
SAMPLE_FIELDS = ("sample_id", "series", "metric", "value", "ts", "suppressed", "note")

WINDOW_KEYS = set(SPEC["series_windows_json"]["required_fields"])
QUEUE_KEYS = set(SPEC["review_queue"]["required_fields"])
SUMMARY_KEYS = set(SPEC["summary_json"]["required_fields"])


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


# --- verifier execution isolation -------------------------------------------------
# The submitted /app/workflow/export_report.py is untrusted once the separate verifier runs it.
# We execute it under an unprivileged UID (65534 / nobody) via setpriv, so it cannot write the
# reward path, read the held-out fixtures under /tests, or interfere with the verifier. Inputs are
# staged into a candidate-writable work area; policy files under /app keep their fixed paths.
_CWORK = Path("/candidate-work")
_run_ctr = itertools.count()
_SETPRIV = ["setpriv", "--reuid=65534", "--regid=65534", "--clear-groups", "--no-new-privs"]

# The submitted program gets a minimal explicit environment rather than inheriting the verifier's
# (PATH/PYTHONPATH/CI variables and any other grader context).
_CANDIDATE_ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": "/candidate-work", "LANG": "C.UTF-8"}
_CANDIDATE_TIMEOUT = 300


def _candidate_dir() -> Path:
    d = _CWORK / f"run-{next(_run_ctr)}"
    d.mkdir(parents=True, exist_ok=True)
    os.chmod(d, 0o777)
    return d


def _run_agent(argv, cwd: Path):
    """Run the submitted program under the unprivileged candidate UID with a scrubbed environment."""
    return subprocess.run(
        _SETPRIV + argv, check=True, capture_output=True, text=True, cwd=str(cwd),
        env=dict(_CANDIDATE_ENV), timeout=_CANDIDATE_TIMEOUT,
    )


def _run_pipeline(tmp_path: Path, script_path: Path = WORKFLOW_PATH, input_path: Path = DEFAULT_INPUT):
    work = _candidate_dir()
    out_dir = work / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(out_dir, 0o777)
    staged_input = work / "input.json"
    shutil.copy(str(input_path), str(staged_input))
    os.chmod(staged_input, 0o644)
    result = _run_agent(
        [sys.executable, str(script_path), "--input", str(staged_input), "--output-dir", str(out_dir)],
        cwd=work,
    )
    assert result.returncode == 0
    summary = _load_json(out_dir / "summary.json")
    windows = _load_json(out_dir / "series_windows.json")
    queue = _load_jsonl(out_dir / "review_queue.jsonl")
    return out_dir, summary, windows, queue


@pytest.fixture(scope="session")
def primary_outputs(tmp_path_factory):
    return _run_pipeline(tmp_path_factory.mktemp("primary"))


# --------------------------------------------------------------------------
# Step 1: the truncated sample stream must be recovered in place
# --------------------------------------------------------------------------
def _naive_concatenation() -> list[dict]:
    """The superseded draft merge: snapshot then journal, bookkeeping fields left on."""
    stream = [dict(r) for r in _load_json(SNAPSHOT_PATH)]
    stream.extend(dict(e) for e in _load_json(JOURNAL_PATH))
    return stream


def test_recovery_sources_are_intact():
    assert _load_json(SNAPSHOT_PATH) == FIXTURE["snapshot"]
    assert _load_json(JOURNAL_PATH) == FIXTURE["journal"]


def test_sample_stream_recovered():
    """/app/data/events.json shipped truncated; it must hold the recovered stream."""
    recovered = _load_json(DEFAULT_INPUT)
    assert isinstance(recovered, list)
    assert recovered == FIXTURE["recovered_stream"]


def test_recovered_records_carry_no_journal_bookkeeping():
    for record in _load_json(DEFAULT_INPUT):
        assert set(record) == set(SAMPLE_FIELDS)


def test_shipped_and_naive_streams_differ_from_the_recovered_one():
    """The recovery is real work: neither the truncated file nor the draft merge match."""
    expected = FIXTURE["recovered_stream"]
    assert FIXTURE["shipped_truncated_stream"] != expected
    assert _load_json(SNAPSHOT_PATH) != expected
    assert _naive_concatenation() != expected


def test_reconciler_output_depends_on_the_recovered_stream(tmp_path: Path):
    """Even a correctly repaired reconciler emits wrong artifacts on a wrongly merged stream."""
    for label, stream in (
        ("truncated", FIXTURE["shipped_truncated_stream"]),
        ("snapshot_only", _load_json(SNAPSHOT_PATH)),
        ("naive_concatenation", _naive_concatenation()),
    ):
        bad_input = tmp_path / f"{label}.json"
        _write_json(bad_input, stream)
        _, summary, windows, queue = _run_pipeline(tmp_path / label, input_path=bad_input)
        assert summary != FIXTURE["primary"]["summary"], label
        assert (windows, queue) != (FIXTURE["primary"]["windows"], FIXTURE["primary"]["queue_rows"]), label


# --------------------------------------------------------------------------
# Step 2: the reconciler output contract
# --------------------------------------------------------------------------
def test_cli_exists():
    assert WORKFLOW_PATH.exists()


def test_output_dir_contains_exactly_three_files(primary_outputs):
    out_dir, _, _, _ = primary_outputs
    names = sorted(p.name for p in out_dir.iterdir() if p.is_file())
    assert names == ["review_queue.jsonl", "series_windows.json", "summary.json"]


def test_primary_summary_matches_fixture(primary_outputs):
    _, summary, _, _ = primary_outputs
    assert summary == FIXTURE["primary"]["summary"]


def test_primary_windows_matches_fixture(primary_outputs):
    _, _, windows, _ = primary_outputs
    assert windows == FIXTURE["primary"]["windows"]


def test_primary_queue_matches_fixture(primary_outputs):
    _, _, _, queue = primary_outputs
    assert queue == FIXTURE["primary"]["queue_rows"]


def test_summary_schema(primary_outputs):
    _, summary, _, _ = primary_outputs
    assert set(summary) == SUMMARY_KEYS
    assert summary["schema_version"] == "obs-window-v1"
    assert list(summary["tier_counts"]) == TIER_ORDER


def test_windows_schema_and_sorting(primary_outputs):
    _, _, windows, _ = primary_outputs
    assert list(windows) == sorted(windows)
    for series_windows in windows.values():
        starts = [row["start_ts"] for row in series_windows]
        assert starts == sorted(starts)
        for row in series_windows:
            assert set(row) == WINDOW_KEYS
            assert row["span_ms"] == max(row["end_ts"] - row["start_ts"], 0)
            assert row["source_sample_ids"] == sorted(row["source_sample_ids"])
            assert row["exposure_reachable_series"] == sorted(row["exposure_reachable_series"])
            assert row["metric"] in {"latency_p99", "error_rate", "saturation", "queue_depth"}


def test_queue_required_fields(primary_outputs):
    _, _, _, queue = primary_outputs
    for row in queue:
        assert set(row) == QUEUE_KEYS
        assert row["tier"] in TIER_RANK
        assert row["window_id"] == f"{row['series']}:{row['metric']}:{row['start_ts']}-{row['end_ts']}"


def test_queue_sorted(primary_outputs):
    _, _, _, queue = primary_outputs
    assert queue == sorted(
        queue,
        key=lambda row: (
            -TIER_RANK[row["tier"]],
            -row["ledger_adjusted_pressure"],
            -row["stability_index"],
            -row["exposure_score"],
            -row["window_pressure"],
            -row["bounded_running_sum"],
            -row["peak_value"],
            row["series"],
            row["metric"],
            row["start_ts"],
        ),
    )


def test_response_queue_jsonl_compact(primary_outputs):
    out_dir, _, _, _ = primary_outputs
    for line in (out_dir / "review_queue.jsonl").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        assert ": " not in line
        assert json.dumps(json.loads(line), separators=(",", ":")) == line


def test_summary_math_consistency(primary_outputs):
    _, summary, windows, queue = primary_outputs
    span = brs = wp = lap = longest = 0
    for series_windows in windows.values():
        for row in series_windows:
            span += row["span_ms"]
            brs += row["bounded_running_sum"]
            wp += row["window_pressure"]
            lap += row["ledger_adjusted_pressure"]
            longest = max(longest, row["span_ms"])
    assert summary["total_span_ms"] == span
    assert summary["total_bounded_running_sum"] == brs
    assert summary["total_window_pressure"] == wp
    assert summary["total_ledger_adjusted_pressure"] == lap
    assert summary["longest_window_ms"] == longest
    assert summary["queued_window_count"] == len(queue)
    assert summary["max_carry_out"] == max(
        (r["carry_out"] for e in windows.values() for r in e), default=0
    )
    for field in ("window_pressure", "ledger_adjusted_pressure", "stability_index", "exposure_score"):
        assert summary["max_" + field] == max((r[field] for r in queue), default=0)


def test_summary_sample_counts_track_the_recovered_stream(primary_outputs):
    _, summary, _, _ = primary_outputs
    stream = _load_json(DEFAULT_INPUT)
    assert summary["raw_sample_count"] == len(stream)
    assert summary["unique_sample_ids"] == len({r["sample_id"] for r in stream})


def test_tier_counts_enumerate_all_three(primary_outputs):
    _, summary, _, queue = primary_outputs
    counts = {t: 0 for t in TIER_ORDER}
    for row in queue:
        counts[row["tier"]] += 1
    assert summary["tier_counts"] == counts
    assert set(summary["tier_counts"]) == set(TIER_ORDER)


# --------------------------------------------------------------------------
# Original / broken snapshot
# --------------------------------------------------------------------------
def test_original_snapshot_preserved():
    assert ORIGINAL_WORKFLOW_PATH.exists()
    digest = hashlib.sha256(ORIGINAL_WORKFLOW_PATH.read_bytes()).hexdigest()
    assert digest == FIXTURE["broken_pipeline_sha256"]


def test_broken_snapshot_is_wrong(tmp_path: Path):
    _, broken_summary, _, broken_queue = _run_pipeline(tmp_path, script_path=ORIGINAL_WORKFLOW_PATH)
    broken_hash = hashlib.sha256(json.dumps(broken_summary, sort_keys=True).encode("utf-8")).hexdigest()
    assert broken_hash == FIXTURE["broken_summary_sha256"]
    assert len(broken_queue) == FIXTURE["broken_queue_count"]
    assert [r.get("window_id") for r in broken_queue] == FIXTURE["broken_queue_window_ids"]
    assert broken_queue != FIXTURE["primary"]["queue_rows"]


# --------------------------------------------------------------------------
# Generalization / idempotency / CLI
# --------------------------------------------------------------------------
def test_pipeline_rerun_idempotent(tmp_path: Path):
    _, sa, wa, qa = _run_pipeline(tmp_path / "a")
    _, sb, wb, qb = _run_pipeline(tmp_path / "b")
    assert (sa, wa, qa) == (sb, wb, qb)


def test_pipeline_supports_alternate_input(tmp_path: Path):
    _, summary, windows, queue = _run_pipeline(tmp_path, input_path=ALT_INPUT)
    assert summary == FIXTURE["alternate"]["summary"]
    assert windows == FIXTURE["alternate"]["windows"]
    assert queue == FIXTURE["alternate"]["queue_rows"]


def test_cli_defaults_work_and_match_explicit_run(tmp_path: Path):
    _, explicit_summary, _, _ = _run_pipeline(tmp_path)
    # The no-argument run writes to the default /app/output; clear any root-owned artifacts from
    # solve.sh and make the dir candidate-writable so the unprivileged program can populate it.
    default_out = Path("/app/output")
    shutil.rmtree(default_out, ignore_errors=True)
    default_out.mkdir(parents=True, exist_ok=True)
    os.chmod(default_out, 0o777)
    _run_agent([sys.executable, str(WORKFLOW_PATH)], cwd=_candidate_dir())
    assert _load_json(default_out / "summary.json") == explicit_summary


def test_submitted_program_runs_unprivileged_and_cannot_write_reward(tmp_path: Path):
    """The isolation itself works: code run the way the verifier runs the agent is unprivileged
    (uid 65534) and cannot write the reward path."""
    # Ensure the reward path exists and is root-owned (as it is under test.sh) before probing.
    os.makedirs("/logs/verifier", exist_ok=True)
    reward = Path("/logs/verifier/reward.txt")
    if not reward.exists():
        reward.write_text("0")
    os.chmod("/logs/verifier", 0o755)
    os.chmod(reward, 0o644)
    probe = _candidate_dir() / "probe.py"
    probe.write_text(
        "import os\n"
        "print(os.getuid())\n"
        "open('/logs/verifier/reward.txt', 'w').write('1')\n",
        encoding="utf-8",
    )
    os.chmod(probe, 0o644)
    res = subprocess.run(
        _SETPRIV + [sys.executable, str(probe)],
        capture_output=True, text=True, cwd=str(_CWORK), check=False,
    )
    assert res.stdout.strip().splitlines()[0] == "65534", "submitted program must run as uid 65534"
    assert res.returncode != 0 and "Permission denied" in res.stderr, (
        "unprivileged submitted program must not be able to write the reward path"
    )


# --------------------------------------------------------------------------
# Source-path influence
# --------------------------------------------------------------------------
def test_topology_source_path_affects_output(tmp_path: Path):
    original = TOPOLOGY_PATH.read_text(encoding="utf-8")
    try:
        _, summary_a, windows_a, queue_a = _run_pipeline(tmp_path / "a")
        TOPOLOGY_PATH.write_text("[]\n", encoding="utf-8")
        _, summary_b, windows_b, queue_b = _run_pipeline(tmp_path / "b")
        assert summary_a["max_exposure_score"] > 0
        assert summary_b["max_exposure_score"] == 0
        assert summary_a != summary_b
        assert windows_a != windows_b
        assert queue_a != queue_b
    finally:
        TOPOLOGY_PATH.write_text(original, encoding="utf-8")


def test_policy_source_path_affects_output(tmp_path: Path):
    original = POLICY_PATH.read_text()
    try:
        data = json.loads(original)
        data["default"]["admission_min"] = 999
        POLICY_PATH.write_text(json.dumps(data, indent=2) + "\n")
        _, summary, _, queue = _run_pipeline(tmp_path / "shifted")
        assert summary != FIXTURE["primary"]["summary"]
        assert len(queue) < len(FIXTURE["primary"]["queue_rows"])
    finally:
        POLICY_PATH.write_text(original)


# --------------------------------------------------------------------------
# Policy resolution
# --------------------------------------------------------------------------
def _resolve(metric: str, data: dict) -> dict:
    base = dict(BASELINE)
    base.update({k: int(v) for k, v in data.get("default", {}).items() if k in BASELINE})
    override = data.get("metric_overrides", {}).get(metric)
    if isinstance(override, dict):
        base.update({k: int(v) for k, v in override.items() if k in BASELINE})
    return base


def test_sparse_override_inherits_remaining_fields():
    data = json.loads(POLICY_PATH.read_text())
    overrides = data.get("metric_overrides", {})
    sparse = [m for m, o in overrides.items() if len(o) == 1]
    assert sparse, "the shipped policy must exercise a single-field override"
    default_resolved = _resolve("__absent__", data)
    for metric in sparse:
        resolved = _resolve(metric, data)
        named = next(iter(overrides[metric]))
        assert resolved[named] == int(overrides[metric][named])
        for field in POLICY_FIELDS:
            if field != named:
                assert resolved[field] == default_resolved[field]


def test_policy_default_may_omit_fields_and_falls_back_to_baseline():
    data = json.loads(POLICY_PATH.read_text())
    omitted = [f for f in POLICY_FIELDS if f not in data.get("default", {})]
    assert omitted, "the shipped policy must omit at least one field to exercise fallback"
    resolved = _resolve("__absent__", data)
    for field in omitted:
        assert resolved[field] == BASELINE[field]


def test_priority_rules_follow_resolved_policy(primary_outputs):
    _, _, _, queue = primary_outputs
    data = json.loads(POLICY_PATH.read_text())
    for row in queue:
        p = _resolve(row["metric"], data)
        if (
            row["peak_value"] >= p["escalate_peak_min"]
            or row["ledger_adjusted_pressure"] >= p["escalate_ledger_min"]
            or row["exposure_score"] >= p["escalate_exposure_min"]
            or row["stability_index"] >= p["escalate_stability_min"]
        ):
            assert row["tier"] == "escalate"
        elif (
            row["ledger_adjusted_pressure"] >= p["review_ledger_min"]
            or row["leader_count"] >= 2
            or row["exposure_score"] >= p["review_exposure_min"]
            or row["frame_peak"] >= p["review_frame_peak_min"]
        ):
            assert row["tier"] == "review"
        else:
            assert row["tier"] == "watch"


# --------------------------------------------------------------------------
# Capacity cap
# --------------------------------------------------------------------------
def test_series_capacity_cap_applied_after_ordering(primary_outputs):
    _, _, windows, queue = primary_outputs
    per_series: dict[str, int] = {}
    for row in queue:
        per_series[row["series"]] = per_series.get(row["series"], 0) + 1
    assert per_series
    assert max(per_series.values()) <= 2, f"series exceeded cap: {per_series}"
    admissible = sum(
        1
        for blocks in windows.values()
        for b in blocks
        if b["metric"] in ADMITTED_METRICS
    )
    assert admissible > len(queue), "fixture must contain more admissible windows than the cap allows"
    seen_order = [r["series"] for r in queue]
    for series in per_series:
        idxs = [i for i, e in enumerate(seen_order) if e == series]
        assert idxs == sorted(idxs)


# --------------------------------------------------------------------------
# Exposure widest-path deviation
# --------------------------------------------------------------------------
def test_exposure_is_widest_path_bounded_reachability(tmp_path: Path):
    original = TOPOLOGY_PATH.read_text(encoding="utf-8")
    try:
        edges = [
            {"source_series": "lab", "target_series": "a", "weight": 6},
            {"source_series": "a", "target_series": "t", "weight": 4},
            {"source_series": "lab", "target_series": "t", "weight": 3},
            {"source_series": "t", "target_series": "deep", "weight": 5},
            {"source_series": "deep", "target_series": "far", "weight": 9},
            {"source_series": "far", "target_series": "toofar", "weight": 9},
        ]
        _write_json(TOPOLOGY_PATH, edges)
        rows = [{
            "sample_id": "x1", "series": "lab", "metric": "latency_p99",
            "value": 800, "ts": 100, "suppressed": False, "note": "trace",
        }]
        input_path = tmp_path / "trust.json"
        _write_json(input_path, rows)
        _, _, windows, _ = _run_pipeline(tmp_path / "run", input_path=input_path)
        w = windows["lab"][0]
        # widest (maximin) bottleneck per reachable target within 3 edges:
        #   a:    lab>a = 6
        #   t:    max(lab>t = 3, lab>a>t = min(6,4)=4) = 4
        #   deep: max(lab>t>deep = min(3,5)=3, lab>a>t>deep = min(6,4,5)=4) = 4
        #   far:  lab>t>deep>far = min(3,5,9)=3  (4 edges lab>a>t>deep>far is out)
        assert w["exposure_reachable_series"] == ["a", "deep", "far", "t"]
        assert w["exposure_score"] == 6 + 4 + 3 + 4  # widest bottleneck sum = 17
        assert w["exposure_strongest_path"] == ["lab", "a"]
        # a standard edge-weight SUM packing would score 6 + 10 + 15 + 17 = 48;
        # the governance widest-path (maximin) sum of 17 deviates.
        assert w["exposure_score"] != 48
    finally:
        TOPOLOGY_PATH.write_text(original, encoding="utf-8")


# --------------------------------------------------------------------------
# Anti-delegation: static AST ban + proof the SQL dialect deviates
# --------------------------------------------------------------------------
def test_reconciler_does_not_import_sql_engines():
    tree = ast.parse(WORKFLOW_PATH.read_text(encoding="utf-8"))
    banned = set(SPEC["workflow_repair"]["prohibited_imports"])
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split(".")[0])
    offending = banned & found
    assert not offending, f"reconciler must not delegate to a SQL/dataframe engine: {offending}"


def test_ast_check_catches_sqlite_importing_engine(tmp_path: Path):
    """The AST ban is real: an sqlite3-importing engine is detected."""
    shim = tmp_path / "delegating_engine.py"
    shim.write_text("import sqlite3\n\n\ndef run(a, b):\n    return sqlite3.connect(':memory:')\n")
    tree = ast.parse(shim.read_text())
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert "sqlite3" in imported


def test_standard_sql_semantics_produce_wrong_answers(tmp_path: Path):
    """A sqlite3 delegate using STANDARD SQL window semantics disagrees with the
    governance dialect on both the default frame and the rank gap rule."""
    rows = [
        {"sample_id": f"s{i}", "series": "lab", "metric": "latency_p99",
         "value": v, "ts": t, "suppressed": False, "note": "n"}
        for i, (v, t) in enumerate([(90, 100), (90, 140), (90, 180), (80, 220), (70, 260)])
    ]
    input_path = tmp_path / "dev.json"
    _write_json(input_path, rows)
    _, _, windows, _ = _run_pipeline(tmp_path / "run", input_path=input_path)
    w = windows["lab"][0]
    values = [90, 90, 90, 80, 70]  # partition values, ts-ascending

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE s(ts INTEGER, value INTEGER)")
    con.executemany("INSERT INTO s VALUES (?, ?)", list(enumerate(values)))
    # SQL default frame: RANGE UNBOUNDED PRECEDING .. CURRENT ROW -> running total
    sql_running_sum_last = con.execute(
        "SELECT SUM(value) OVER (ORDER BY ts) FROM s ORDER BY ts DESC LIMIT 1"
    ).fetchone()[0]
    # SQL standard RANK over value DESC -> max rank
    sql_rank_max = con.execute(
        "SELECT MAX(r) FROM (SELECT RANK() OVER (ORDER BY value DESC) r FROM s)"
    ).fetchone()[0]
    con.close()

    # Governance default frame is ROWS 2 PRECEDING (bounded) -> last-3 sum = 240,
    # never the SQL running total of 420.
    assert sql_running_sum_last == sum(values)  # 420
    assert w["bounded_running_sum"] == 240
    assert w["bounded_running_sum"] != sql_running_sum_last
    # Governance rank gap caps advance at 2, so rank_span = 4; SQL RANK gives 5.
    assert sql_rank_max == 5
    assert w["rank_span"] == 4
    assert w["rank_span"] != sql_rank_max


# --------------------------------------------------------------------------
# Sources stay operational
# --------------------------------------------------------------------------
def test_governance_log_present():
    assert LOG_PATH.exists() and LOG_PATH.stat().st_size > 0


def test_pipeline_does_not_reference_test_artifacts():
    code = WORKFLOW_PATH.read_text(encoding="utf-8")
    for token in ("/tests", "expected_report.json", "alt_events.json"):
        assert token not in code
