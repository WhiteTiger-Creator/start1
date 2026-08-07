#!/usr/bin/env python3
"""Rebuild the authoritative telemetry sample stream truncated by the rollout.

Implements the metrics governance board's final recovery decision (#OBS-6170 in
/app/incident/metrics_governance_log.md), which supersedes the #OBS-6002 draft
and revises the #OBS-6009 interim: start from the pre-rollout snapshot in file
order, then replay the journal in ascending journal_seq order; an ``append``
entry overwrites the first record already carrying that sample_id in place (and
is otherwise appended to the end), a ``retract`` entry removes every record with
that sample_id, journal bookkeeping fields never reach the recovered stream, and
the result is written back to /app/data/events.json.
"""

from __future__ import annotations

import json
from pathlib import Path

STREAM_PATH = Path("/app/data/events.json")
SNAPSHOT_PATH = Path("/app/data/events_snapshot_pre_rollout.json")
JOURNAL_PATH = Path("/app/data/events_replay_journal.json")

SAMPLE_FIELDS = ("sample_id", "series", "metric", "value", "ts", "suppressed", "note")


def recover(snapshot: list[dict], journal: list[dict]) -> list[dict]:
    stream = [dict(record) for record in snapshot]
    for entry in sorted(journal, key=lambda e: e["journal_seq"]):
        sample_id = entry["sample_id"]
        if entry["journal_op"] == "retract":
            stream = [r for r in stream if r["sample_id"] != sample_id]
            continue
        record = {field: entry[field] for field in SAMPLE_FIELDS}
        for index, existing in enumerate(stream):
            if existing["sample_id"] == sample_id:
                stream[index] = record
                break
        else:
            stream.append(record)
    return stream


def main() -> None:
    snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    journal = json.loads(JOURNAL_PATH.read_text(encoding="utf-8"))
    recovered = recover(snapshot, journal)
    STREAM_PATH.write_text(json.dumps(recovered, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
