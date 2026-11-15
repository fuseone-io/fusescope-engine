#!/usr/bin/env python3
"""The D12 observation corpus: OTLP spans that each vary one input of a span
event's stored `isError`, and the fixture pairing each span with the events
the ClickHouse pipeline wrote for it.

  span-events-corpus.py request RUN START > request.json
  span-events-corpus.py fixture RUN rows.json versions.json [FIXTURE] > fixture.json

A fixture holds one entry per recorded run; passing an existing FIXTURE adds
the new run to it. Stored events are kept verbatim, so an absent `isError`
stays absent.
"""

import json
import sys
from pathlib import Path

KINDS = {"INTERNAL": 1, "SERVER": 2, "CLIENT": 3, "PRODUCER": 4, "CONSUMER": 5}
STATUS = {"UNSET": {}, "OK": {"code": 1}, "ERROR": {"code": 2, "message": "boom"}}
EXCEPTION = {"exception.type": "E", "exception.message": "m"}


def event(name, attributes=None):
    return {"name": name, "attributes": attributes or {}}


def cases():
    return [
        ("exception_error", "INTERNAL", "ERROR", [event("exception", EXCEPTION)]),
        ("exception_unset", "INTERNAL", "UNSET", [event("exception", EXCEPTION)]),
        ("exception_ok", "INTERNAL", "OK", [event("exception", EXCEPTION)]),
        ("exception_no_attrs_error", "INTERNAL", "ERROR", [event("exception")]),
        ("exception_no_attrs_unset", "INTERNAL", "UNSET", [event("exception")]),
        (
            "exception_escaped_unset",
            "INTERNAL",
            "UNSET",
            [event("exception", {**EXCEPTION, "exception.escaped": "true"})],
        ),
        ("Exception_case_error", "INTERNAL", "ERROR", [event("Exception", EXCEPTION)]),
        ("exceptions_error", "INTERNAL", "ERROR", [event("exceptions", EXCEPTION)]),
        (
            "other_with_exc_attrs_error",
            "INTERNAL",
            "ERROR",
            [event("failure", EXCEPTION)],
        ),
        (
            "other_with_exc_attrs_unset",
            "INTERNAL",
            "UNSET",
            [event("failure", EXCEPTION)],
        ),
        ("other_error", "INTERNAL", "ERROR", [event("log", {"level": "error"})]),
        (
            "other_then_exception_error",
            "INTERNAL",
            "ERROR",
            [event("log"), event("exception", EXCEPTION), event("log")],
        ),
        (
            "two_exceptions_error",
            "INTERNAL",
            "ERROR",
            [event("exception", EXCEPTION), event("exception", EXCEPTION)],
        ),
        ("exception_error_server", "SERVER", "ERROR", [event("exception", EXCEPTION)]),
        ("exception_error_client", "CLIENT", "ERROR", [event("exception", EXCEPTION)]),
        (
            "exception_error_producer",
            "PRODUCER",
            "ERROR",
            [event("exception", EXCEPTION)],
        ),
        (
            "exception_error_consumer",
            "CONSUMER",
            "ERROR",
            [event("exception", EXCEPTION)],
        ),
        ("exception_unset_server", "SERVER", "UNSET", [event("exception", EXCEPTION)]),
    ]


def ids(run, n):
    return f"d12e{run:08x}{0:012x}{n:08x}", f"d12e{n:012x}"


def string(v):
    return {"stringValue": v}


def request(run, start):
    spans = []
    for n, (name, kind, status, events) in enumerate(cases(), start=1):
        trace_id, span_id = ids(run, n)
        at = start + n * 1_000_000
        spans.append(
            {
                "traceId": trace_id,
                "spanId": span_id,
                "name": name,
                "kind": KINDS[kind],
                "startTimeUnixNano": str(at),
                "endTimeUnixNano": str(at + 500_000),
                "status": STATUS[status],
                "events": [
                    {
                        "timeUnixNano": str(at + 100_000 + i),
                        "name": e["name"],
                        "attributes": [
                            {"key": k, "value": string(v)}
                            for k, v in e["attributes"].items()
                        ],
                    }
                    for i, e in enumerate(events)
                ],
            }
        )
    resource = [{"key": "service.name", "value": string(f"d12-corpus-{run:08x}")}]
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": resource},
                "scopeSpans": [{"scope": {"name": "d12-corpus"}, "spans": spans}],
            }
        ]
    }


def fixture(run, rows_path, versions_path, existing=None):
    rows = {
        r["span_id"]: r
        for r in map(
            json.loads, filter(str.strip, Path(rows_path).read_text().splitlines())
        )
    }
    recorded = []
    for n, (name, kind, status, events) in enumerate(cases(), start=1):
        _, span_id = ids(run, n)
        row = rows.get(span_id)
        recorded.append(
            {
                "case": name,
                "sent": {"kind": kind, "status": status, "events": events},
                "row": None
                if row is None
                else {
                    "status_code": row["status_code"],
                    "has_error": row["has_error"],
                    "events": [json.loads(e) for e in row["events"]],
                },
            }
        )
    out = json.loads(Path(existing).read_text()) if existing else {"runs": []}
    out["runs"].append(
        {"source": json.loads(Path(versions_path).read_text()), "cases": recorded}
    )
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    if sys.argv[1] == "request":
        print(json.dumps(request(int(sys.argv[2], 16), int(sys.argv[3]))))
    else:
        fixture(int(sys.argv[2], 16), *sys.argv[3:6])
