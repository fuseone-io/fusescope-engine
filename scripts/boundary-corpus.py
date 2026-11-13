#!/usr/bin/env python3
"""The D7 observation corpus: small traces whose spans vary what could make one a top-level
operation of its service (its parent's service, the parent's presence, its kind, the OTLP
is_remote flags, whether parent and child arrive in one export or two), and the fixture
pairing them with the top_level_operations rows the ClickHouse pipeline wrote.

  boundary-corpus.py requests RUN_HEX START_UNIX_NANO > requests.json
  boundary-corpus.py fixture requests.json rows.json versions.json > fixture.json
"""

import json
import sys

KINDS = {"UNSPECIFIED": 0, "INTERNAL": 1, "SERVER": 2, "CLIENT": 3, "PRODUCER": 4, "CONSUMER": 5}
# SpanFlags: bit 0x100 says the parent's remoteness is known, 0x200 that it is remote.
LOCAL, REMOTE, UNKNOWN = 0x100, 0x300, 0


def span(trace, n, name, kind="INTERNAL", parent=None, flags=UNKNOWN):
    out = {
        "traceId": trace,
        "spanId": f"d7{n:014x}",
        "name": name,
        "kind": KINDS[kind],
        "flags": flags,
        "status": {},
    }
    if parent is not None:
        out["parentSpanId"] = f"d7{parent:014x}"
    return out


def cases():
    """Each case: name, then its exports in order, each a list of (service, span arguments)."""
    out = [
        ("root_only", [[("a", dict(n=1, name="root"))]]),
        ("local_child", [[("a", dict(n=1, name="root")), ("a", dict(n=2, name="child", parent=1))]]),
        ("missing_parent_same_service_unknown", [[("a", dict(n=2, name="orphan", parent=1))]]),
        ("missing_parent_remote", [[("a", dict(n=2, name="orphan", parent=1, flags=REMOTE))]]),
        ("missing_parent_local", [[("a", dict(n=2, name="orphan", parent=1, flags=LOCAL))]]),
        ("local_child_remote_flag", [[("a", dict(n=1, name="root")), ("a", dict(n=2, name="child", parent=1, flags=REMOTE))]]),
        ("cross_child_local_flag", [[("a", dict(n=1, name="root")), ("b", dict(n=2, name="child", parent=1, flags=LOCAL))]]),
        ("cross_child_remote_flag", [[("a", dict(n=1, name="root")), ("b", dict(n=2, name="child", parent=1, flags=REMOTE))]]),
        ("cross_grandchild", [[
            ("a", dict(n=1, name="root")),
            ("b", dict(n=2, name="child", parent=1)),
            ("b", dict(n=3, name="grandchild", parent=2)),
        ]]),
        ("cross_child_parent_first_export", [[("a", dict(n=1, name="root"))], [("b", dict(n=2, name="child", parent=1))]]),
        ("cross_child_child_first_export", [[("b", dict(n=2, name="child", parent=1))], [("a", dict(n=1, name="root"))]]),
        ("cross_child_no_service", [[("a", dict(n=1, name="root")), (None, dict(n=2, name="child", parent=1))]]),
        ("parent_no_service", [[(None, dict(n=1, name="root")), ("b", dict(n=2, name="child", parent=1))]]),
        ("back_to_first_service", [[
            ("a", dict(n=1, name="root")),
            ("b", dict(n=2, name="middle", parent=1)),
            ("a", dict(n=3, name="back", parent=2)),
        ]]),
    ]
    out += [
        ("root_local_flag", [[("a", dict(n=1, name="root", flags=LOCAL))]]),
        ("root_remote_flag", [[("a", dict(n=1, name="root", flags=REMOTE))]]),
        ("local_child_local_flag", [[("a", dict(n=1, name="root")), ("a", dict(n=2, name="child", parent=1, flags=LOCAL))]]),
        ("local_child_local_flag_parent_first_export", [[("a", dict(n=1, name="root"))], [("a", dict(n=2, name="child", parent=1, flags=LOCAL))]]),
        ("local_child_remote_flag_parent_first_export", [[("a", dict(n=1, name="root"))], [("a", dict(n=2, name="child", parent=1, flags=REMOTE))]]),
        ("cross_child_local_flag_parent_first_export", [[("a", dict(n=1, name="root"))], [("b", dict(n=2, name="child", parent=1, flags=LOCAL))]]),
        ("cross_child_local_flag_child_first_export", [[("b", dict(n=2, name="child", parent=1, flags=LOCAL))], [("a", dict(n=1, name="root"))]]),
        ("local_child_unknown_parent_first_export", [[("a", dict(n=1, name="root"))], [("a", dict(n=2, name="child", parent=1))]]),
        ("missing_parent_remote_with_root", [[("a", dict(n=1, name="root")), ("a", dict(n=3, name="orphan", parent=2, flags=REMOTE))]]),
        ("missing_parent_local_with_root", [[("a", dict(n=1, name="root")), ("a", dict(n=3, name="orphan", parent=2, flags=LOCAL))]]),
        ("missing_parent_remote_other_service", [[("b", dict(n=2, name="orphan", parent=1, flags=REMOTE))]]),
        ("local_grandchild_local_flags", [[
            ("a", dict(n=1, name="root")),
            ("a", dict(n=2, name="child", parent=1, flags=LOCAL)),
            ("a", dict(n=3, name="grandchild", parent=2, flags=LOCAL)),
        ]]),
    ]
    for kind in KINDS:
        out.append((f"cross_child_{kind.lower()}", [[("a", dict(n=1, name="root")), ("b", dict(n=2, name="child", kind=kind, parent=1))]]))
    return out


def requests(run, start):
    exports = []
    for c, (case, steps) in enumerate(cases(), start=1):
        trace = f"d7c0{run:08x}{0:012x}{c:08x}"
        for step, spans in enumerate(steps):
            by_service = {}
            for service, args in spans:
                s = span(trace, **args)
                s["startTimeUnixNano"] = str(start + args["n"] * 1_000_000)
                s["endTimeUnixNano"] = str(start + args["n"] * 1_000_000 + 500_000)
                by_service.setdefault(service, []).append(s)
            resource_spans = []
            for service, group in by_service.items():
                attrs = [] if service is None else [{"key": "service.name", "value": {"stringValue": f"d7c-{run:08x}-{c:02d}{service}"}}]
                resource_spans.append({"resource": {"attributes": attrs}, "scopeSpans": [{"scope": {"name": "d7-corpus"}, "spans": group}]})
            exports.append({"case": case, "export": step, "request": {"resourceSpans": resource_spans}})
    return exports


def fixture(requests_path, rows_path, versions_path):
    with open(requests_path) as f:
        exports = json.load(f)
    with open(rows_path) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    with open(versions_path) as f:
        versions = json.load(f)
    out = {}
    for export in exports:
        case = out.setdefault(export["case"], {"case": export["case"], "exports": [], "observed": []})
        case["exports"].append(export["request"])
        for rs in export["request"]["resourceSpans"]:
            for a in rs["resource"]["attributes"]:
                prefix = a["value"]["stringValue"][:-1]
                case["prefix"] = prefix
    for case in out.values():
        prefix = case.pop("prefix")
        case["observed"] = sorted(
            [r["name"], r["serviceName"][len(prefix):]] for r in rows if r["serviceName"].startswith(prefix)
        )
    return {"versions": versions, "cases": list(out.values())}


def main():
    if sys.argv[1] == "requests":
        json.dump(requests(int(sys.argv[2], 16), int(sys.argv[3])), sys.stdout, indent=1)
    elif sys.argv[1] == "fixture":
        json.dump(fixture(*sys.argv[2:5]), sys.stdout, indent=1)
    else:
        sys.exit(f"unknown command {sys.argv[1]}")


if __name__ == "__main__":
    main()
