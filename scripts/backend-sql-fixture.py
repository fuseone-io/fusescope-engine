#!/usr/bin/env python3
"""Copy the Go backend's DataFusion trace statements into the fs-query contract fixture.

    scripts/backend-sql-fixture.py <backend-checkout>          write the fixture from HEAD there
    scripts/backend-sql-fixture.py --check [<backend-checkout>]  verify it, and that it matches HEAD

The statements are read from the backend commit, never its working tree, so the recorded
commit reproduces them. Only statements the dialect renders are kept; refusals stay in the Go
tests. The fixture is generated, never edited by hand: its sha256 covers the cases.
"""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

FORMAT_VERSION = 1
SOURCE = "pkg/statementbuilder/tracesstatementbuilder/testdata/datafusion_statements.json"
FIXTURE = Path(__file__).resolve().parent.parent / "crates/fs-query/tests/fixtures/backend_trace_sql.json"


def canonical_sha256(cases):
    body = json.dumps(cases, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def from_backend(checkout):
    commit = subprocess.run(
        ["git", "-C", checkout, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    source = subprocess.run(
        ["git", "-C", checkout, "show", f"{commit}:{SOURCE}"], check=True, capture_output=True, text=True
    ).stdout
    golden = json.loads(source)
    if golden.get("format_version") != FORMAT_VERSION:
        sys.exit(f"backend statements are format {golden.get('format_version')}, expected {FORMAT_VERSION}")
    cases = sorted(
        ({"name": c["name"], "sql": c["sql"]} for c in golden["cases"] if c.get("sql")),
        key=lambda c: c["name"],
    )
    if not cases:
        sys.exit("the backend renders no statement")
    return {
        "format_version": FORMAT_VERSION,
        "backend_commit": commit,
        "sha256": canonical_sha256(cases),
        "cases": cases,
    }


def check(checkout):
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if fixture.get("format_version") != FORMAT_VERSION:
        sys.exit(f"fixture is format {fixture.get('format_version')}, expected {FORMAT_VERSION}")
    if canonical_sha256(fixture["cases"]) != fixture["sha256"]:
        sys.exit("fixture cases do not match their sha256: regenerate it, never edit it")
    if checkout is not None:
        expected = from_backend(checkout)
        if expected["cases"] != fixture["cases"]:
            sys.exit(f"fixture differs from the backend at {expected['backend_commit']}")
    print(f"ok: {len(fixture['cases'])} statements from backend {fixture['backend_commit']}")


def main(args):
    if args and args[0] == "--check":
        check(args[1] if len(args) > 1 else None)
        return
    if len(args) != 1:
        sys.exit(__doc__)
    fixture = from_backend(args[0])
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(fixture['cases'])} statements from backend {fixture['backend_commit']}")


if __name__ == "__main__":
    main(sys.argv[1:])
