"""Checks the image's SBOM: a CycloneDX 1.5 JSON document whose root
component is the fusescope-engine binary, with a non-empty dependency graph
naming the engine's runtime crates.

usage: python3 scripts/sbom-check.py <sbom.cdx.json>
"""

import json
import sys

RUNTIME_CRATES = {"fs-ingest", "fs-query", "fs-compactor", "fs-auth", "fs-catalog", "fs-schema", "fs-wal"}


def fail(why: str) -> None:
    sys.exit(f"sbom: {why}")


def main(path: str) -> None:
    with open(path, encoding="utf-8") as handle:
        bom = json.load(handle)
    if bom.get("bomFormat") != "CycloneDX" or bom.get("specVersion") != "1.5":
        fail(f"not CycloneDX 1.5 JSON: {bom.get('bomFormat')} {bom.get('specVersion')}")
    root = bom.get("metadata", {}).get("component", {})
    if root.get("name") != "fusescope-engine" or root.get("type") != "application":
        fail(f"root component is {root.get('type')} {root.get('name')!r}, not the fusescope-engine binary")
    components = bom.get("components") or []
    names = {component.get("name") for component in components}
    missing = RUNTIME_CRATES - names
    if missing:
        fail(f"runtime crates missing from the components: {sorted(missing)}")
    dependencies = bom.get("dependencies") or []
    edges = sum(len(node.get("dependsOn") or []) for node in dependencies)
    if not dependencies or edges == 0:
        fail("empty dependency graph")
    refs = {component.get("bom-ref") for component in components} | {root.get("bom-ref")}
    dangling = [node.get("ref") for node in dependencies if node.get("ref") not in refs]
    if dangling:
        fail(f"dependency nodes naming no component: {dangling[:5]}")
    print(f"sbom: CycloneDX 1.5, root fusescope-engine, {len(components)} components, {edges} dependency edges")


if __name__ == "__main__":
    main(sys.argv[1])
