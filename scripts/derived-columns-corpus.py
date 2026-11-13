#!/usr/bin/env python3
"""The D6 observation corpus: OTLP spans that each vary one input of the
derived span columns, and the fixture pairing each span with the row the
ClickHouse pipeline wrote for it.

  derived-columns-corpus.py request RUN_HEX START_UNIX_NANO [derived|host-keys] > request.json
  derived-columns-corpus.py fixture request.json rows.json versions.json > fixture.json
"""

import json
import sys

KINDS = {
    "UNSPECIFIED": 0,
    "INTERNAL": 1,
    "SERVER": 2,
    "CLIENT": 3,
    "PRODUCER": 4,
    "CONSUMER": 5,
}
COLUMNS = ["kind", "http_url", "http_host", "external_http_method", "external_http_url", "http_method"]


def s(v):
    return {"stringValue": v}


def i(v):
    return {"intValue": str(v)}


NULL = {}
MISSING = None

FULL = {
    "url.full": s("https://shop.d6.test:8443/cart/add?item=7"),
    "server.address": s("shop.d6.test"),
    "server.port": i(8443),
    "http.request.method": s("POST"),
}

VARIATIONS = {
    "url_full_only": {"url.full": s("https://a.d6.test/p?q=1")},
    "http_url_only": {"http.url": s("https://b.d6.test/p?q=2")},
    "url_full_and_http_url_conflict": {
        "url.full": s("https://new.d6.test/full"),
        "http.url": s("https://old.d6.test/legacy"),
    },
    "url_parts_stable": {
        "url.scheme": s("https"),
        "server.address": s("parts.d6.test"),
        "server.port": i(9443),
        "url.path": s("/a/b"),
        "url.query": s("x=1"),
    },
    "url_parts_legacy": {
        "http.scheme": s("http"),
        "http.host": s("legacy.d6.test:8080"),
        "http.target": s("/legacy?y=2"),
    },
    "url_path_only": {"url.path": s("/only/path")},
    "url_full_empty": {"url.full": s("")},
    "url_full_null": {"url.full": NULL},
    "url_full_value_missing": {"url.full": MISSING},
    "url_full_int": {"url.full": i(42)},
    "url_full_host_port": {"url.full": s("http://hp.d6.test:8081/customer?customer=392")},
    "url_full_ip": {"url.full": s("http://127.0.0.1:8080/ip")},
    "url_full_no_scheme": {"url.full": s("noscheme.d6.test/path")},
    "server_address_only": {"server.address": s("sa.d6.test")},
    "server_address_and_port": {"server.address": s("sap.d6.test"), "server.port": i(8443)},
    "server_address_with_port": {"server.address": s("sawp.d6.test:7443")},
    "net_peer_name_only": {"net.peer.name": s("npn.d6.test")},
    "net_peer_name_and_port": {"net.peer.name": s("npnp.d6.test"), "net.peer.port": i(6443)},
    "net_host_name_only": {"net.host.name": s("nhn.d6.test")},
    "net_host_name_and_port": {"net.host.name": s("nhnp.d6.test"), "net.host.port": i(5443)},
    "http_host_only": {"http.host": s("hh.d6.test")},
    "http_host_with_port": {"http.host": s("hhp.d6.test:4443")},
    "server_address_vs_net_peer_name": {"server.address": s("sa1.d6.test"), "net.peer.name": s("npn1.d6.test")},
    "server_address_vs_http_host": {"server.address": s("sa2.d6.test"), "http.host": s("hh2.d6.test")},
    "net_peer_name_vs_http_host": {"net.peer.name": s("npn3.d6.test"), "http.host": s("hh3.d6.test")},
    "server_address_vs_url_full_host": {
        "server.address": s("sa4.d6.test"),
        "url.full": s("https://uf4.d6.test/x"),
    },
    "server_address_empty": {"server.address": s("")},
    "server_address_null": {"server.address": NULL},
    "server_address_int": {"server.address": i(7)},
    "server_address_empty_net_peer_name": {"server.address": s(""), "net.peer.name": s("npn5.d6.test")},
    "peer_service_only": {"peer.service": s("ps.d6")},
    "method_stable_only": {"http.request.method": s("GET")},
    "method_legacy_only": {"http.method": s("PUT")},
    "method_stable_vs_legacy": {"http.request.method": s("PATCH"), "http.method": s("DELETE")},
    "method_lowercase": {"http.request.method": s("get")},
    "method_empty": {"http.request.method": s("")},
    "method_null": {"http.request.method": NULL},
    "method_int": {"http.request.method": i(1)},
    "method_empty_legacy": {"http.request.method": s(""), "http.method": s("HEAD")},
    "method_and_url_full": {"http.request.method": s("GET"), "url.full": s("https://mu.d6.test/m")},
    "method_and_server_address": {"http.request.method": s("GET"), "server.address": s("ms.d6.test")},
    "http_url_and_url_full_conflict": {
        "http.url": s("https://old.d6.test/legacy"),
        "url.full": s("https://new.d6.test/full"),
    },
    "url_full_then_http_url_empty": {"url.full": s("https://ue.d6.test/x"), "http.url": s("")},
    "http_url_int": {"http.url": i(42)},
    "url_full_then_server_address": {"url.full": s("https://uf6.d6.test/x"), "server.address": s("sa6.d6.test")},
    "net_peer_name_vs_server_address": {"net.peer.name": s("npn7.d6.test"), "server.address": s("sa7.d6.test")},
    "http_host_vs_server_address": {"http.host": s("hh8.d6.test"), "server.address": s("sa8.d6.test")},
    "http_host_vs_net_peer_name": {"http.host": s("hh9.d6.test"), "net.peer.name": s("npn9.d6.test")},
    "server_address_then_net_peer_name_empty": {"server.address": s("sa10.d6.test"), "net.peer.name": s("")},
    "http_host_int": {"http.host": i(8)},
    "net_peer_name_int": {"net.peer.name": i(9)},
    "server_address_bool": {"server.address": {"boolValue": True}},
    "method_legacy_vs_stable": {"http.method": s("DELETE"), "http.request.method": s("PATCH")},
    "method_stable_then_legacy_empty": {"http.request.method": s("GET"), "http.method": s("")},
    "method_double": {"http.request.method": {"doubleValue": 1.5}},
    "method_bool": {"http.request.method": {"boolValue": True}},
    "server_address_then_net_peer_name_int": {"server.address": s("sa11.d6.test"), "net.peer.name": i(11)},
    "url_full_then_http_url_int": {"url.full": s("https://ui.d6.test/x"), "http.url": i(12)},
    "method_stable_then_legacy_int": {"http.request.method": s("GET"), "http.method": i(5)},
    "method_legacy_int_then_stable": {"http.method": i(5), "http.request.method": s("GET")},
    "server_address_empty_then_url_full": {"server.address": s(""), "url.full": s("https://se.d6.test/x")},
    "url_full_then_server_address_empty": {"url.full": s("https://us.d6.test/x"), "server.address": s("")},
    "url_full_userinfo_upper": {"url.full": s("https://User:pw@UP.D6.test:1/x")},
    "url_full_ipv6": {"url.full": s("http://[::1]:8080/x")},
    "url_full_scheme_only": {"url.full": s("https://")},
    "url_full_relative": {"url.full": s("/relative/path?q=1")},
    "url_full_empty_then_http_url": {"url.full": s(""), "http.url": s("https://ue2.d6.test/x")},
    "url_full_int_then_http_url": {"url.full": i(13), "http.url": s("https://ui2.d6.test/x")},
    "url_full_unparsable_then_http_url": {"url.full": s("noscheme.d6.test/x"), "http.url": s("https://up2.d6.test/x")},
    "url_full_bad_port": {"url.full": s("http://bp.d6.test:port/x")},
    "url_full_bad_scheme": {"url.full": s("1http://bs.d6.test/x")},
    "url_full_fragment": {"url.full": s("https://fr.d6.test#frag")},
    "url_full_query_no_path": {"url.full": s("https://qn.d6.test?q=1")},
    "url_full_empty_port": {"url.full": s("http://ep.d6.test:/x")},
    "nothing": {},
}

RESOURCE_CASES = {
    "resource_host_name_only": {"host.name": s("rhost.d6.test")},
    "resource_server_address": {"server.address": s("rsa.d6.test")},
}


def arr(*values):
    return {"arrayValue": {"values": [s(v) for v in values]}}


# The keys semconv names in place of the deprecated http.host: client.address, the client as the
# server sees it, and http.request.header.host, an opt-in string[] header attribute.
HOST_KEY_VARIATIONS = {
    "client_address_only": {"client.address": s("ca.d6.test")},
    "client_address_ip": {"client.address": s("10.0.0.7")},
    "client_address_empty": {"client.address": s("")},
    "client_address_and_port": {"client.address": s("cap.d6.test"), "client.port": i(51000)},
    "header_host_array": {"http.request.header.host": arr("hh.d6.test")},
    "header_host_array_with_port": {"http.request.header.host": arr("hhp.d6.test:8080")},
    "header_host_array_two": {"http.request.header.host": arr("hh1.d6.test", "hh2.d6.test")},
    "header_host_array_empty": {"http.request.header.host": {"arrayValue": {}}},
    "header_host_array_empty_string": {"http.request.header.host": arr("")},
    "header_host_string": {"http.request.header.host": s("hs.d6.test")},
    "server_address_then_client_address": {"server.address": s("sa1.d6.test"), "client.address": s("ca1.d6.test")},
    "client_address_then_server_address": {"client.address": s("ca2.d6.test"), "server.address": s("sa2.d6.test")},
    "client_address_then_server_address_empty": {"client.address": s("ca3.d6.test"), "server.address": s("")},
    "client_address_vs_net_peer_name": {"client.address": s("ca4.d6.test"), "net.peer.name": s("npn4.d6.test")},
    "net_peer_name_vs_client_address": {"net.peer.name": s("npn5.d6.test"), "client.address": s("ca5.d6.test")},
    "client_address_vs_http_host": {"client.address": s("ca6.d6.test"), "http.host": s("hh6.d6.test")},
    "http_host_vs_client_address": {"http.host": s("hh7.d6.test"), "client.address": s("ca7.d6.test")},
    "server_address_then_header_host": {"server.address": s("sa8.d6.test"), "http.request.header.host": arr("hh8.d6.test")},
    "header_host_then_server_address": {"http.request.header.host": arr("hh9.d6.test"), "server.address": s("sa9.d6.test")},
    "header_host_then_server_address_empty": {"http.request.header.host": arr("hh10.d6.test"), "server.address": s("")},
    "client_address_vs_header_host": {"client.address": s("ca11.d6.test"), "http.request.header.host": arr("hh11.d6.test")},
    "header_host_vs_client_address": {"http.request.header.host": arr("hh12.d6.test"), "client.address": s("ca12.d6.test")},
    "header_host_string_then_server_address": {"http.request.header.host": s("hs13.d6.test"), "server.address": s("sa13.d6.test")},
    "client_address_and_url_full": {"client.address": s("ca14.d6.test"), "url.full": s("https://uf14.d6.test/x")},
    "url_full_and_client_address": {"url.full": s("https://uf15.d6.test/x"), "client.address": s("ca15.d6.test")},
    "header_host_and_url_full": {"http.request.header.host": arr("hh16.d6.test"), "url.full": s("https://uf16.d6.test/x")},
    "client_address_empty_and_url_full": {"client.address": s(""), "url.full": s("https://uf17.d6.test/x")},
}


def attributes(attrs):
    out = []
    for key, value in attrs.items():
        if value is MISSING:
            out.append({"key": key})
        else:
            out.append({"key": key, "value": value})
    return out


def host_key_cases():
    out = []
    for kind in KINDS:
        out.append((f"kind_{kind.lower()}_client_address", kind, {"client.address": s("k.d6.test")}, {}))
        out.append((f"kind_{kind.lower()}_header_host", kind, {"http.request.header.host": arr("kh.d6.test")}, {}))
    for kind in ("SERVER", "CLIENT"):
        for name, attrs in HOST_KEY_VARIATIONS.items():
            out.append((f"{kind.lower()}_{name}", kind, attrs, {}))
    return out


def cases():
    out = []
    for kind in KINDS:
        out.append((f"kind_{kind.lower()}_full", kind, FULL, {}))
    for kind in ("SERVER", "CLIENT", "INTERNAL"):
        for name, attrs in VARIATIONS.items():
            out.append((f"{kind.lower()}_{name}", kind, attrs, {}))
    for kind in ("SERVER", "CLIENT"):
        for name, attrs in RESOURCE_CASES.items():
            out.append((f"{kind.lower()}_{name}", kind, {"http.request.method": s("GET")}, attrs))
    return out


def ids(run, n):
    return f"d6c0{run:08x}{0:012x}{n:08x}", f"d6{n:014x}"


CORPORA = {"derived": cases, "host-keys": host_key_cases}


def request(run, start, corpus="derived"):
    by_resource = {}
    for n, (name, kind, attrs, resource) in enumerate(CORPORA[corpus](), start=1):
        trace_id, span_id = ids(run, n)
        res = {"service.name": s(f"d6-corpus-{run:08x}"), **resource}
        key = json.dumps(res, sort_keys=True)
        by_resource.setdefault(key, (res, []))[1].append(
            {
                "traceId": trace_id,
                "spanId": span_id,
                "name": name,
                "kind": KINDS[kind],
                "startTimeUnixNano": str(start + n * 1_000_000),
                "endTimeUnixNano": str(start + n * 1_000_000 + 500_000),
                "attributes": attributes(attrs),
                "status": {},
            }
        )
    return {
        "resourceSpans": [
            {"resource": {"attributes": attributes(res)}, "scopeSpans": [{"scope": {"name": "d6-corpus"}, "spans": spans}]}
            for res, spans in by_resource.values()
        ]
    }


def fixture(request_path, rows_path, versions_path):
    with open(request_path) as f:
        req = json.load(f)
    with open(rows_path) as f:
        rows = {r["span_id"]: r for r in (json.loads(line) for line in f if line.strip())}
    with open(versions_path) as f:
        versions = json.load(f)
    out = []
    for rs in req["resourceSpans"]:
        for ss in rs["scopeSpans"]:
            for span in ss["spans"]:
                row = rows.get(span["spanId"])
                out.append(
                    {
                        "case": span["name"],
                        "resource": rs["resource"]["attributes"],
                        "span": {"kind": span["kind"], "attributes": span["attributes"]},
                        "observed": None if row is None else {c: row[c] for c in COLUMNS},
                    }
                )
    return {"versions": versions, "request": req, "cases": out}


def main():
    if sys.argv[1] == "request":
        json.dump(request(int(sys.argv[2], 16), int(sys.argv[3]), *sys.argv[4:5]), sys.stdout, indent=1)
    elif sys.argv[1] == "fixture":
        json.dump(fixture(*sys.argv[2:5]), sys.stdout, indent=1, ensure_ascii=False)
    else:
        sys.exit(f"unknown command {sys.argv[1]}")


if __name__ == "__main__":
    main()
