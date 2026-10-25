//! Deterministic OTLP trace generator.
//!
//! Produces requests that look like a service mesh: a fixed set of services and
//! operations, HTTP attributes on server spans, a parent chain per trace,
//! log-normal-ish durations and a configurable fraction of errors. Seeded, so
//! two runs with the same parameters produce the same bytes.

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

/// Shape of the generated traffic.
#[derive(Debug, Clone)]
pub struct Shape {
    /// Distinct `service.name` values.
    pub services: usize,
    /// Distinct operations per service.
    pub operations: usize,
    /// Spans per trace (one trace per request).
    pub spans_per_trace: usize,
    /// Extra string attributes per span, beyond the semantic-convention ones.
    pub extra_attributes: usize,
    /// Fraction of spans with an error status, 0.0 to 1.0.
    pub error_rate: f64,
    /// Time window the timestamps are spread over, in seconds, ending now.
    pub window_secs: u64,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            services: 20,
            operations: 15,
            spans_per_trace: 12,
            extra_attributes: 6,
            error_rate: 0.02,
            window_secs: 3600,
        }
    }
}

/// xorshift64*: small, fast, deterministic. Not for security.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seeded generator; the same seed gives the same sequence.
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A random element of a non-empty slice.
    pub fn pick<'a>(&mut self, items: &'a [&'a str]) -> &'a str {
        items.get(self.below(items.len())).copied().unwrap_or("")
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        usize::try_from(self.next_u64() % n as u64).unwrap_or(0)
    }

    /// Uniform in `[0, 1)`.
    #[allow(clippy::cast_precision_loss)]
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn bytes(&mut self, out: &mut [u8]) {
        for chunk in out.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            for (dst, src) in chunk.iter_mut().zip(v.iter()) {
                *dst = *src;
            }
        }
    }
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..KeyValue::default()
    }
}

fn s(v: impl Into<String>) -> Value {
    Value::StringValue(v.into())
}

const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const ROUTES: [&str; 6] = [
    "/api/orders/{id}",
    "/api/users/{id}",
    "/api/cart",
    "/api/checkout",
    "/healthz",
    "/api/search",
];

/// Generator state.
pub struct Generator {
    shape: Shape,
    rng: Rng,
    now_ns: u64,
}

impl Generator {
    /// Generator for `shape` with timestamps ending at `now_ns`.
    pub fn new(shape: Shape, seed: u64, now_ns: u64) -> Self {
        Self {
            shape,
            rng: Rng::new(seed),
            now_ns,
        }
    }

    /// One request: one trace of `spans_per_trace` spans under one service.
    pub fn request(&mut self) -> ExportTraceServiceRequest {
        let service_index = self.rng.below(self.shape.services);
        let service = format!("svc-{service_index:02}");
        let mut trace_id = [0u8; 16];
        self.rng.bytes(&mut trace_id);
        let window_ns = self.shape.window_secs.saturating_mul(1_000_000_000);
        let trace_start =
            self.now_ns.saturating_sub(window_ns) + (self.rng.next_u64() % window_ns.max(1));

        let mut spans = Vec::with_capacity(self.shape.spans_per_trace);
        let mut parent: Option<[u8; 8]> = None;
        let mut cursor = trace_start;
        for i in 0..self.shape.spans_per_trace {
            let mut span_id = [0u8; 8];
            self.rng.bytes(&mut span_id);
            // Roughly log-normal: 50 µs to 500 ms, skewed low.
            let duration_ns = (50_000.0 * (1.0 / (1.0 - self.rng.unit().min(0.9999))).powf(1.7))
                .min(500_000_000.0);
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let duration_ns = duration_ns as u64;
            let error = self.rng.unit() < self.shape.error_rate;
            let operation = self.rng.below(self.shape.operations);
            let mut attributes = vec![
                kv("http.request.method", s(self.rng.pick(&METHODS))),
                kv("http.route", s(self.rng.pick(&ROUTES))),
                kv(
                    "http.response.status_code",
                    Value::IntValue(if error { 500 } else { 200 }),
                ),
                kv("user.id", s(format!("u{}", self.rng.below(100_000)))),
                kv(
                    "net.peer.port",
                    Value::IntValue(i64::try_from(1024 + self.rng.below(60_000)).unwrap_or(0)),
                ),
                kv("retry", Value::BoolValue(self.rng.below(10) == 0)),
            ];
            for a in 0..self.shape.extra_attributes {
                attributes.push(kv(
                    &format!("app.attr{a}"),
                    s(format!("v{}", self.rng.below(50))),
                ));
            }
            spans.push(Span {
                trace_id: trace_id.to_vec(),
                span_id: span_id.to_vec(),
                parent_span_id: parent.map(|p| p.to_vec()).unwrap_or_default(),
                name: format!("op-{operation:02}"),
                kind: if i == 0 { 2 } else { 3 },
                start_time_unix_nano: cursor,
                end_time_unix_nano: cursor + duration_ns,
                attributes,
                status: Some(Status {
                    code: if error {
                        StatusCode::Error as i32
                    } else {
                        StatusCode::Unset as i32
                    },
                    message: if error {
                        "upstream timeout".into()
                    } else {
                        String::new()
                    },
                }),
                ..Span::default()
            });
            parent = Some(span_id);
            cursor += duration_ns / 4 + 1;
        }
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![
                        kv("service.name", s(service)),
                        kv("deployment.environment.name", s("bench")),
                        kv("host.name", s(format!("host-{}", service_index % 7))),
                        kv("k8s.pod.name", s(format!("pod-{}", self.rng.below(50)))),
                    ],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "fs-bench".into(),
                        version: "1".into(),
                        ..InstrumentationScope::default()
                    }),
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_shaped() {
        let shape = Shape {
            spans_per_trace: 5,
            ..Shape::default()
        };
        let a = Generator::new(shape.clone(), 7, 1_700_000_000_000_000_000).request();
        let b = Generator::new(shape, 7, 1_700_000_000_000_000_000).request();
        assert_eq!(a, b);
        let spans = &a.resource_spans[0].scope_spans[0].spans;
        assert_eq!(spans.len(), 5);
        assert_eq!(spans[0].parent_span_id.len(), 0);
        assert_eq!(spans[1].parent_span_id, spans[0].span_id);
        assert!(
            spans
                .iter()
                .all(|s| s.end_time_unix_nano > s.start_time_unix_nano)
        );
    }
}
