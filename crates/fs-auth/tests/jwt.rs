//! Tenant JWTs signed with the fixtures' Ed25519 keys through the rustls
//! provider, verified at a fixed clock.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fs_auth::{JwtError, JwtRules, JwtVerifier, Reload};
use serde_json::{Value, json};
use tokio_rustls::rustls::SignatureScheme;
use tokio_rustls::rustls::crypto::ring::default_provider;
use tokio_rustls::rustls::pki_types::PrivateKeyDer;
use tokio_rustls::rustls::pki_types::pem::PemObject;

/// 2026-10-30T12:00:00Z.
const NOW: u64 = 1_793_361_600;
const TENANT: &str = "6f1c2b9e-1d2a-4c55-9a51-0b7f8c3e2d10";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn rules() -> JwtRules {
    JwtRules {
        issuer: "fusescope-backend".into(),
        audience: "fusescope-engine".into(),
        max_ttl: Duration::from_secs(300),
        leeway: Duration::from_secs(30),
    }
}

fn verifier(jwks: &Path) -> std::sync::Arc<JwtVerifier> {
    JwtVerifier::load(jwks.to_owned(), rules(), Duration::from_secs(3600)).unwrap()
}

fn segment(value: &Value) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap())
}

/// `header.claims` signed with the fixture key of `kid`.
fn sign(kid: &str, header: &Value, claims: &Value) -> String {
    let signed = format!("{}.{}", segment(header), segment(claims));
    let key = default_provider()
        .key_provider
        .load_private_key(PrivateKeyDer::from_pem_file(fixture(&format!("jwt-{kid}.key"))).unwrap())
        .unwrap();
    let signature = key
        .choose_scheme(&[SignatureScheme::ED25519])
        .unwrap()
        .sign(signed.as_bytes())
        .unwrap();
    format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature))
}

fn header(kid: &str) -> Value {
    json!({"alg": "EdDSA", "typ": "JWT", "kid": kid})
}

fn claims() -> Value {
    json!({
        "iss": "fusescope-backend",
        "aud": "fusescope-engine",
        "sub": "user:42",
        "fusescope_tenant": TENANT,
        "iat": NOW - 10,
        "exp": NOW + 200,
    })
}

fn with(mut value: Value, key: &str, field: Value) -> Value {
    value[key] = field;
    value
}

fn without(mut value: Value, key: &str) -> Value {
    value.as_object_mut().unwrap().remove(key);
    value
}

#[test]
fn a_valid_token_names_its_tenant_and_subject() {
    let v = verifier(&fixture("jwks-a.json"));
    let verified = v
        .verify_at(&sign("a", &header("a"), &claims()), NOW)
        .unwrap();
    assert_eq!(verified.tenant.to_string(), TENANT);
    assert_eq!(verified.subject, "user:42");
    assert_eq!(verified.expires, NOW + 200);
    let listed = with(claims(), "aud", json!(["other", "fusescope-engine"]));
    v.verify_at(&sign("a", &header("a"), &listed), NOW).unwrap();
}

#[test]
fn every_refusal_has_its_reason() {
    let v = verifier(&fixture("jwks-a.json"));
    let good = sign("a", &header("a"), &claims());
    let mut tampered = good.clone();
    let middle = tampered.rfind('.').unwrap() - 3;
    tampered.replace_range(middle..=middle, "A");
    let cases: Vec<(String, JwtError)> = vec![
        ("a.b".into(), JwtError::Malformed),
        (format!("{good}.x"), JwtError::Malformed),
        (
            "x".repeat(fs_auth::MAX_TOKEN_BYTES + 1),
            JwtError::Malformed,
        ),
        (
            sign("a", &json!({"alg": "none", "kid": "a"}), &claims()),
            JwtError::Header,
        ),
        (
            sign("a", &json!({"alg": "HS256", "kid": "a"}), &claims()),
            JwtError::Header,
        ),
        (
            sign("a", &json!({"alg": "EdDSA"}), &claims()),
            JwtError::Header,
        ),
        (
            sign(
                "a",
                &json!({"alg": "EdDSA", "kid": "a", "crit": ["x"]}),
                &claims(),
            ),
            JwtError::Header,
        ),
        (sign("b", &header("b"), &claims()), JwtError::UnknownKey),
        (sign("b", &header("a"), &claims()), JwtError::Signature),
        (tampered, JwtError::Signature),
        (
            sign("a", &header("a"), &with(claims(), "iss", json!("other"))),
            JwtError::Claim("iss"),
        ),
        (
            sign("a", &header("a"), &with(claims(), "aud", json!("other"))),
            JwtError::Claim("aud"),
        ),
        (
            sign("a", &header("a"), &without(claims(), "aud")),
            JwtError::Claim("aud"),
        ),
        (
            sign(
                "a",
                &header("a"),
                &with(
                    with(claims(), "iat", json!(NOW - 400)),
                    "exp",
                    json!(NOW - 31),
                ),
            ),
            JwtError::Claim("exp"),
        ),
        (
            sign("a", &header("a"), &with(claims(), "exp", json!(NOW + 300))),
            JwtError::Claim("exp"),
        ),
        (
            sign("a", &header("a"), &without(claims(), "exp")),
            JwtError::Claim("exp"),
        ),
        (
            sign("a", &header("a"), &with(claims(), "exp", json!(1.5))),
            JwtError::Claim("exp"),
        ),
        (
            sign("a", &header("a"), &with(claims(), "iat", json!(NOW + 31))),
            JwtError::Claim("iat"),
        ),
        (
            sign("a", &header("a"), &with(claims(), "nbf", json!(NOW + 31))),
            JwtError::Claim("nbf"),
        ),
        (
            sign("a", &header("a"), &with(claims(), "sub", json!(""))),
            JwtError::Claim("sub"),
        ),
        (
            sign(
                "a",
                &header("a"),
                &with(
                    claims(),
                    "sub",
                    json!("x".repeat(fs_auth::MAX_SUBJECT_BYTES + 1)),
                ),
            ),
            JwtError::Claim("sub"),
        ),
        (
            sign(
                "a",
                &header("a"),
                &with(claims(), "fusescope_tenant", json!("acme")),
            ),
            JwtError::Claim("fusescope_tenant"),
        ),
        (
            sign("a", &header("a"), &without(claims(), "fusescope_tenant")),
            JwtError::Claim("fusescope_tenant"),
        ),
    ];
    for (token, reason) in cases {
        assert_eq!(v.verify_at(&token, NOW), Err(reason.clone()), "{reason}");
    }
    assert!(
        v.verify_at(
            &sign(
                "a",
                &header("a"),
                &with(
                    with(claims(), "iat", json!(NOW - 100)),
                    "exp",
                    json!(NOW - 29)
                )
            ),
            NOW
        )
        .is_ok(),
        "within the leeway"
    );
}

#[test]
fn a_key_rotation_overlaps_without_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let jwks = dir.path().join("jwks.json");
    std::fs::copy(fixture("jwks-a.json"), &jwks).unwrap();
    let v = verifier(&jwks);
    let by_a = sign("a", &header("a"), &claims());
    let by_b = sign("b", &header("b"), &claims());
    assert!(v.verify_at(&by_a, NOW).is_ok());
    assert_eq!(v.verify_at(&by_b, NOW), Err(JwtError::UnknownKey));

    std::fs::copy(fixture("jwks-ab.json"), &jwks).unwrap();
    assert_eq!(v.reload().unwrap(), Reload::Reloaded);
    assert!(v.verify_at(&by_a, NOW).is_ok());
    assert!(v.verify_at(&by_b, NOW).is_ok());

    std::fs::copy(fixture("jwks-b.json"), &jwks).unwrap();
    assert_eq!(v.reload().unwrap(), Reload::Reloaded);
    assert_eq!(v.verify_at(&by_a, NOW), Err(JwtError::UnknownKey));
    assert!(v.verify_at(&by_b, NOW).is_ok());

    std::fs::write(&jwks, b"{\"keys\":[]}").unwrap();
    assert!(v.reload().is_err());
    assert!(v.stale(), "the failed reload is reported");
    assert!(v.verify_at(&by_b, NOW).is_ok(), "the last good set stays");
    assert_eq!(
        v.stats()
            .reload_failures
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    std::fs::copy(fixture("jwks-b.json"), &jwks).unwrap();
    assert_eq!(v.reload().unwrap(), Reload::Reloaded);
    assert!(!v.stale());
}

#[test]
fn a_key_set_that_does_not_load_refuses_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.json");
    assert!(matches!(
        JwtVerifier::load(missing, rules(), Duration::from_secs(60)),
        Err(JwtError::Keys { .. })
    ));
    let tls_key = fixture("server-a.key");
    assert!(matches!(
        JwtVerifier::load(tls_key, rules(), Duration::from_secs(60)),
        Err(JwtError::Keys { .. })
    ));
}
