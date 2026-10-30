//! Client CA bundles as trust domains.
//!
//! A certificate verifies under an anchor when it is signed by the anchor's
//! key; serial, validity and even subject do not decide it. Two bundles
//! are therefore disjoint only when no key appears in both and every anchor
//! is a root, self-issued and signed by its own key: an intermediate or a
//! cross-certificate in one bundle can descend from a root in the other.
//! The few fields this needs are read from the DER here, so no parser
//! enters the dependency graph; the signature is checked by the crypto
//! provider rustls already uses.

use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::CertificateDer;

const SEQUENCE: u8 = 0x30;
const BIT_STRING: u8 = 0x03;
const INTEGER: u8 = 0x02;
const VERSION: u8 = 0xa0;

/// The parts of a certificate the checks read.
struct Parts<'a> {
    tbs: &'a [u8],
    signature_alg: &'a [u8],
    signature: &'a [u8],
    issuer: &'a [u8],
    subject: &'a [u8],
    spki: &'a [u8],
    key_alg: &'a [u8],
    key: &'a [u8],
}

/// The keys of the bundle's anchors, each one a self-signed root.
///
/// # Errors
/// A certificate does not parse, is not self-issued, or does not verify
/// under its own key; the message names its position in the bundle.
pub(crate) fn root_keys(
    bundle: &[CertificateDer<'_>],
    provider: &CryptoProvider,
) -> Result<Vec<Vec<u8>>, String> {
    bundle
        .iter()
        .enumerate()
        .map(|(n, cert)| {
            let at = |reason: &str| format!("certificate {} of the bundle {reason}", n + 1);
            let parts = parts(cert).ok_or_else(|| at("does not parse"))?;
            if parts.issuer != parts.subject {
                return Err(at("is not a root: its issuer is another CA"));
            }
            if !self_signed(&parts, provider) {
                return Err(at("is not a root: it is not signed by its own key"));
            }
            Ok(parts.spki.to_vec())
        })
        .collect()
}

fn self_signed(parts: &Parts<'_>, provider: &CryptoProvider) -> bool {
    provider
        .signature_verification_algorithms
        .all
        .iter()
        .filter(|alg| {
            alg.public_key_alg_id().as_ref() == parts.key_alg
                && alg.signature_alg_id().as_ref() == parts.signature_alg
        })
        .any(|alg| {
            alg.verify_signature(parts.key, parts.tbs, parts.signature)
                .is_ok()
        })
}

fn parts(der: &[u8]) -> Option<Parts<'_>> {
    let (certificate, rest) = tlv(der, SEQUENCE)?;
    if !rest.is_empty() {
        return None;
    }
    let (tbs_body, after_tbs) = tlv(certificate.value, SEQUENCE)?;
    let (signature_alg, after_alg) = tlv(after_tbs, SEQUENCE)?;
    let (signature, _) = tlv(after_alg, BIT_STRING)?;
    let mut fields = tbs_body.value;
    if fields.first() == Some(&VERSION) {
        fields = tlv(fields, VERSION)?.1;
    }
    let (_, fields) = tlv(fields, INTEGER)?;
    let (_, fields) = tlv(fields, SEQUENCE)?;
    let (issuer, fields) = tlv(fields, SEQUENCE)?;
    let (_, fields) = tlv(fields, SEQUENCE)?;
    let (subject, fields) = tlv(fields, SEQUENCE)?;
    let (spki, _) = tlv(fields, SEQUENCE)?;
    let (key_alg, after_key_alg) = tlv(spki.value, SEQUENCE)?;
    let (key, _) = tlv(after_key_alg, BIT_STRING)?;
    Some(Parts {
        tbs: tbs_body.whole,
        signature_alg: signature_alg.value,
        signature: bits(signature.value)?,
        issuer: issuer.value,
        subject: subject.value,
        spki: spki.whole,
        key_alg: key_alg.value,
        key: bits(key.value)?,
    })
}

/// A BIT STRING's bytes, which for keys and signatures have no unused bits.
fn bits(value: &[u8]) -> Option<&[u8]> {
    match value.split_first() {
        Some((0, rest)) => Some(rest),
        _ => None,
    }
}

struct Tlv<'a> {
    whole: &'a [u8],
    value: &'a [u8],
}

/// The element of `tag` at the start of `input` and what follows it.
fn tlv(input: &[u8], tag: u8) -> Option<(Tlv<'_>, &[u8])> {
    let (&found, rest) = input.split_first()?;
    if found != tag {
        return None;
    }
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 {
            return None;
        }
        let (octets, rest) = rest.split_at_checked(count)?;
        let len = octets
            .iter()
            .fold(0usize, |len, &octet| (len << 8) | usize::from(octet));
        (len, rest)
    };
    let (value, rest) = rest.split_at_checked(len)?;
    let header = input.len() - rest.len() - len;
    Some((
        Tlv {
            whole: input.get(..header + len)?,
            value,
        },
        rest,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_or_mistagged_input_does_not_parse() {
        assert!(tlv(&[], SEQUENCE).is_none());
        assert!(
            tlv(&[0x30, 0x05, 1, 2], SEQUENCE).is_none(),
            "shorter than its length"
        );
        assert!(tlv(&[0x31, 0x00], SEQUENCE).is_none(), "another tag");
        assert!(
            tlv(&[0x30, 0x85, 0, 0, 0, 0, 1], SEQUENCE).is_none(),
            "a length too long"
        );
        let (element, rest) = tlv(&[0x30, 0x81, 0x01, 7, 9], SEQUENCE).unwrap();
        assert_eq!(
            (element.value, element.whole, rest),
            (&[7][..], &[0x30, 0x81, 1, 7][..], &[9][..])
        );
        assert!(parts(&[0x30, 0x00]).is_none());
        assert!(bits(&[1, 0xff]).is_none(), "unused bits");
    }
}
