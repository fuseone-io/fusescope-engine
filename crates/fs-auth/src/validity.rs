//! Certificate validity periods, read from the DER as in `anchors`.
//!
//! `UTCTime` (`YYMMDDHHMMSSZ`, years 1950 to 2049) and `GeneralizedTime`
//! (`YYYYMMDDHHMMSSZ`) are the two encodings RFC 5280 allows, always in
//! UTC with seconds and without fractions.

use std::fmt;

use crate::anchors::{tlv, validity};

const UTC_TIME: u8 = 0x17;
const GENERALIZED_TIME: u8 = 0x18;

/// A moment of a validity period, to the second, in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Moment {
    year: i64,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
}

impl Moment {
    /// Seconds since the Unix epoch.
    fn unix(self) -> i64 {
        let (y, m) = if self.month <= 2 {
            (self.year - 1, i64::from(self.month) + 9)
        } else {
            (self.year, i64::from(self.month) - 3)
        };
        let era = y.div_euclid(400);
        let year_of_era = y.rem_euclid(400);
        let day_of_year = (153 * m + 2) / 5 + i64::from(self.day) - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        let days = era * 146_097 + day_of_era - 719_468;
        days * 86_400
            + i64::from(self.hour) * 3600
            + i64::from(self.minute) * 60
            + i64::from(self.second)
    }
}

impl fmt::Display for Moment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }
}

/// When a certificate may be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Validity {
    not_before: Moment,
    not_after: Moment,
}

impl Validity {
    /// The validity of the DER certificate `der`.
    pub(crate) fn of(der: &[u8]) -> Option<Self> {
        let (not_before, rest) = moment(validity(der)?)?;
        let (not_after, rest) = moment(rest)?;
        rest.is_empty().then_some(Self {
            not_before,
            not_after,
        })
    }

    /// Why the certificate cannot be used at `now`, seconds since the
    /// epoch; `None` when it can.
    pub(crate) fn refusal(&self, now: i64) -> Option<String> {
        if now < self.not_before.unix() {
            return Some(format!("is not valid before {}", self.not_before));
        }
        (now > self.not_after.unix()).then(|| format!("expired at {}", self.not_after))
    }
}

/// The time at the start of `input` and what follows it.
fn moment(input: &[u8]) -> Option<(Moment, &[u8])> {
    let (year, fields, rest) = if let Some((time, rest)) = tlv(input, UTC_TIME) {
        let (yy, fields) = number(time.value, 2)?;
        (if yy < 50 { 2000 + yy } else { 1900 + yy }, fields, rest)
    } else {
        let (time, rest) = tlv(input, GENERALIZED_TIME)?;
        let (year, fields) = number(time.value, 4)?;
        (year, fields, rest)
    };
    let fields = fields.strip_suffix(b"Z")?;
    if fields.len() != 10 {
        return None;
    }
    let field = |at: usize, min: i64, max: i64| {
        let (value, _) = number(fields.get(at..)?, 2)?;
        u8::try_from(value)
            .ok()
            .filter(|_| (min..=max).contains(&value))
    };
    let moment = Moment {
        year,
        month: field(0, 1, 12)?,
        day: field(2, 1, 31)?,
        hour: field(4, 0, 23)?,
        minute: field(6, 0, 59)?,
        second: field(8, 0, 59)?,
    };
    Some((moment, rest))
}

/// The first `len` bytes of `input` as a decimal number, and the rest.
fn number(input: &[u8], len: usize) -> Option<(i64, &[u8])> {
    let (digits, rest) = input.split_at_checked(len)?;
    let value = digits.iter().try_fold(0i64, |value, &d| {
        d.is_ascii_digit().then(|| value * 10 + i64::from(d - b'0'))
    })?;
    Some((value, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(tag: u8, text: &str) -> Vec<u8> {
        let mut der = vec![tag, u8::try_from(text.len()).unwrap()];
        der.extend_from_slice(text.as_bytes());
        der
    }

    fn at(tag: u8, text: &str) -> Option<String> {
        moment(&time(tag, text)).map(|(m, _)| m.to_string())
    }

    #[test]
    fn both_encodings_read_to_the_second() {
        assert_eq!(
            at(UTC_TIME, "491231235959Z").as_deref(),
            Some("2049-12-31T23:59:59Z")
        );
        assert_eq!(
            at(UTC_TIME, "500101000000Z").as_deref(),
            Some("1950-01-01T00:00:00Z")
        );
        assert_eq!(
            at(GENERALIZED_TIME, "21250101000000Z").as_deref(),
            Some("2125-01-01T00:00:00Z")
        );
        let epoch = moment(&time(UTC_TIME, "700101000000Z")).unwrap().0;
        assert_eq!(epoch.unix(), 0);
        let leap = moment(&time(GENERALIZED_TIME, "20240229120000Z"))
            .unwrap()
            .0;
        assert_eq!(leap.unix(), 1_709_208_000);
    }

    #[test]
    fn malformed_times_do_not_read() {
        for (tag, text) in [
            (UTC_TIME, "491231235959"),
            (UTC_TIME, "4912312359Z"),
            (UTC_TIME, "491331235959Z"),
            (UTC_TIME, "491200235959Z"),
            (UTC_TIME, "491231245959Z"),
            (UTC_TIME, "49123123595aZ"),
            (GENERALIZED_TIME, "20491231235959.5Z"),
            (0x04, "491231235959Z"),
        ] {
            assert_eq!(at(tag, text), None, "{text}");
        }
    }

    #[test]
    fn a_refusal_names_the_bound_passed() {
        let validity = Validity {
            not_before: moment(&time(UTC_TIME, "000101000000Z")).unwrap().0,
            not_after: moment(&time(UTC_TIME, "001231000000Z")).unwrap().0,
        };
        let start = validity.not_before.unix();
        let end = validity.not_after.unix();
        assert_eq!(validity.refusal(start), None);
        assert_eq!(validity.refusal(end), None);
        assert_eq!(
            validity.refusal(start - 1).as_deref(),
            Some("is not valid before 2000-01-01T00:00:00Z")
        );
        assert_eq!(
            validity.refusal(end + 1).as_deref(),
            Some("expired at 2000-12-31T00:00:00Z")
        );
    }
}
