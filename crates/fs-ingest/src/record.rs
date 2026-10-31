//! WAL record framing: which tenant and signal a raw OTLP payload belongs to.
//!
//! The payload holds the protobuf exactly as received, after the seed of its
//! row ids for logs, so replay runs the same conversion code as live traffic
//! and produces identical rows.

use bytes::Bytes;
use fs_catalog::TenantId;

use crate::IngestError;

/// Record format version: rows, and intents covering every row of their
/// range.
pub const VERSION: u8 = 1;
/// Record format version of an intent holding one hour of its range. A
/// reader of the previous version meets it and fails, rather than reading
/// the intent as the whole range.
pub const VERSION_HOUR_INTENT: u8 = 2;
/// Record format version of an intent of a signal other than traces, with
/// or without an hour. A reader without that signal meets it and fails,
/// rather than reading the intent as one over spans.
pub const VERSION_SIGNAL_INTENT: u8 = 3;

/// Bytes of the seed at the head of a [`RecordSignal::Logs`] payload.
pub const LOG_SEED_BYTES: usize = 16;

/// The payload of a [`RecordSignal::Logs`] record.
pub fn log_payload(seed: [u8; LOG_SEED_BYTES], request: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(LOG_SEED_BYTES + request.len());
    out.extend_from_slice(&seed);
    out.extend_from_slice(request);
    Bytes::from(out)
}

/// The seed and the request of a [`RecordSignal::Logs`] payload, or `None`
/// when it is shorter than a seed.
pub fn split_log_payload(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    payload.split_at_checked(LOG_SEED_BYTES)
}

/// Signal carried by a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordSignal {
    /// `ExportTraceServiceRequest`.
    Traces = 1,
    /// A [`FlushIntent`]: the exact set of earlier records one file is made of.
    FlushIntent = 2,
    /// A [`LOG_SEED_BYTES`] seed, then an `ExportLogsServiceRequest`: the
    /// seed names the request for the ids of its rows, so a replay gives
    /// them the ids the live conversion did.
    Logs = 3,
}

impl RecordSignal {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Traces),
            2 => Some(Self::FlushIntent),
            3 => Some(Self::Logs),
            _ => None,
        }
    }
}

/// Written to the WAL before a flush uploads anything. After a crash, replay
/// groups the tenant's records in `[first, end)` under `file` again, so a
/// retried flush produces the same object and the same catalog row even if
/// later rows arrived meanwhile.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FlushIntent {
    /// Tenant whose rows the file holds.
    pub tenant: TenantId,
    /// Id derived from `(wal instance, tenant, first)`.
    pub file: fs_catalog::FileId,
    /// Position of the first record in the file.
    pub first: fs_wal::Position,
    /// Position just past the last record in the file.
    pub end: fs_wal::Position,
    /// How the bytes are laid out, so a retry reproduces them. Absent in
    /// intents logged before layouts were recorded: those retries use the
    /// current layout and trust the object they find.
    #[serde(default)]
    pub layout: Option<FileLayout>,
    /// Start of the UTC hour, in seconds since the epoch, whose rows of the
    /// range the file holds, when the buffer was cut by hour. Absent, the
    /// file holds every row of the range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hour: Option<i64>,
    /// Signal of the rows the file holds: an intent covers the tenant's
    /// records of that signal in its range. Absent in intents of traces.
    #[serde(default, skip_serializing_if = "RowSignal::is_traces")]
    pub signal: RowSignal,
}

/// Signal of a set of rows: what a buffer holds and a [`FlushIntent`] names.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum RowSignal {
    /// Span rows from [`RecordSignal::Traces`] records.
    #[default]
    Traces,
    /// Log rows from [`RecordSignal::Logs`] records.
    Logs,
}

impl RowSignal {
    /// The kind of record whose rows the intent covers.
    pub fn record(self) -> RecordSignal {
        match self {
            Self::Traces => RecordSignal::Traces,
            Self::Logs => RecordSignal::Logs,
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn is_traces(&self) -> bool {
        *self == Self::Traces
    }
}

/// The encoder and its parameters that turn a set of rows into one exact
/// sequence of bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileLayout {
    /// Encoder version; bumped whenever the same rows would give other bytes.
    pub version: u32,
    /// Rows per Parquet row group.
    pub row_group_rows: usize,
}

impl FileLayout {
    /// Version of the current encoder: per-batch sort, k-way merge, one row
    /// group at a time, bloom filters sized to the row group on `trace_id`,
    /// `span_id` and `service_name` alone. Version 3 is the same merge with
    /// a bloom filter on every column; it is still written for its intents.
    /// Version 2 had the writer's limit at twice the group and bloom filters
    /// sized by it; it never shipped and is refused like any other.
    pub const CURRENT_VERSION: u32 = 4;

    /// Versions this engine writes for rows of `signal`: the current one
    /// and those whose intents may still be in a log. Log files exist only
    /// from version 4 on.
    pub fn written_versions(signal: RowSignal) -> &'static [u32] {
        match signal {
            RowSignal::Traces => &[3, 4],
            RowSignal::Logs => &[4],
        }
    }

    /// The current encoder with `row_group_rows` rows per row group.
    pub fn current(row_group_rows: usize) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            row_group_rows,
        }
    }
}

impl FlushIntent {
    /// Serialises the intent as a WAL record payload, under the record
    /// version its signal and its hour need.
    pub fn to_record(&self) -> WalRecord {
        WalRecord {
            version: match (self.signal, self.hour) {
                (RowSignal::Logs, _) => VERSION_SIGNAL_INTENT,
                (RowSignal::Traces, Some(_)) => VERSION_HOUR_INTENT,
                (RowSignal::Traces, None) => VERSION,
            },
            tenant: self.tenant,
            signal: RecordSignal::FlushIntent,
            payload: Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
        }
    }

    /// Reads an intent from its record, refusing what the record's version
    /// does not allow: the first version names traces and no hour, the hour
    /// version traces and an hour, the signal version another signal, with
    /// or without an hour, and any hour is the start of a representable
    /// one. Anything else was not written by this engine's cut.
    ///
    /// # Errors
    /// Not an intent record, a malformed payload, or a signal or an hour the
    /// version does not allow.
    pub fn from_record(record: &WalRecord) -> Result<Self, IngestError> {
        if record.signal != RecordSignal::FlushIntent {
            return Err(IngestError::Record("not a flush intent".into()));
        }
        let intent = Self::from_payload(&record.payload)?;
        let allowed = match (record.version, intent.signal, intent.hour) {
            (VERSION, RowSignal::Traces, None)
            | (VERSION_HOUR_INTENT, RowSignal::Traces, Some(_))
            | (VERSION_SIGNAL_INTENT, RowSignal::Logs, _) => true,
            (VERSION | VERSION_HOUR_INTENT | VERSION_SIGNAL_INTENT, _, _) => false,
            (version, _, _) => {
                return Err(IngestError::Record(format!(
                    "unsupported version {version}"
                )));
            }
        };
        if !allowed {
            return Err(IngestError::Record(format!(
                "an intent of version {} names {:?} with hour {:?}",
                record.version, intent.signal, intent.hour
            )));
        }
        if let Some(secs) = intent.hour {
            let representable = chrono::DateTime::from_timestamp(secs, 0)
                .and_then(|hour| hour.timestamp_nanos_opt())
                .is_some();
            if secs.rem_euclid(3600) != 0 || !representable {
                return Err(IngestError::Record(format!(
                    "intent hour {secs} is not the start of an hour"
                )));
            }
        }
        Ok(intent)
    }

    /// The hour the file holds, when the buffer was cut by hour. Always
    /// representable for an intent read through [`Self::from_record`].
    pub fn hour_start(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.hour
            .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
    }

    /// Parses an intent payload.
    ///
    /// # Errors
    /// Malformed payload.
    pub fn from_payload(payload: &[u8]) -> Result<Self, IngestError> {
        serde_json::from_slice(payload)
            .map_err(|e| IngestError::Record(format!("bad flush intent: {e}")))
    }

    /// True when a record of `signal` of this tenant at `position` belongs
    /// to the file.
    pub fn covers(&self, tenant: TenantId, signal: RowSignal, position: fs_wal::Position) -> bool {
        self.tenant == tenant
            && self.signal == signal
            && position >= self.first
            && position < self.end
    }
}

/// Header plus payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Format version of the record.
    pub version: u8,
    /// Owner.
    pub tenant: TenantId,
    /// Payload type.
    pub signal: RecordSignal,
    /// Raw protobuf.
    pub payload: Bytes,
}

/// Bytes of the header: version, signal, tenant uuid.
pub const HEADER_LEN: usize = 1 + 1 + 16;

impl WalRecord {
    /// Serialises header and payload into one buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.push(self.version);
        out.push(self.signal as u8);
        out.extend_from_slice(self.tenant.0.as_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Parses a record written by [`WalRecord::encode`].
    ///
    /// # Errors
    /// Short buffer, unknown version or signal.
    pub fn decode(bytes: &[u8]) -> Result<Self, IngestError> {
        let (header, payload) = bytes
            .split_at_checked(HEADER_LEN)
            .ok_or_else(|| IngestError::Record(format!("short record: {} bytes", bytes.len())))?;
        let version = header.first().copied().unwrap_or(0);
        if ![VERSION, VERSION_HOUR_INTENT, VERSION_SIGNAL_INTENT].contains(&version) {
            return Err(IngestError::Record(format!(
                "unsupported version {version}"
            )));
        }
        let signal = RecordSignal::from_byte(header.get(1).copied().unwrap_or(0))
            .ok_or_else(|| IngestError::Record("unknown signal".into()))?;
        if version != VERSION && signal != RecordSignal::FlushIntent {
            return Err(IngestError::Record(format!(
                "version {version} is for flush intents only"
            )));
        }
        let uuid = header
            .get(2..18)
            .and_then(|b| uuid::Uuid::from_slice(b).ok())
            .ok_or_else(|| IngestError::Record("bad tenant id".into()))?;
        Ok(Self {
            version,
            tenant: TenantId(uuid),
            signal,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let record = WalRecord {
            version: VERSION,
            tenant: TenantId::new(),
            signal: RecordSignal::Traces,
            payload: Bytes::from_static(b"otlp"),
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), HEADER_LEN + 4);
        assert_eq!(WalRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn intent_round_trip_and_coverage() {
        let tenant = TenantId::new();
        let intent = FlushIntent {
            tenant,
            file: fs_catalog::FileId::new(),
            first: fs_wal::Position {
                segment: 1,
                offset: 16,
            },
            end: fs_wal::Position {
                segment: 1,
                offset: 4096,
            },
            layout: Some(FileLayout::current(4096)),
            hour: None,
            signal: RowSignal::Traces,
        };
        let record = intent.to_record();
        assert_eq!(record.signal, RecordSignal::FlushIntent);
        let decoded = WalRecord::decode(&record.encode()).unwrap();
        assert_eq!(FlushIntent::from_payload(&decoded.payload).unwrap(), intent);
        assert!(intent.covers(
            tenant,
            RowSignal::Traces,
            fs_wal::Position {
                segment: 1,
                offset: 16
            }
        ));
        assert!(intent.covers(
            tenant,
            RowSignal::Traces,
            fs_wal::Position {
                segment: 1,
                offset: 4095
            }
        ));
        assert!(
            !intent.covers(
                tenant,
                RowSignal::Traces,
                fs_wal::Position {
                    segment: 1,
                    offset: 4096
                }
            ),
            "end is exclusive"
        );
        assert!(!intent.covers(
            TenantId::new(),
            RowSignal::Traces,
            fs_wal::Position {
                segment: 1,
                offset: 100
            }
        ));
        assert!(FlushIntent::from_payload(b"nope").is_err());

        // An intent logged before layouts were recorded.
        let old = format!(
            r#"{{"tenant":"{}","file":"{}","first":{{"segment":1,"offset":16}},"end":{{"segment":1,"offset":4096}}}}"#,
            tenant.0, intent.file.0
        );
        let decoded = FlushIntent::from_payload(old.as_bytes()).unwrap();
        assert_eq!(decoded.layout, None);
        assert_eq!(decoded.file, intent.file);
    }

    /// An intent of one hour is written under the record version that a
    /// reader of the previous version refuses, so it is never read as the
    /// whole range; an intent of the whole range keeps the first version,
    /// and an intent logged before hours were recorded has none.
    #[test]
    fn an_intent_of_one_hour_takes_the_record_version_older_readers_refuse() {
        let tenant = TenantId::new();
        let whole = FlushIntent {
            tenant,
            file: fs_catalog::FileId::new(),
            first: fs_wal::Position {
                segment: 1,
                offset: 16,
            },
            end: fs_wal::Position {
                segment: 1,
                offset: 4096,
            },
            layout: Some(FileLayout::current(4096)),
            hour: None,
            signal: RowSignal::Traces,
        };
        let one_hour = FlushIntent {
            hour: Some(1_792_677_600),
            ..whole.clone()
        };
        assert_eq!(whole.to_record().version, VERSION);
        assert_eq!(one_hour.to_record().version, VERSION_HOUR_INTENT);
        let bytes = one_hour.to_record().encode();
        assert_eq!(bytes.first().copied(), Some(VERSION_HOUR_INTENT));
        let decoded = WalRecord::decode(&bytes).unwrap();
        assert_eq!(decoded.version, VERSION_HOUR_INTENT);
        let intent = FlushIntent::from_payload(&decoded.payload).unwrap();
        assert_eq!(intent, one_hour);
        assert_eq!(
            intent.hour_start().unwrap().to_rfc3339(),
            "2026-10-22T14:00:00+00:00"
        );
        assert!(
            !String::from_utf8_lossy(&whole.to_record().payload).contains("hour"),
            "the whole range is written as before"
        );
        let before_hours = format!(
            r#"{{"tenant":"{}","file":"{}","first":{{"segment":1,"offset":16}},"end":{{"segment":1,"offset":4096}}}}"#,
            tenant.0, whole.file.0
        );
        assert_eq!(
            FlushIntent::from_payload(before_hours.as_bytes())
                .unwrap()
                .hour,
            None
        );

        // Reading fails closed: a version and an hour that do not match
        // were not written by this engine's cut.
        assert!(FlushIntent::from_record(&whole.to_record()).is_ok());
        assert!(FlushIntent::from_record(&one_hour.to_record()).is_ok());
        let as_version = |intent: &FlushIntent, version: u8| WalRecord {
            version,
            ..intent.to_record()
        };
        assert!(FlushIntent::from_record(&as_version(&one_hour, VERSION)).is_err());
        assert!(FlushIntent::from_record(&as_version(&whole, VERSION_HOUR_INTENT)).is_err());
        // 2300-01-01T00:00:00Z is a date but not a nanosecond timestamp.
        for bad in [
            1_792_677_601,
            10_413_792_000,
            i64::MAX,
            -3600 * 400_000_000_000,
        ] {
            let intent = FlushIntent {
                hour: Some(bad),
                ..whole.clone()
            };
            assert!(
                FlushIntent::from_record(&intent.to_record()).is_err(),
                "{bad} is not the start of a representable hour"
            );
        }
        let rows_v2 = WalRecord {
            version: VERSION_HOUR_INTENT,
            tenant,
            signal: RecordSignal::Traces,
            payload: Bytes::from_static(b"otlp"),
        }
        .encode();
        assert!(
            WalRecord::decode(&rows_v2).is_err(),
            "the hour version is for intents"
        );
        assert!(
            FlushIntent::from_record(&WalRecord {
                version: VERSION,
                tenant,
                signal: RecordSignal::Traces,
                payload: Bytes::from_static(b"otlp"),
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(WalRecord::decode(b"short").is_err());
        let mut bad_version = WalRecord {
            version: VERSION,
            tenant: TenantId::new(),
            signal: RecordSignal::Traces,
            payload: Bytes::new(),
        }
        .encode();
        *bad_version.first_mut().unwrap() = 9;
        assert!(WalRecord::decode(&bad_version).is_err());
        let mut bad_signal = WalRecord {
            version: VERSION,
            tenant: TenantId::new(),
            signal: RecordSignal::Traces,
            payload: Bytes::new(),
        }
        .encode();
        *bad_signal.get_mut(1).unwrap() = 7;
        assert!(WalRecord::decode(&bad_signal).is_err());
    }

    fn logs_intent(hour: Option<i64>) -> FlushIntent {
        FlushIntent {
            tenant: TenantId::new(),
            file: fs_catalog::FileId::new(),
            first: fs_wal::Position {
                segment: 2,
                offset: 16,
            },
            end: fs_wal::Position {
                segment: 2,
                offset: 900,
            },
            layout: Some(FileLayout::current(64)),
            hour,
            signal: RowSignal::Logs,
        }
    }

    /// A log record and a log intent are of their own kinds, which an engine
    /// without logs refuses as unknown, and a trace intent keeps the bytes
    /// of the versions before logs.
    #[test]
    fn log_records_and_intents_round_trip_under_their_own_codes() {
        let record = WalRecord {
            version: VERSION,
            tenant: TenantId::new(),
            signal: RecordSignal::Logs,
            payload: Bytes::from_static(b"otlp logs"),
        };
        let bytes = record.encode();
        assert_eq!(bytes.get(1), Some(&3));
        assert_eq!(WalRecord::decode(&bytes).unwrap(), record);

        for hour in [None, Some(1_792_677_600)] {
            let intent = logs_intent(hour);
            let record = intent.to_record();
            assert_eq!(record.version, VERSION_SIGNAL_INTENT);
            let decoded = WalRecord::decode(&record.encode()).unwrap();
            assert_eq!(FlushIntent::from_record(&decoded).unwrap(), intent);
        }
        let bad_hour = logs_intent(Some(1_792_677_601)).to_record();
        assert!(FlushIntent::from_record(&bad_hour).is_err());

        let traces = FlushIntent {
            signal: RowSignal::Traces,
            ..logs_intent(None)
        };
        let record = traces.to_record();
        assert_eq!(record.version, VERSION);
        let payload = String::from_utf8(record.payload.to_vec()).unwrap();
        assert!(!payload.contains("signal"), "{payload}");
    }

    /// Each intent version names one signal: a log intent under a trace
    /// version, or a trace intent under the signal version, was not written
    /// by this engine's cut.
    #[test]
    fn an_intent_under_the_version_of_another_signal_is_refused() {
        let logs = logs_intent(None);
        for version in [VERSION, VERSION_HOUR_INTENT] {
            let record = WalRecord {
                version,
                ..logs.to_record()
            };
            assert!(FlushIntent::from_record(&record).is_err(), "{version}");
        }
        let traces = FlushIntent {
            signal: RowSignal::Traces,
            ..logs_intent(None)
        };
        let record = WalRecord {
            version: VERSION_SIGNAL_INTENT,
            ..traces.to_record()
        };
        assert!(FlushIntent::from_record(&record).is_err());
        let rows = WalRecord {
            version: VERSION_SIGNAL_INTENT,
            tenant: TenantId::new(),
            signal: RecordSignal::Logs,
            payload: Bytes::new(),
        }
        .encode();
        assert!(
            WalRecord::decode(&rows).is_err(),
            "the signal version is for intents"
        );
    }
}
