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
/// Record format version of an intent over metric records: one hour, and
/// the set of two files, samples and series, its rows go to. A reader
/// without metrics meets it and fails.
pub const VERSION_METRIC_INTENT: u8 = 4;
/// Record format version of a metric record whose payload carries, ahead
/// of the request, the mapping version and the limits it was converted
/// under ([`crate::metric_record`]). A reader of the previous version
/// meets it and fails; a metric record of [`VERSION`] carries the request
/// alone, its limits lost, and stops the replay.
pub const VERSION_METRIC_RECORD: u8 = 5;

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
    /// An `ExportMetricsServiceRequest`, after what it was converted under
    /// at [`VERSION_METRIC_RECORD`]. Its samples take their source order
    /// from the record's position, so a replay converts it as the live
    /// request was.
    Metrics = 4,
}

impl RecordSignal {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Traces),
            2 => Some(Self::FlushIntent),
            3 => Some(Self::Logs),
            4 => Some(Self::Metrics),
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
    /// For metric rows: the set the samples file, `file` under `layout`,
    /// and its series file belong to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<MetricIntent>,
}

/// The two files of a metric intent.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricIntent {
    /// The set both files register under.
    pub set: fs_catalog::FileSetId,
    /// Each file with its role and layout: exactly a samples and a series
    /// file, the samples one the intent's `file` and `layout`.
    pub files: Vec<MetricFile>,
}

/// One file of a metric intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricFile {
    /// Samples or series.
    pub role: fs_catalog::MetricRole,
    /// Its id.
    pub file: fs_catalog::FileId,
    /// How its bytes are laid out.
    pub layout: FileLayout,
}

impl MetricIntent {
    /// The file of `role`.
    pub fn file(&self, role: fs_catalog::MetricRole) -> Option<&MetricFile> {
        self.files.iter().find(|f| f.role == role)
    }
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
    /// Metric samples, with their series, from [`RecordSignal::Metrics`]
    /// records.
    Metrics,
}

impl RowSignal {
    /// The kind of record whose rows the intent covers.
    pub fn record(self) -> RecordSignal {
        match self {
            Self::Traces => RecordSignal::Traces,
            Self::Logs => RecordSignal::Logs,
            Self::Metrics => RecordSignal::Metrics,
        }
    }

    /// True for spans; the form serde's `skip_serializing_if` calls.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(crate) fn is_traces(&self) -> bool {
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
    /// Sizing of the text sidecar of a log file; `None` writes none, as
    /// for spans and for log intents logged before text sidecars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_index: Option<TextIndexLayout>,
}

/// The text sidecar's format and budgets, which decide its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TextIndexLayout {
    /// Sidecar format version.
    pub version: u8,
    /// [`fs_index::TextIndexLimits::max_bytes_per_row_group`].
    pub max_bytes_per_row_group: usize,
    /// [`fs_index::TextIndexLimits::max_bytes_per_file`].
    pub max_bytes_per_file: usize,
}

impl TextIndexLayout {
    /// The sidecar format this engine writes.
    pub const CURRENT_VERSION: u8 = 1;
}

impl FileLayout {
    /// Version of the current encoder: per-batch sort, k-way merge, one row
    /// group at a time, bloom filters sized to the row group on `trace_id`,
    /// `span_id` and `service_name` alone. Version 3 is the same merge with
    /// a bloom filter on every column; it is still written for its intents.
    /// Version 2 had the writer's limit at twice the group and bloom filters
    /// sized by it; it never shipped and is refused like any other.
    pub const CURRENT_VERSION: u32 = 4;

    /// Version of the current encoder of metric files: version 4 with
    /// series of schema version 2, which keeps each label's origin;
    /// version 4 still writes series of version 1 for its intents.
    pub const METRIC_VERSION: u32 = 5;

    /// Versions this engine writes for rows of `signal`: the current one
    /// and those whose intents may still be in a log. Log and metric
    /// files exist only from version 4 on.
    pub fn written_versions(signal: RowSignal) -> &'static [u32] {
        match signal {
            RowSignal::Traces => &[3, 4],
            RowSignal::Logs => &[4],
            RowSignal::Metrics => &[4, 5],
        }
    }

    /// The current encoder with `row_group_rows` rows per row group and
    /// no text sidecar.
    pub fn current(row_group_rows: usize) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            row_group_rows,
            text_index: None,
        }
    }

    /// The current encoder for rows of `signal`: log files get a text
    /// sidecar within `text`.
    pub fn for_signal(
        signal: RowSignal,
        row_group_rows: usize,
        text: fs_index::TextIndexLimits,
    ) -> Self {
        let text_index = (signal == RowSignal::Logs).then_some(TextIndexLayout {
            version: TextIndexLayout::CURRENT_VERSION,
            max_bytes_per_row_group: text.max_bytes_per_row_group,
            max_bytes_per_file: text.max_bytes_per_file,
        });
        let version = if signal == RowSignal::Metrics {
            Self::METRIC_VERSION
        } else {
            Self::CURRENT_VERSION
        };
        Self {
            version,
            text_index,
            ..Self::current(row_group_rows)
        }
    }

    /// The budgets of the text sidecar, `None` when the layout writes none.
    ///
    /// # Errors
    /// A sidecar version this engine does not write, or budgets outside
    /// their ceilings.
    pub fn text_limits(&self) -> Result<Option<fs_index::TextIndexLimits>, IngestError> {
        let Some(text) = self.text_index else {
            return Ok(None);
        };
        if text.version != TextIndexLayout::CURRENT_VERSION {
            return Err(IngestError::Encode(format!(
                "text index version {} is not one this engine writes",
                text.version
            )));
        }
        fs_index::TextIndexLimits::new(text.max_bytes_per_row_group, text.max_bytes_per_file)
            .map(Some)
            .map_err(IngestError::from)
    }
}

impl FlushIntent {
    /// Serialises the intent as a WAL record payload, under the record
    /// version its signal and its hour need.
    pub fn to_record(&self) -> WalRecord {
        WalRecord {
            version: match (self.signal, self.hour) {
                (RowSignal::Metrics, _) => VERSION_METRIC_INTENT,
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
    /// version traces and an hour, the signal version logs, with or without
    /// an hour, the metric version metrics, an hour and its two files, and
    /// any hour is the start of a representable one. Anything else was not
    /// written by this engine's cut.
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
            | (VERSION_SIGNAL_INTENT, RowSignal::Logs, _)
            | (VERSION_METRIC_INTENT, RowSignal::Metrics, Some(_)) => {
                intent.metric.is_some() == (intent.signal == RowSignal::Metrics)
            }
            (
                VERSION | VERSION_HOUR_INTENT | VERSION_SIGNAL_INTENT | VERSION_METRIC_INTENT,
                _,
                _,
            ) => false,
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
        if let Some(metric) = &intent.metric {
            intent.check_metric(metric)?;
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

    /// Exactly a samples and a series file under distinct ids, the samples
    /// one this intent's file and layout, both layouts written for metrics.
    fn check_metric(&self, metric: &MetricIntent) -> Result<(), IngestError> {
        use fs_catalog::MetricRole;
        let refuse = |why: &str| Err(IngestError::Record(format!("metric intent: {why}")));
        let roles: Vec<MetricRole> = metric.files.iter().map(|f| f.role).collect();
        if roles != MetricRole::BOTH {
            return refuse("not a samples then a series file");
        }
        let (Some(samples), Some(series)) = (
            metric.file(MetricRole::Samples),
            metric.file(MetricRole::Series),
        ) else {
            return refuse("a role missing");
        };
        if samples.file != self.file || Some(samples.layout) != self.layout {
            return refuse("the samples file is not the intent's");
        }
        if series.file == samples.file {
            return refuse("one id for both files");
        }
        for file in &metric.files {
            if !FileLayout::written_versions(RowSignal::Metrics).contains(&file.layout.version)
                || file.layout.text_index.is_some()
            {
                return refuse("a layout this engine does not write");
            }
        }
        Ok(())
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
        if ![
            VERSION,
            VERSION_HOUR_INTENT,
            VERSION_SIGNAL_INTENT,
            VERSION_METRIC_INTENT,
            VERSION_METRIC_RECORD,
        ]
        .contains(&version)
        {
            return Err(IngestError::Record(format!(
                "unsupported version {version}"
            )));
        }
        let signal = RecordSignal::from_byte(header.get(1).copied().unwrap_or(0))
            .ok_or_else(|| IngestError::Record("unknown signal".into()))?;
        if version == VERSION_METRIC_RECORD && signal != RecordSignal::Metrics {
            return Err(IngestError::Record(format!(
                "version {version} is for metric records only"
            )));
        }
        if ![VERSION, VERSION_METRIC_RECORD].contains(&version)
            && signal != RecordSignal::FlushIntent
        {
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
            metric: None,
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

    #[test]
    fn a_log_intent_records_its_text_index_sizing() {
        let limits = fs_index::TextIndexLimits::new(4096, 65536).unwrap();
        let layout = FileLayout::for_signal(RowSignal::Logs, 64, limits);
        assert_eq!(
            layout.text_index,
            Some(TextIndexLayout {
                version: TextIndexLayout::CURRENT_VERSION,
                max_bytes_per_row_group: 4096,
                max_bytes_per_file: 65536,
            })
        );
        assert_eq!(
            FileLayout::for_signal(RowSignal::Traces, 64, limits),
            FileLayout::current(64)
        );
        let intent = FlushIntent {
            tenant: TenantId::new(),
            file: fs_catalog::FileId::new(),
            first: fs_wal::Position {
                segment: 1,
                offset: 16,
            },
            end: fs_wal::Position {
                segment: 1,
                offset: 64,
            },
            layout: Some(layout),
            hour: None,
            signal: RowSignal::Logs,
            metric: None,
        };
        let decoded =
            FlushIntent::from_record(&WalRecord::decode(&intent.to_record().encode()).unwrap())
                .unwrap();
        assert_eq!(decoded, intent);
        assert_eq!(decoded.layout.unwrap().text_limits().unwrap(), Some(limits));

        // A span layout, and a log layout logged before text sidecars, say
        // nothing of them; the latter writes no sidecar.
        let span = serde_json::to_string(&FileLayout::current(64)).unwrap();
        assert!(!span.contains("text_index"), "{span}");
        let before: FileLayout =
            serde_json::from_str(r#"{"version":4,"row_group_rows":64}"#).unwrap();
        assert_eq!(before.text_index, None);
        assert_eq!(before.text_limits().unwrap(), None);
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
            metric: None,
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
            metric: None,
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

    fn metric_intent() -> FlushIntent {
        use fs_catalog::{FileId, FileSetId, MetricRole};
        let samples = FileId::new();
        let layout = FileLayout::current(64);
        FlushIntent {
            tenant: TenantId::new(),
            file: samples,
            first: fs_wal::Position {
                segment: 3,
                offset: 16,
            },
            end: fs_wal::Position {
                segment: 3,
                offset: 800,
            },
            layout: Some(layout),
            hour: Some(1_792_677_600),
            signal: RowSignal::Metrics,
            metric: Some(MetricIntent {
                set: FileSetId::new(),
                files: vec![
                    MetricFile {
                        role: MetricRole::Samples,
                        file: samples,
                        layout,
                    },
                    MetricFile {
                        role: MetricRole::Series,
                        file: FileId::new(),
                        layout,
                    },
                ],
            }),
        }
    }

    /// A metric record and its intent have kinds of their own, which an
    /// engine without metrics refuses as unknown; the intent names one
    /// hour and exactly a samples and a series file.
    #[test]
    fn metric_records_and_intents_round_trip_under_their_own_codes() {
        let record = WalRecord {
            version: VERSION,
            tenant: TenantId::new(),
            signal: RecordSignal::Metrics,
            payload: Bytes::from_static(b"otlp metrics"),
        };
        let bytes = record.encode();
        assert_eq!(bytes.get(1), Some(&4));
        assert_eq!(WalRecord::decode(&bytes).unwrap(), record);
        let carrying = WalRecord {
            version: VERSION_METRIC_RECORD,
            ..record.clone()
        };
        assert_eq!(WalRecord::decode(&carrying.encode()).unwrap(), carrying);
        for signal in [
            RecordSignal::Traces,
            RecordSignal::Logs,
            RecordSignal::FlushIntent,
        ] {
            let other = WalRecord {
                signal,
                ..carrying.clone()
            };
            assert!(WalRecord::decode(&other.encode()).is_err(), "{signal:?}");
        }

        let intent = metric_intent();
        let record = intent.to_record();
        assert_eq!(record.version, VERSION_METRIC_INTENT);
        let decoded = WalRecord::decode(&record.encode()).unwrap();
        assert_eq!(FlushIntent::from_record(&decoded).unwrap(), intent);
    }

    #[test]
    fn a_metric_intent_is_refused_unless_it_names_a_samples_and_a_series_file_of_one_hour() {
        use fs_catalog::{FileId, MetricRole};
        let refused = |intent: &FlushIntent| FlushIntent::from_record(&intent.to_record()).is_err();
        let good = metric_intent();
        assert!(!refused(&good));
        let mut no_hour = good.clone();
        no_hour.hour = None;
        assert!(refused(&no_hour));
        let mut no_files = good.clone();
        no_files.metric = None;
        assert!(refused(&no_files));
        let edit = |change: &dyn Fn(&mut MetricIntent)| {
            let mut intent = good.clone();
            change(intent.metric.as_mut().unwrap());
            intent
        };
        assert!(refused(&edit(&|m| m.files.reverse())), "series first");
        assert!(
            refused(&edit(&|m| {
                m.files.pop();
            })),
            "one file"
        );
        assert!(
            refused(&edit(&|m| {
                let extra = *m.files.get(1).unwrap();
                m.files.push(extra);
            })),
            "a role twice"
        );
        assert!(
            refused(&edit(
                &|m| m.files.get_mut(1).unwrap().role = MetricRole::Samples
            )),
            "two samples"
        );
        assert!(
            refused(&edit(&|m| m.files.get_mut(0).unwrap().file = FileId::new())),
            "samples not the intent's file"
        );
        assert!(
            refused(&edit(
                &|m| m.files.get_mut(1).unwrap().file = m.files.first().unwrap().file
            )),
            "one id twice"
        );
        assert!(
            refused(&edit(&|m| m.files.get_mut(1).unwrap().layout.version = 3)),
            "an unwritten layout"
        );
        assert!(
            refused(&edit(&|m| {
                m.files.get_mut(1).unwrap().layout.text_index = Some(TextIndexLayout {
                    version: 1,
                    max_bytes_per_row_group: 1,
                    max_bytes_per_file: 1,
                });
            })),
            "a text sidecar"
        );
        // Unknown fields in the files are not tolerated.
        let mut record = good.to_record();
        let text = String::from_utf8(record.payload.to_vec()).unwrap();
        let tampered = text.replacen("\"role\":\"series\"", "\"role\":\"series\",\"extra\":1", 1);
        assert_ne!(tampered, text);
        record.payload = Bytes::from(tampered);
        assert!(FlushIntent::from_record(&record).is_err());
        // Only the metric version names metrics, and it names nothing else.
        for version in [VERSION, VERSION_HOUR_INTENT, VERSION_SIGNAL_INTENT] {
            let record = WalRecord {
                version,
                ..good.to_record()
            };
            assert!(FlushIntent::from_record(&record).is_err(), "{version}");
        }
        let logs = WalRecord {
            version: VERSION_METRIC_INTENT,
            ..logs_intent(Some(1_792_677_600)).to_record()
        };
        assert!(FlushIntent::from_record(&logs).is_err());
        let mut logs_with_files = logs_intent(Some(1_792_677_600));
        logs_with_files.metric = good.metric.clone();
        assert!(FlushIntent::from_record(&logs_with_files.to_record()).is_err());
    }
}
