//! The contract between the querier and an index over the files of a
//! scan: per file, read it whole, skip it, or read some of its row groups.
//! An index only ever narrows a scan; whatever it answers wrongly or not at
//! all is read whole.

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use fs_catalog::{FileId, FileRecord};
use fs_index::text_index::Probe;

/// What to read of one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// Every row group.
    All,
    /// Nothing: no row group may hold a matching row.
    Skip,
    /// These row groups, sorted and unique, a strict subset of the file's.
    RowGroups(Box<[usize]>),
}

/// An index's answer for one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSelection {
    /// The file answered for.
    pub file: FileId,
    /// What to read of it.
    pub selection: Selection,
    /// Row groups of the file, when the index checked them against the
    /// footer; [`Selection::RowGroups`] needs it.
    pub row_groups: Option<usize>,
}

/// An index that narrows the files of a scan for a body probe.
#[async_trait]
pub trait FileIndex: Send + Sync + Debug {
    /// What to read of each of `files` for a row `probe` keeps.
    async fn select(&self, files: &[FileRecord], probe: &Probe) -> Vec<FileSelection>;
}

/// The selection of each of `files`, in their order, from an index's
/// answers: a file answered not at all, twice or with row groups out of
/// order, out of its geometry or naming all or none of them is read
/// whole, its geometry kept only when the answer was sound.
pub fn resolve(files: &[FileRecord], answers: Vec<FileSelection>) -> Vec<FileSelection> {
    let mut by_file: HashMap<FileId, Option<FileSelection>> = HashMap::with_capacity(files.len());
    for answer in answers {
        by_file
            .entry(answer.file)
            .and_modify(|seen| *seen = None)
            .or_insert(Some(answer));
    }
    files
        .iter()
        .map(|file| {
            by_file
                .get_mut(&file.id)
                .and_then(Option::take)
                .filter(sound)
                .unwrap_or(FileSelection {
                    file: file.id,
                    selection: Selection::All,
                    row_groups: None,
                })
        })
        .collect()
}

fn sound(answer: &FileSelection) -> bool {
    match &answer.selection {
        Selection::All | Selection::Skip => true,
        Selection::RowGroups(groups) => answer.row_groups.is_some_and(|count| {
            !groups.is_empty()
                && groups.len() < count
                && groups.windows(2).all(|w| w.first() < w.last())
                && groups.last().is_some_and(|last| *last < count)
        }),
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use chrono::Utc;
    use fs_catalog::{FileState, Level, Signal, TenantId, TimeRange};

    use super::*;

    fn record(n: u128) -> FileRecord {
        let now = Utc::now();
        FileRecord {
            id: FileId(uuid::Uuid::from_u128(n)),
            origin: FileId(uuid::Uuid::from_u128(n)),
            tenant_id: TenantId(uuid::Uuid::nil()),
            signal: Signal::Logs,
            level: Level::L0,
            path: format!("t/x/logs/l0/{n}.parquet"),
            size_bytes: 1,
            row_count: 1,
            range: TimeRange {
                start: now,
                end: now,
            },
            services: Vec::new(),
            summary: None,
            state: FileState::Visible,
            created_at: now,
            content_sha256: None,
            quarantined_at: None,
        }
    }

    fn answer(n: u128, selection: Selection, row_groups: Option<usize>) -> FileSelection {
        FileSelection {
            file: FileId(uuid::Uuid::from_u128(n)),
            selection,
            row_groups,
        }
    }

    fn groups(g: &[usize]) -> Selection {
        Selection::RowGroups(g.into())
    }

    #[test]
    fn unsound_or_missing_answers_read_the_file_whole() {
        let files: Vec<FileRecord> = (1..=9).map(record).collect();
        let answers = vec![
            answer(1, Selection::Skip, None),
            answer(2, groups(&[0, 2]), Some(4)),
            answer(3, groups(&[2, 0]), Some(4)),
            answer(4, groups(&[1, 1]), Some(4)),
            answer(5, groups(&[0, 4]), Some(4)),
            answer(6, groups(&[0, 1]), None),
            answer(7, groups(&[]), Some(4)),
            answer(8, groups(&[0, 1]), Some(2)),
            answer(9, Selection::Skip, None),
            answer(9, Selection::Skip, None),
            answer(42, Selection::Skip, None),
        ];
        let got: Vec<Selection> = resolve(&files, answers)
            .into_iter()
            .map(|a| a.selection)
            .collect();
        let mut want = vec![Selection::Skip, groups(&[0, 2])];
        want.extend(std::iter::repeat_n(Selection::All, 7));
        assert_eq!(got, want);
        assert_eq!(
            resolve(&files[..1], Vec::new()),
            [answer(1, Selection::All, None)],
            "an index that answers nothing reads everything"
        );
    }
}
