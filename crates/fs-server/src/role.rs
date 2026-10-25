//! Process roles. One binary, selected at start with `--role` or in the config file.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

/// A role the process can run. `All` runs every role in one process for
/// development and small installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Receives OTLP, writes the WAL, flushes Parquet, serves the hot window.
    Ingester,
    /// Plans and executes queries over the bucket and the hot window.
    Querier,
    /// Merges files, maintains derived tables and indexes, applies retention.
    Compactor,
    /// Every role in one process.
    All,
}

impl Role {
    /// Roles that this selection expands to.
    pub fn expand(self) -> &'static [Role] {
        match self {
            Role::Ingester => &[Role::Ingester],
            Role::Querier => &[Role::Querier],
            Role::Compactor => &[Role::Compactor],
            Role::All => &[Role::Ingester, Role::Querier, Role::Compactor],
        }
    }

    /// Lowercase name.
    pub fn name(self) -> &'static str {
        match self {
            Role::Ingester => "ingester",
            Role::Querier => "querier",
            Role::Compactor => "compactor",
            Role::All => "all",
        }
    }
}

/// Error for an unknown role name.
#[derive(Debug, PartialEq, Eq)]
pub struct UnknownRole(pub String);

impl fmt::Display for UnknownRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown role {:?}, expected ingester, querier, compactor or all",
            self.0
        )
    }
}

impl std::error::Error for UnknownRole {}

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ingester" => Ok(Role::Ingester),
            "querier" => Ok(Role::Querier),
            "compactor" => Ok(Role::Compactor),
            "all" => Ok(Role::All),
            other => Err(UnknownRole(other.to_owned())),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_role() {
        for (text, role) in [
            ("ingester", Role::Ingester),
            ("querier", Role::Querier),
            ("compactor", Role::Compactor),
            ("all", Role::All),
        ] {
            assert_eq!(text.parse::<Role>(), Ok(role));
            assert_eq!(role.to_string(), text);
        }
    }

    #[test]
    fn rejects_unknown_role() {
        assert_eq!(
            "coordinator".parse::<Role>(),
            Err(UnknownRole("coordinator".into()))
        );
    }

    #[test]
    fn all_expands_to_three_roles() {
        assert_eq!(
            Role::All.expand(),
            &[Role::Ingester, Role::Querier, Role::Compactor]
        );
        assert_eq!(Role::Querier.expand(), &[Role::Querier]);
    }
}
