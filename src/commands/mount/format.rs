use std::fmt;

use runtime_format::{FormatKey, FormatKeyError};
use rustic_core::repofile::SnapshotFile;

pub struct FormattedSnapshot<'a>(pub &'a SnapshotFile, pub &'a str);

impl<'a> FormatKey for FormattedSnapshot<'a> {
    fn fmt(&self, key: &str, f: &mut fmt::Formatter<'_>) -> Result<(), FormatKeyError> {
        match key {
            "id" => write!(f, "{}", self.0.id),
            "long_id" => write!(f, "{:?}", self.0.id),
            "time" => write!(f, "{}", self.0.time.format(self.1)),
            "username" => write!(f, "{}", self.0.username),
            "hostname" => write!(f, "{}", self.0.hostname),
            "label" => write!(f, "{}", self.0.label),
            "tags" => write!(f, "{}", self.0.tags),
            "backup_start" => {
                if let Some(summary) = &self.0.summary {
                    write!(f, "{}", summary.backup_start.format(self.1))
                } else {
                    write!(f, "no_backup_start")
                }
            }
            "backup_end" => {
                if let Some(summary) = &self.0.summary {
                    write!(f, "{}", summary.backup_end.format(self.1))
                } else {
                    write!(f, "no_backup_end")
                }
            }

            _ => return Err(FormatKeyError::UnknownKey),
        }
        .map_err(FormatKeyError::Fmt)
    }
}
