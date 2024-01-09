//! `mount` subcommand
mod format;
pub mod fs;
mod mountfs;

use mountfs::RusticFS;

use std::{ffi::OsStr, path::PathBuf};

use crate::{commands::open_repository, status_err, Application, RUSTIC_APP};

use abscissa_core::{Command, Runnable, Shutdown};
use anyhow::Result;
use fuse_mt::{mount, FuseMT};

#[derive(clap::Parser, Command, Debug)]
pub(crate) struct MountCmd {
    /// The path template to use for snapshots. {id}, {id_long}, {time}, {username}, {hostname}, {label}, {tags}, {backup_start}, {backup_end} are replaced. [default: "[{hostname}]/[{label}]/{time}"]
    #[clap(long)]
    path_template: Option<String>,

    /// The time template to use to display times in the path template. See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for format options. [default: "%Y-%m-%d_%H-%M-%S"]
    #[clap(long)]
    time_template: Option<String>,

    #[clap(value_name = "PATH")]
    /// The mount point to use
    mountpoint: PathBuf,

    /// Specify directly which path to mount
    #[clap(value_name = "SNAPSHOT[:PATH]")]
    snap: Option<String>,
}

impl Runnable for MountCmd {
    fn run(&self) {
        if let Err(err) = self.inner_run() {
            status_err!("{}", err);
            RUSTIC_APP.shutdown(Shutdown::Crash);
        };
    }
}

impl MountCmd {
    fn inner_run(&self) -> Result<()> {
        let config = RUSTIC_APP.config();

        let repo = open_repository(&config)?.to_indexed()?;

        let path_template = self
            .path_template
            .clone()
            .unwrap_or("[{hostname}]/[{label}]/{time}".to_string());
        let time_template = self
            .time_template
            .clone()
            .unwrap_or("%Y-%m-%d_%H-%M-%S".to_string());

        let sn_filter = |sn: &_| config.snapshot_filter.matches(sn);
        let target_fs = if let Some(snap) = &self.snap {
            let node = repo.node_from_snapshot_path(snap, sn_filter)?;
            RusticFS::from_node(repo, node)?
        } else {
            let snapshots = repo.get_matching_snapshots(sn_filter)?;
            RusticFS::from_snapshots(repo, snapshots, path_template, time_template)?
        };

        let options = [OsStr::new("-o"), OsStr::new("fsname=rusticfs")];

        let fs = FuseMT::new(target_fs, 1);
        mount(fs, &self.mountpoint, &options)?;

        Ok(())
    }
}
