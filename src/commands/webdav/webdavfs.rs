//! TODO: add blocking() to operation which may block!!

use std::fmt::{Debug, Formatter};
use std::io::SeekFrom;
#[cfg(not(windows))]
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use bytes::{Buf, Bytes};
use futures::FutureExt;
use rustic_core::repofile::{Node, SnapshotFile};
use rustic_core::{IndexedFull, OpenFile, Repository};
use tokio::task;

use webdav_handler::davpath::DavPath;
use webdav_handler::fs::*;

use crate::commands::mount::fs::{FsTree, IdenticalSnapshot, Latest};

const RUNTIME_TYPE_BASIC: u32 = 1;
const RUNTIME_TYPE_THREADPOOL: u32 = 2;
static RUNTIME_TYPE: AtomicU32 = AtomicU32::new(0);

fn now() -> SystemTime {
    static NOW: OnceLock<SystemTime> = OnceLock::new();
    NOW.get_or_init(|| SystemTime::now()).clone()
}

#[derive(Clone, Copy)]
#[repr(u32)]
enum RuntimeType {
    Basic = RUNTIME_TYPE_BASIC,
    ThreadPool = RUNTIME_TYPE_THREADPOOL,
}

impl RuntimeType {
    #[inline]
    fn get() -> RuntimeType {
        match RUNTIME_TYPE.load(Ordering::Relaxed) {
            RUNTIME_TYPE_BASIC => RuntimeType::Basic,
            RUNTIME_TYPE_THREADPOOL => RuntimeType::ThreadPool,
            _ => {
                let dbg = format!("{:?}", tokio::runtime::Handle::current());
                let rt = if dbg.contains("ThreadPool") {
                    RuntimeType::ThreadPool
                } else {
                    RuntimeType::Basic
                };
                RUNTIME_TYPE.store(rt as u32, Ordering::SeqCst);
                rt
            }
        }
    }
}

// Run some code via block_in_place() or spawn_blocking().
#[inline]
async fn blocking<F, R>(func: F) -> R
where
    F: FnOnce() -> R,
    F: Send + 'static,
    R: Send + 'static,
{
    match RuntimeType::get() {
        RuntimeType::Basic => task::spawn_blocking(func).await.unwrap(),
        RuntimeType::ThreadPool => task::block_in_place(func),
    }
}

/// DAV Filesystem implementation.
pub struct RusticWebDavFS<P, S> {
    inner: Arc<DavFsInner<P, S>>,
}

// inner struct.
struct DavFsInner<P, S> {
    repo: Repository<P, S>,
    root: FsTree,
}
impl<P, S> Debug for DavFsInner<P, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DavFS")
    }
}

struct DavFsFile<P, S> {
    node: Node,
    open: OpenFile,
    fs: Arc<DavFsInner<P, S>>,
    seek: usize,
}
impl<P, S> Debug for DavFsFile<P, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DavFile")
    }
}

struct DavFsDirEntry(Node);
#[derive(Clone, Debug)]
struct DavFsMetaData(Node);

impl<P, S: IndexedFull> RusticWebDavFS<P, S> {
    pub(crate) fn from_node(repo: Repository<P, S>, node: Node) -> Box<Self> {
        let root = FsTree::RusticTree(node.subtree.unwrap());
        Self::new(repo, root)
    }

    pub(crate) fn new(repo: Repository<P, S>, root: FsTree) -> Box<Self> {
        let inner = DavFsInner { repo, root };
        Box::new({
            RusticWebDavFS {
                inner: Arc::new(inner),
            }
        })
    }

    pub(crate) fn from_snapshots(
        repo: Repository<P, S>,
        snapshots: Vec<SnapshotFile>,
        path_template: String,
        time_template: String,
        symlinks: bool,
    ) -> anyhow::Result<Box<Self>> {
        let (latest, identical) = if symlinks {
            (Latest::AsLink, IdenticalSnapshot::AsLink)
        } else {
            (Latest::AsDir, IdenticalSnapshot::AsDir)
        };
        let root =
            FsTree::from_snapshots(snapshots, path_template, time_template, latest, identical)?;
        Ok(Self::new(repo, root))
    }

    fn node_from_path(&self, path: &DavPath) -> Result<Node, FsError> {
        self.inner
            .root
            .node_from_path(&self.inner.repo, &path.as_pathbuf())
            .map_err(|_| FsError::GeneralFailure)
    }

    fn dir_entries_from_path(&self, path: &DavPath) -> Result<Vec<Node>, FsError> {
        self.inner
            .root
            .dir_entries_from_path(&self.inner.repo, &path.as_pathbuf())
            .map_err(|_| FsError::GeneralFailure)
    }
}

impl<P, S: IndexedFull> Clone for RusticWebDavFS<P, S> {
    fn clone(&self) -> Self {
        RusticWebDavFS {
            inner: self.inner.clone(),
        }
    }
}
// This implementation is basically a bunch of boilerplate to
// wrap the std::fs call in self.blocking() calls.
impl<P: Debug + Send + Sync + 'static, S: IndexedFull + Debug + Send + Sync + 'static> DavFileSystem
    for RusticWebDavFS<P, S>
{
    fn metadata<'a>(&'a self, davpath: &'a DavPath) -> FsFuture<'_, Box<dyn DavMetaData>> {
        self.symlink_metadata(davpath)
    }

    fn symlink_metadata<'a>(&'a self, davpath: &'a DavPath) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async move {
            let node = self.node_from_path(davpath)?;
            Ok(Box::new(DavFsMetaData(node)) as Box<dyn DavMetaData>)
        }
        .boxed()
    }

    // read_dir is a bit more involved - but not much - than a simple wrapper,
    // because it returns a stream.
    fn read_dir<'a>(
        &'a self,
        davpath: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'_, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            let entries = self.dir_entries_from_path(davpath)?;
            let v: Vec<_> = entries
                .into_iter()
                .map(|e| Box::new(DavFsDirEntry(e)) as Box<dyn DavDirEntry>)
                .collect();
            let strm = futures::stream::iter(v.into_iter());
            Ok(Box::pin(strm) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }

    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'_, Box<dyn DavFile>> {
        async move {
            if options.write
                || options.append
                || options.truncate
                || options.create
                || options.create_new
            {
                return Err(FsError::Forbidden);
            }

            let node = self.node_from_path(path)?;
            let open = self
                .inner
                .repo
                .open_file(&node)
                .map_err(|_| FsError::GeneralFailure)?;
            Ok(Box::new(DavFsFile {
                node,
                open,
                fs: self.inner.clone(),
                seek: 0,
            }) as Box<dyn DavFile>)
        }
        .boxed()
    }
}

impl DavDirEntry for DavFsDirEntry {
    fn metadata<'a>(&'a self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async move { Ok(Box::new(DavFsMetaData(self.0.clone())) as Box<dyn DavMetaData>) }.boxed()
    }

    #[cfg(not(windows))]
    fn name(&self) -> Vec<u8> {
        self.0.name().as_bytes().to_vec()
    }

    #[cfg(windows)]
    fn name(&self) -> Vec<u8> {
        self.0
            .name()
            .as_os_str()
            .to_string_lossy()
            .to_string()
            .into_bytes()
    }
}

impl<P: Debug + Send + Sync, S: IndexedFull + Debug + Send + Sync> DavFile for DavFsFile<P, S> {
    fn metadata<'a>(&'a mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async move { Ok(Box::new(DavFsMetaData(self.node.clone())) as Box<dyn DavMetaData>) }
            .boxed()
    }

    fn write_bytes<'a>(&'a mut self, _buf: Bytes) -> FsFuture<'_, ()> {
        async move { Err(FsError::Forbidden) }.boxed()
    }

    fn write_buf<'a>(&'a mut self, _buf: Box<dyn Buf + Send>) -> FsFuture<'_, ()> {
        async move { Err(FsError::Forbidden) }.boxed()
    }

    fn read_bytes<'a>(&'a mut self, count: usize) -> FsFuture<'_, Bytes> {
        async move {
            let data = self
                .fs
                .repo
                .read_file_at(&self.open, self.seek, count)
                .map_err(|_| FsError::GeneralFailure)?;
            Ok(data)
        }
        .boxed()
    }

    fn seek<'a>(&'a mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        async move {
            match pos {
                SeekFrom::Start(start) => self.seek = start as usize,
                SeekFrom::Current(delta) => self.seek = (self.seek as i64 + delta) as usize,
                SeekFrom::End(end) => self.seek = (self.node.meta.size as i64 + end) as usize,
            }

            Ok(self.seek as u64)
        }
        .boxed()
    }

    fn flush<'a>(&'a mut self) -> FsFuture<'_, ()> {
        async move { Ok(()) }.boxed()
    }
}

impl DavMetaData for DavFsMetaData {
    fn len(&self) -> u64 {
        self.0.meta.size
    }
    fn created(&self) -> FsResult<SystemTime> {
        Ok(now())
    }
    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.0.meta.mtime.map(SystemTime::from).unwrap_or(now()))
    }
    fn accessed(&self) -> FsResult<SystemTime> {
        Ok(self.0.meta.atime.map(SystemTime::from).unwrap_or(now()))
    }

    fn status_changed(&self) -> FsResult<SystemTime> {
        Ok(self.0.meta.ctime.map(SystemTime::from).unwrap_or(now()))
    }

    fn is_dir(&self) -> bool {
        self.0.is_dir()
    }
    fn is_file(&self) -> bool {
        self.0.is_file()
    }
    fn is_symlink(&self) -> bool {
        self.0.is_symlink()
    }
    fn executable(&self) -> FsResult<bool> {
        if self.0.is_file() {
            let Some(mode) = self.0.meta.mode else {
                return Ok(false);
            };
            return Ok((mode & 0o100) > 0);
        }
        Err(FsError::NotImplemented)
    }
}
