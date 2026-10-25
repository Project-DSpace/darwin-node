//! The node's log files, when it logs to a folder (`--log-dir`).
//!
//! The files are segments named `ant-node.<UTC time it started>.log`, for
//! example `ant-node.2026-10-25T14-03-07.log`, so their names sort in the
//! order they were written, after the daily files of releases up to ds.15
//! (`ant-node.2026-10-25.log`). A new segment starts when the current one
//! would pass [`SEGMENT_MAX_BYTES`], or in a new hour (UTC), whichever comes
//! first. Each time one starts, the oldest closed files are deleted until the
//! folder holds at most [`FOLDER_MAX_BYTES`] with the new segment counted at
//! the most it can grow to, so the folder never holds more, the current file
//! included. `--log-max-files`, when given, limits how many files are kept
//! too.
//!
//! The files give way at the full line: the free space the node's chunk store
//! keeps unused (`disk_reserve_mb`, 1 GiB unless configured). The writer reads
//! the free space from the chunk store's capacity guard, which queries the
//! filesystem at most once every few seconds, and counts its own writes and
//! deletions since that query. Once at or below the line, it writes only
//! warnings and errors and drops every other line. A warning or error with no
//! room deletes the writer's own oldest closed files to make room for itself;
//! a dropped line deletes nothing. Once nothing is left to delete, it adds no
//! bytes at all. It stays so until the free space is above the line by more
//! than what it freed there (and at least a file's size), so the room it freed
//! itself never takes it back to writing everything. It counts what it drops,
//! and writes one line saying so when the disk has room again. Every node on
//! a disk reads the same partition, so this holds however many nodes share
//! it.

use crate::storage::{SpaceReading, MIB};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{Level, Metadata};
use tracing_subscriber::fmt::MakeWriter;

/// The most a log file holds before a new one starts.
pub const SEGMENT_MAX_BYTES: u64 = 50 * MIB;

/// The most a node's log folder holds, the current file included.
pub const FOLDER_MAX_BYTES: u64 = 500 * MIB;

/// How long a log file is written to at most: a new one starts in each hour.
pub const SEGMENT_MAX_AGE: Duration = Duration::from_secs(3600);

/// The start of every log file's name.
pub const FILE_PREFIX: &str = "ant-node.";

/// The end of every log file's name.
pub const FILE_SUFFIX: &str = ".log";

/// The first byte of a line [`TagLevels`] passes on for a warning or an error,
/// which the writer keeps first at the full line.
const TAG_KEEP_FIRST: u8 = 0x01;

/// The first byte of every other line [`TagLevels`] passes on.
const TAG_DROP_FIRST: u8 = 0x02;

/// The targets of the INFO summary lines kept first at the full line.
///
/// A canary and an operator read them: the browser refusal counts and the
/// hourly commitment tick. They are kept first as warnings are; a few lines
/// an hour, so the room they take is negligible. They stay at INFO, since
/// they report normal running.
pub const KEEP_FIRST_TARGETS: &[&str] = &[
    "ant_node::web_rtc::refusals",
    "ant_node::replication::commitment_tick",
];

/// How the node's log lines are written, so the writer's own line matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineFormat {
    /// Plain text.
    Text,
    /// One JSON object a line, with the build's version and commit.
    Json,
}

/// Where the writer reads the free space on the disk from.
pub trait FreeSpace: Send {
    /// The free space and the full line now, or `None` if unknown.
    fn reading(&mut self) -> Option<SpaceReading>;
}

/// The free space as the node's chunk store's capacity guard reads it.
struct ChunkStoreSpace;

impl FreeSpace for ChunkStoreSpace {
    fn reading(&mut self) -> Option<SpaceReading> {
        crate::storage::full_line_reading(crate::storage::FULL_LINE_READING_MAX_AGE)
    }
}

/// The file being written.
struct Segment {
    file: File,
    name: String,
    len: u64,
    /// The hour (seconds since the epoch ÷ 3600) the file was started in.
    hour: u64,
}

/// The limits on a log folder: the shipped ones, or a test's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Limits {
    segment_max: u64,
    folder_max: u64,
}

impl Limits {
    const SHIPPED: Self = Self {
        segment_max: SEGMENT_MAX_BYTES,
        folder_max: FOLDER_MAX_BYTES,
    };
}

/// How long the writer waits before trying again to open a file it couldn't.
const REOPEN_AFTER: Duration = Duration::from_secs(1);

/// What the writer dropped since it last said so.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Dropped {
    lines: u64,
    bytes: u64,
    /// Of those, warnings and errors.
    kept_first: u64,
}

/// The node's log files (see the module's description).
///
/// A [`Write`] that takes one log line a call, as `tracing_appender`'s
/// non-blocking worker gives them, behind [`TagLevels`]. It never returns an
/// error: a line it can't write is dropped and counted.
pub struct LogFiles {
    dir: PathBuf,
    max_files: Option<usize>,
    format: LineFormat,
    limits: Limits,
    space: Box<dyn FreeSpace>,
    clock: Box<dyn Fn() -> SystemTime + Send>,
    current: Option<Segment>,
    /// When the last attempt to open a file failed.
    open_failed_at: Option<SystemTime>,
    /// The capacity guard's measurement the two counts below are since.
    measurement: Option<u64>,
    written_since: u64,
    deleted_since: u64,
    /// Whether the writer is at the full line: it has had to give way, and
    /// the disk hasn't had room again since.
    at_line: bool,
    /// The bytes the writer deleted since it came to the line.
    freed_at_line: u64,
    dropped: Dropped,
}

impl LogFiles {
    /// Log files in `dir`, which is created if missing, giving way at the
    /// full line the node's chunk store reads. A new file is started now.
    ///
    /// # Errors
    ///
    /// When `dir` can't be created.
    pub fn open(dir: &Path, max_files: Option<usize>, format: LineFormat) -> io::Result<Self> {
        Self::with_space(
            dir,
            max_files,
            format,
            Box::new(ChunkStoreSpace),
            Box::new(SystemTime::now),
        )
    }

    /// As [`Self::open`], reading the free space from `space` and the time
    /// from `clock`.
    ///
    /// # Errors
    ///
    /// When `dir` can't be created.
    pub fn with_space(
        dir: &Path,
        max_files: Option<usize>,
        format: LineFormat,
        space: Box<dyn FreeSpace>,
        clock: Box<dyn Fn() -> SystemTime + Send>,
    ) -> io::Result<Self> {
        Self::with_limits(dir, max_files, format, Limits::SHIPPED, space, clock)
    }

    fn with_limits(
        dir: &Path,
        max_files: Option<usize>,
        format: LineFormat,
        limits: Limits,
        space: Box<dyn FreeSpace>,
        clock: Box<dyn Fn() -> SystemTime + Send>,
    ) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let mut files = Self {
            dir: dir.to_path_buf(),
            max_files,
            format,
            limits,
            space,
            clock,
            current: None,
            open_failed_at: None,
            measurement: None,
            written_since: 0,
            deleted_since: 0,
            at_line: false,
            freed_at_line: 0,
            dropped: Dropped::default(),
        };
        let now = (files.clock)();
        files.start_segment(now);
        Ok(files)
    }

    /// The name of the file being written, if one is open.
    #[must_use]
    pub fn current_file(&self) -> Option<&str> {
        self.current.as_ref().map(|segment| segment.name.as_str())
    }

    /// Writes one line, or drops it at the full line (see the module's
    /// description).
    fn write_line(&mut self, kept_first: bool, line: &[u8]) {
        let need = len_u64(line);
        if let Some(reading) = self.space.reading() {
            if self.measurement != Some(reading.measurement) {
                // A new query of the filesystem counts everything written and
                // deleted before it.
                self.measurement = Some(reading.measurement);
                self.written_since = 0;
                self.deleted_since = 0;
            }
            let measured = reading.free.saturating_sub(self.written_since);
            if self.at_line {
                // Room the writer freed itself doesn't count as room again.
                let clear = reading
                    .line
                    .saturating_add(need)
                    .saturating_add(self.freed_at_line.max(self.limits.segment_max));
                if measured > clear {
                    self.at_line = false;
                    self.freed_at_line = 0;
                }
            }
            if self.at_line || measured <= reading.line.saturating_add(need) {
                if !self.give_way(reading.line, measured, need, kept_first) {
                    self.dropped.lines += 1;
                    self.dropped.bytes += need;
                    if kept_first {
                        self.dropped.kept_first += 1;
                    }
                    return;
                }
            } else if self.dropped.lines > 0 {
                let summary = self.summary_line();
                if measured > reading.line.saturating_add(need + len_u64(&summary)) {
                    self.append(&summary);
                    self.dropped = Dropped::default();
                }
            }
        }
        self.append(line);
    }

    /// At the full line: whether a line of `need` bytes may be written. Only
    /// a warning or an error may, and only into room above the line; one
    /// without room deletes this writer's oldest closed files until there is
    /// room for it, or none are left. Any other line is dropped, deleting
    /// nothing.
    fn give_way(&mut self, line: u64, measured: u64, need: u64, kept_first: bool) -> bool {
        self.at_line = true;
        if !kept_first {
            return false;
        }
        let room = |deleted: u64| measured.saturating_add(deleted) > line.saturating_add(need);
        while !room(self.deleted_since) {
            match self.delete_oldest_closed() {
                Some(freed) => {
                    self.deleted_since += freed;
                    self.freed_at_line = self.freed_at_line.saturating_add(freed);
                }
                None => break,
            }
        }
        room(self.deleted_since)
    }

    /// Appends `bytes` to the current file, starting a new one first if it is
    /// due.
    fn append(&mut self, bytes: &[u8]) {
        let now = (self.clock)();
        let segment_max = self.limits.segment_max;
        let due = self.current.as_ref().is_none_or(|segment| {
            segment.len > 0
                && (segment.len + len_u64(bytes) > segment_max || hour_of(now) != segment.hour)
        });
        let retry_wait = self.open_failed_at.is_some_and(|failed| {
            now.duration_since(failed)
                .is_ok_and(|since| since < REOPEN_AFTER)
        });
        if due && !retry_wait {
            self.start_segment(now);
        }
        let Some(segment) = self.current.as_mut() else {
            self.count_failed(bytes);
            return;
        };
        if segment.file.write_all(bytes).is_ok() {
            segment.len += len_u64(bytes);
            self.written_since += len_u64(bytes);
        } else {
            self.count_failed(bytes);
        }
    }

    fn count_failed(&mut self, bytes: &[u8]) {
        self.dropped.lines += 1;
        self.dropped.bytes += len_u64(bytes);
    }

    /// Starts a file named for `now`, unless the current one has that name,
    /// then deletes the oldest closed files beyond the folder's limits.
    fn start_segment(&mut self, now: SystemTime) {
        let name = segment_name(now);
        if self
            .current
            .as_ref()
            .is_none_or(|segment| segment.name != name)
        {
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(&name))
            {
                Ok(file) => {
                    let len = file.metadata().map_or(0, |meta| meta.len());
                    self.current = Some(Segment {
                        file,
                        name,
                        len,
                        hour: hour_of(now),
                    });
                    self.open_failed_at = None;
                }
                Err(_) => self.open_failed_at = Some(now),
            }
        }
        self.enforce_limits();
    }

    /// Deletes the oldest closed files until the closed ones and the current
    /// one, at the most it can grow to, fit in [`FOLDER_MAX_BYTES`], and
    /// until no more than `max_files` files are left.
    fn enforce_limits(&mut self) {
        let segment_max = self.limits.segment_max;
        let current_most = self
            .current
            .as_ref()
            .map_or(0, |segment| segment.len.max(segment_max));
        let budget = self.limits.folder_max.saturating_sub(current_most);
        let closed_allowed = self
            .max_files
            .map(|max| max.saturating_sub(usize::from(self.current.is_some())));
        let closed = self.closed_files();
        let mut total: u64 = closed.iter().map(|(_, len)| len).sum();
        let mut count = closed.len();
        for (name, len) in closed {
            if total <= budget && closed_allowed.is_none_or(|allowed| count <= allowed) {
                break;
            }
            if fs::remove_file(self.dir.join(&name)).is_ok() {
                total -= len;
                count -= 1;
                self.deleted_since += len;
            }
        }
    }

    /// Deletes the oldest closed file, returning how many bytes it held.
    fn delete_oldest_closed(&self) -> Option<u64> {
        for (name, len) in self.closed_files() {
            if fs::remove_file(self.dir.join(&name)).is_ok() {
                return Some(len);
            }
        }
        None
    }

    /// This writer's files other than the current one, oldest first, with
    /// their sizes. The daily files of releases up to ds.15 are among them.
    fn closed_files(&self) -> Vec<(String, u64)> {
        let current = self.current.as_ref().map(|segment| segment.name.as_str());
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut files: Vec<(String, u64)> = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                let ours = name.starts_with(FILE_PREFIX)
                    && name.ends_with(FILE_SUFFIX)
                    && Some(name.as_str()) != current;
                let meta = entry.metadata().ok()?;
                (ours && meta.is_file()).then_some((name, meta.len()))
            })
            .collect();
        files.sort();
        files
    }

    /// The line that says what was dropped, in the node's log format.
    fn summary_line(&self) -> Vec<u8> {
        const MESSAGE: &str = "Log lines dropped while the disk was at its full line";
        const TARGET: &str = "ant_node::log_files";
        let now = chrono::DateTime::<chrono::Utc>::from((self.clock)())
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let Dropped {
            lines,
            bytes,
            kept_first,
        } = self.dropped;
        match self.format {
            LineFormat::Text => format!(
                "{now}  WARN {TARGET}: {MESSAGE} lines={lines} bytes={bytes} \
                 warnings_or_errors={kept_first}\n"
            ),
            LineFormat::Json => format!(
                "{{\"timestamp\":\"{now}\",\"level\":\"WARN\",\"message\":\"{MESSAGE}\",\
                 \"lines\":{lines},\"bytes\":{bytes},\"warnings_or_errors\":{kept_first},\
                 \"target\":\"{TARGET}\",\"node_version\":\"{}\",\"node_commit\":\"{}\"}}\n",
                env!("CARGO_PKG_VERSION"),
                env!("ANT_GIT_COMMIT"),
            ),
        }
        .into_bytes()
    }
}

impl Write for LogFiles {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (kept_first, line) = match buf.split_first() {
            Some((&TAG_KEEP_FIRST, line)) => (true, line),
            Some((&TAG_DROP_FIRST, line)) => (false, line),
            _ => (false, buf),
        };
        if !line.is_empty() {
            self.write_line(kept_first, line);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.current.as_mut() {
            Some(segment) => segment.file.flush(),
            None => Ok(()),
        }
    }
}

/// Marks each event's line with its level, for [`LogFiles`]: a `MakeWriter`
/// around the writer the log layer writes to (the non-blocking channel to
/// [`LogFiles`]), which puts one byte before each line.
#[derive(Clone)]
pub struct TagLevels<W>(pub W);

/// The writer [`TagLevels`] gives for one event.
pub struct Tagged<W> {
    inner: W,
    tag: u8,
}

impl<'a, W: Write + Clone + 'a> MakeWriter<'a> for TagLevels<W> {
    type Writer = Tagged<W>;

    fn make_writer(&'a self) -> Self::Writer {
        Tagged {
            inner: self.0.clone(),
            tag: TAG_DROP_FIRST,
        }
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        let tag = if matches!(*meta.level(), Level::ERROR | Level::WARN)
            || KEEP_FIRST_TARGETS.contains(&meta.target())
        {
            TAG_KEEP_FIRST
        } else {
            TAG_DROP_FIRST
        };
        Tagged {
            inner: self.0.clone(),
            tag,
        }
    }
}

impl<W: Write> Write for Tagged<W> {
    /// Passes `buf`, one formatted event, on in one write, after the tag.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut line = Vec::with_capacity(buf.len() + 1);
        line.push(self.tag);
        line.extend_from_slice(buf);
        self.inner.write_all(&line)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn len_u64(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

fn hour_of(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs() / SEGMENT_MAX_AGE.as_secs())
}

fn segment_name(time: SystemTime) -> String {
    let started = chrono::DateTime::<chrono::Utc>::from(time);
    format!(
        "{FILE_PREFIX}{}{FILE_SUFFIX}",
        started.format("%Y-%m-%dT%H-%M-%S")
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
