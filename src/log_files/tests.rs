//! The log files' segments, folder cap and full line, on a partition the
//! tests measure themselves: the folder's own files plus whatever else the
//! test says the disk holds.

use super::*;
use std::sync::{Arc, Mutex};

const KIB: u64 = 1024;

/// The folder limits these tests use, small enough to reach with short lines.
const SMALL: Limits = Limits {
    segment_max: 4 * KIB,
    folder_max: 16 * KIB,
};

/// A partition of `size` bytes holding the log folder and `other` bytes
/// besides. Each reading measures it again, unless `stale` holds the last
/// measurement, as the capacity guard holds one for a few seconds.
#[derive(Clone)]
struct Partition(Arc<Mutex<PartitionState>>);

struct PartitionState {
    dir: PathBuf,
    size: u64,
    other: u64,
    line: u64,
    stale: bool,
    measurement: u64,
    measured_free: u64,
}

impl Partition {
    fn new(dir: &Path, size: u64, line: u64) -> Self {
        Self(Arc::new(Mutex::new(PartitionState {
            dir: dir.to_path_buf(),
            size,
            other: 0,
            line,
            stale: false,
            measurement: 0,
            measured_free: 0,
        })))
    }

    /// Free bytes now: the size, less the folder and everything else.
    fn free(&self) -> u64 {
        let state = self.0.lock().unwrap();
        state
            .size
            .saturating_sub(folder_bytes(&state.dir))
            .saturating_sub(state.other)
    }

    /// Fills the disk until `free` bytes are left (other writers, such as
    /// chunks).
    fn fill_to(&self, free: u64) {
        let folder = folder_bytes(&self.0.lock().unwrap().dir);
        let mut state = self.0.lock().unwrap();
        state.other = state.size - folder - free;
    }

    fn hold_readings(&self, stale: bool) {
        self.0.lock().unwrap().stale = stale;
    }
}

impl FreeSpace for Partition {
    fn reading(&mut self) -> Option<SpaceReading> {
        let free = self.free();
        let mut state = self.0.lock().unwrap();
        if !state.stale || state.measurement == 0 {
            state.measurement += 1;
            state.measured_free = free;
        }
        Some(SpaceReading {
            free: state.measured_free,
            line: state.line,
            measurement: state.measurement,
        })
    }
}

/// A partition nobody can measure: no chunk store open yet.
struct Unknown;

impl FreeSpace for Unknown {
    fn reading(&mut self) -> Option<SpaceReading> {
        None
    }
}

/// A clock the test moves.
#[derive(Clone)]
struct Clock(Arc<Mutex<SystemTime>>);

impl Clock {
    /// 2026-10-25 14:03:07 UTC.
    fn new() -> Self {
        Self(Arc::new(Mutex::new(
            UNIX_EPOCH + Duration::from_secs(1_792_936_987),
        )))
    }

    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }

    fn boxed(&self) -> Box<dyn Fn() -> SystemTime + Send> {
        let clock = self.clone();
        Box::new(move || *clock.0.lock().unwrap())
    }
}

fn folder_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter_map(|entry| entry.metadata().ok())
            .map(|meta| meta.len())
            .sum()
    })
}

/// The writer's files, oldest first.
fn log_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().into_string().unwrap())
        .filter(|name| name.starts_with(FILE_PREFIX) && name.ends_with(FILE_SUFFIX))
        .collect();
    names.sort();
    names
}

fn text_of(dir: &Path) -> String {
    log_files(dir)
        .iter()
        .map(|name| fs::read_to_string(dir.join(name)).unwrap())
        .collect()
}

fn info(files: &mut LogFiles, text: &str) {
    let line = format!("{text}\n");
    let mut tagged = vec![TAG_DROP_FIRST];
    tagged.extend_from_slice(line.as_bytes());
    assert_eq!(files.write(&tagged).unwrap(), tagged.len());
}

fn warn(files: &mut LogFiles, text: &str) {
    let line = format!("{text}\n");
    let mut tagged = vec![TAG_KEEP_FIRST];
    tagged.extend_from_slice(line.as_bytes());
    assert_eq!(files.write(&tagged).unwrap(), tagged.len());
}

/// A line of `len` bytes, newline included.
fn line_of(len: usize, fill: char) -> String {
    std::iter::repeat_n(fill, len - 1).collect()
}

fn small_files(dir: &Path, space: Box<dyn FreeSpace>, clock: &Clock) -> LogFiles {
    LogFiles::with_limits(dir, None, LineFormat::Text, SMALL, space, clock.boxed()).unwrap()
}

#[test]
fn the_shipped_limits_are_50_mib_files_an_hour_at_most_and_500_mib_a_folder() {
    assert_eq!(SEGMENT_MAX_BYTES, 50 * 1024 * 1024);
    assert_eq!(FOLDER_MAX_BYTES, 500 * 1024 * 1024);
    assert_eq!(SEGMENT_MAX_AGE, Duration::from_hours(1));
    assert_eq!(Limits::SHIPPED.segment_max, SEGMENT_MAX_BYTES);
    assert_eq!(Limits::SHIPPED.folder_max, FOLDER_MAX_BYTES);
}

#[test]
fn files_are_named_for_when_they_start_and_sort_after_earlier_daily_files() {
    let clock = Clock::new();
    assert_eq!(
        segment_name(*clock.0.lock().unwrap()),
        "ant-node.2026-10-25T14-03-07.log"
    );
    // The daily files of releases up to ds.15 sort first, the same day's too,
    // as the server scripts that read the files "by date, oldest first" need.
    let mut names = vec![
        "ant-node.2026-10-25T14-03-07.log".to_string(),
        "ant-node.2026-10-25.log".to_string(),
        "ant-node.2026-10-24.log".to_string(),
        "ant-node.2026-10-25T09-00-00.log".to_string(),
    ];
    names.sort();
    assert_eq!(
        names,
        [
            "ant-node.2026-10-24.log",
            "ant-node.2026-10-25.log",
            "ant-node.2026-10-25T09-00-00.log",
            "ant-node.2026-10-25T14-03-07.log",
        ]
    );
}

#[test]
fn a_new_file_starts_before_one_would_pass_its_size_and_in_each_new_hour() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let mut files = small_files(dir.path(), Box::new(Unknown), &clock);
    let first = files.current_file().unwrap().to_string();

    // 4 lines of 1,000 bytes fit in 4 KiB; a 5th would pass it.
    let line = line_of(1000, 'a');
    for _ in 0..4 {
        info(&mut files, &line);
    }
    assert_eq!(files.current_file().unwrap(), first);
    clock.advance(Duration::from_secs(1));
    info(&mut files, &line);
    let second = files.current_file().unwrap().to_string();
    assert_ne!(second, first, "the 5th line went to a new file");
    assert_eq!(fs::metadata(dir.path().join(&first)).unwrap().len(), 4000);

    // The next hour starts a new file, however little the current one holds.
    clock.advance(Duration::from_hours(1));
    info(&mut files, "a new hour");
    let third = files.current_file().unwrap().to_string();
    assert_ne!(third, second);
    assert_eq!(log_files(dir.path()), [first, second, third]);

    // Within the same second, a file that is due keeps going: names stay
    // unique.
    for _ in 0..5 {
        info(&mut files, &line);
    }
    assert_eq!(log_files(dir.path()).len(), 3);
}

#[test]
fn an_empty_file_is_never_left_for_a_new_hour() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let mut files = small_files(dir.path(), Box::new(Unknown), &clock);
    clock.advance(Duration::from_hours(3));
    info(&mut files, "late");
    assert_eq!(log_files(dir.path()).len(), 1, "the empty file was used");
}

#[test]
fn the_folder_never_holds_more_than_its_cap_and_the_oldest_files_go_first() {
    let dir = tempfile::tempdir().unwrap();
    // Files of releases up to ds.15, and a file that isn't the writer's.
    fs::write(
        dir.path().join("ant-node.2026-10-23.log"),
        line_of(3000, 'o'),
    )
    .unwrap();
    fs::write(
        dir.path().join("ant-node.2026-10-24.log"),
        line_of(3000, 'p'),
    )
    .unwrap();
    fs::write(dir.path().join("stdout.log"), "kept").unwrap();
    let clock = Clock::new();
    let mut files = small_files(dir.path(), Box::new(Unknown), &clock);

    let line = line_of(700, 'x');
    let mut seen = Vec::new();
    for _ in 0..200 {
        clock.advance(Duration::from_secs(1));
        info(&mut files, &line);
        let writer_bytes: u64 = log_files(dir.path())
            .iter()
            .map(|name| fs::metadata(dir.path().join(name)).unwrap().len())
            .sum();
        assert!(writer_bytes <= SMALL.folder_max, "{writer_bytes} bytes");
        for name in log_files(dir.path()) {
            if !seen.contains(&name) {
                seen.push(name);
            }
        }
    }
    // Deleted in the order they were written, the earlier releases' first.
    let kept = log_files(dir.path());
    assert!(!kept.contains(&"ant-node.2026-10-23.log".to_string()));
    assert!(!kept.contains(&"ant-node.2026-10-24.log".to_string()));
    assert_eq!(seen[seen.len() - kept.len()..], kept[..]);
    assert_eq!(
        fs::read_to_string(dir.path().join("stdout.log")).unwrap(),
        "kept"
    );
}

#[test]
fn a_file_count_limit_given_is_kept_too() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let mut files = LogFiles::with_limits(
        dir.path(),
        Some(2),
        LineFormat::Text,
        SMALL,
        Box::new(Unknown),
        clock.boxed(),
    )
    .unwrap();
    for hour in 0..5 {
        clock.advance(Duration::from_hours(1));
        info(&mut files, &format!("hour {hour}"));
    }
    assert_eq!(log_files(dir.path()).len(), 2);
    assert!(text_of(dir.path()).contains("hour 4"));
}

#[test]
fn at_the_full_line_old_files_go_first_then_only_warnings_are_written() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let line_at = 100 * KIB;
    let partition = Partition::new(dir.path(), 10 * 1024 * KIB, line_at);
    let limits = Limits {
        segment_max: 4 * KIB,
        folder_max: 64 * KIB,
    };
    let mut files = LogFiles::with_limits(
        dir.path(),
        None,
        LineFormat::Text,
        limits,
        Box::new(partition.clone()),
        clock.boxed(),
    )
    .unwrap();

    // Five files of 4 lines each, four of them closed.
    for n in 0..20 {
        clock.advance(Duration::from_secs(1));
        info(&mut files, &line_of(1000, char::from(b'a' + n)));
    }
    assert_eq!(log_files(dir.path()).len(), 5);

    // Each time the disk is at the line (other writers take back whatever
    // the log files free), an info line is dropped, deleting nothing; a
    // warning makes room for itself: the oldest closed file gives way, and
    // the warning is written into the room freed for it.
    let steps: [(bool, &str); 6] = [
        // Short, so the four warnings fit the current file beside its 4000
        // bytes and no new file starts.
        (false, "info 1"),
        (true, "warn 2"),
        (false, "info 3"),
        (true, "warn 4"),
        (true, "warn 5"),
        (true, "warn 6"),
    ];
    for (n, (warning, text)) in steps.into_iter().enumerate() {
        partition.fill_to(line_at);
        let oldest = log_files(dir.path())[0].clone();
        if warning {
            warn(&mut files, text);
        } else {
            info(&mut files, text);
        }
        assert_eq!(
            !log_files(dir.path()).contains(&oldest),
            warning,
            "step {n}: only a warning makes the oldest file give way"
        );
        assert_eq!(
            text_of(dir.path()).contains(text),
            warning,
            "step {n}: {text}"
        );
        assert!(
            partition.free() >= line_at,
            "step {n}: never below the line"
        );
    }

    // With no closed file left to delete, nothing is written at all.
    assert_eq!(log_files(dir.path()).len(), 1);
    partition.fill_to(line_at);
    let before = folder_bytes(dir.path());
    for _ in 0..50 {
        warn(&mut files, "a warning with nothing left to give");
        info(&mut files, "an info line with nothing left to give");
    }
    assert_eq!(
        folder_bytes(dir.path()),
        before,
        "no byte added at the line"
    );
    assert_eq!(partition.free(), line_at);
}

/// Five files of 4 lines each (four of them closed) on a partition at the
/// full line whose other writers don't take back what the writer frees.
fn at_the_line_with_five_files(dir: &Path, clock: &Clock, line_at: u64) -> (Partition, LogFiles) {
    let partition = Partition::new(dir, 10 * 1024 * KIB, line_at);
    let mut files = LogFiles::with_limits(
        dir,
        None,
        LineFormat::Text,
        Limits {
            segment_max: 4 * KIB,
            folder_max: 64 * KIB,
        },
        Box::new(partition.clone()),
        clock.boxed(),
    )
    .unwrap();
    for n in 0..20 {
        clock.advance(Duration::from_secs(1));
        info(&mut files, &line_of(1000, char::from(b'a' + n)));
    }
    assert_eq!(log_files(dir).len(), 5);
    partition.fill_to(line_at);
    (partition, files)
}

#[test]
fn at_the_line_the_room_the_writer_freed_is_not_spent_on_info_lines() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let line_at = 100 * KIB;
    let (partition, mut files) = at_the_line_with_five_files(dir.path(), &clock, line_at);

    // A flood of info lines with a warning now and then. The first warning
    // frees one file; the room it freed stays free, and goes to warnings,
    // never to info lines, so no other file gives way.
    for n in 0..400 {
        if n % 50 == 0 {
            warn(&mut files, &format!("warning {n}"));
        } else {
            info(&mut files, &format!("info {n} {}", line_of(200, 'i')));
        }
    }
    assert_eq!(log_files(dir.path()).len(), 4, "one file gave way");
    let text = text_of(dir.path());
    assert!(!text.contains("info "), "an info line was written");
    for n in (0..400).step_by(50) {
        assert!(
            text.contains(&format!(
                "warning {n}
"
            )),
            "warning {n}"
        );
    }
    assert!(partition.free() >= line_at);
}

#[test]
fn the_writer_leaves_the_line_only_when_real_space_returns() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let line_at = 100 * KIB;
    let (partition, mut files) = at_the_line_with_five_files(dir.path(), &clock, line_at);
    warn(&mut files, "a warning that frees a file");
    assert_eq!(log_files(dir.path()).len(), 4);

    // A little more room than the writer freed, less than a file: still at
    // the line.
    partition.fill_to(line_at + 3 * KIB);
    info(&mut files, "still at the line");
    assert!(!text_of(dir.path()).contains("still at the line"));

    // Room above the line by more than a file and what the writer freed:
    // every line is written again, after the line saying what was dropped.
    partition.fill_to(line_at + 64 * KIB);
    info(&mut files, "room again");
    let text = text_of(dir.path());
    assert!(text.contains("room again"), "{text}");
    assert!(
        text.contains("Log lines dropped while the disk was at its full line"),
        "{text}"
    );
}

#[test]
fn the_writer_counts_its_own_bytes_between_readings() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let line_at = 100 * KIB;
    let partition = Partition::new(dir.path(), 10 * 1024 * KIB, line_at);
    let mut files = LogFiles::with_limits(
        dir.path(),
        None,
        LineFormat::Text,
        Limits {
            segment_max: 64 * KIB,
            folder_max: 1024 * KIB,
        },
        Box::new(partition.clone()),
        clock.boxed(),
    )
    .unwrap();
    // 8 KiB above the line, and the reading held while the writer writes on:
    // it stops at the line by its own count.
    partition.fill_to(line_at + 8 * KIB);
    info(&mut files, "measured");
    partition.hold_readings(true);
    for _ in 0..40 {
        info(&mut files, &line_of(500, 'z'));
    }
    assert!(
        partition.free() >= line_at,
        "{} free, below the line",
        partition.free()
    );
}

#[test]
fn what_was_dropped_is_said_once_the_disk_has_room_again() {
    for format in [LineFormat::Text, LineFormat::Json] {
        let dir = tempfile::tempdir().unwrap();
        let clock = Clock::new();
        let line_at = 100 * KIB;
        let partition = Partition::new(dir.path(), 10 * 1024 * KIB, line_at);
        let mut files = LogFiles::with_limits(
            dir.path(),
            None,
            format,
            SMALL,
            Box::new(partition.clone()),
            clock.boxed(),
        )
        .unwrap();
        partition.fill_to(line_at);
        for _ in 0..3 {
            info(&mut files, "lost");
        }
        warn(&mut files, "lost too");
        partition.fill_to(line_at + 512 * KIB);
        info(&mut files, "back");
        info(&mut files, "and on");

        let text = text_of(dir.path());
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "{format:?}: {text}");
        assert!(lines[1] == "back" && lines[2] == "and on", "{text}");
        let summary = lines[0];
        assert!(
            summary.contains("Log lines dropped while the disk was at its full line"),
            "{summary}"
        );
        match format {
            LineFormat::Text => {
                assert!(summary.contains(" WARN ant_node::log_files: "), "{summary}");
                assert!(
                    summary.ends_with("lines=4 bytes=24 warnings_or_errors=1"),
                    "{summary}"
                );
            }
            LineFormat::Json => {
                let doc: serde_json::Value = serde_json::from_str(summary).unwrap();
                assert_eq!(doc["level"], "WARN");
                assert_eq!(doc["lines"], 4);
                assert_eq!(doc["bytes"], 24);
                assert_eq!(doc["warnings_or_errors"], 1);
                assert_eq!(doc["node_version"], env!("CARGO_PKG_VERSION"));
            }
        }
    }
}

#[test]
fn without_a_reading_the_writer_writes_as_usual() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Clock::new();
    let mut files = small_files(dir.path(), Box::new(Unknown), &clock);
    info(&mut files, "no store open yet");
    assert!(text_of(dir.path()).contains("no store open yet"));
}

/// The bytes `TagLevels` passed on, by level.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Vec<u8>>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().push(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn warnings_and_errors_are_marked_to_be_kept_first() {
    use tracing_subscriber::prelude::*;

    let capture = Capture::default();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(TagLevels(capture.clone()));
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::error!("an error");
        tracing::warn!("a warning");
        tracing::info!("some news");
        tracing::debug!("a detail");
    });
    let lines = capture.0.lock().unwrap().clone();
    let tags: Vec<u8> = lines.iter().map(|line| line[0]).collect();
    assert_eq!(
        tags,
        [
            TAG_KEEP_FIRST,
            TAG_KEEP_FIRST,
            TAG_DROP_FIRST,
            TAG_DROP_FIRST
        ]
    );
    // One write each, the whole formatted line after the tag.
    assert!(String::from_utf8_lossy(&lines[0][1..]).contains("an error"));
    assert!(lines.iter().all(|line| line.ends_with(b"\n")));
}
