//! When a node takes a release: at its publication plus the node's fixed
//! rollout offset, by GitHub's clock (its `Date`, the local clock without
//! one), so a restart doesn't start the delay again. Without a publication
//! time, or with one later than GitHub's now, the offset counts from when the
//! node first saw the release, as before. A file of its own, which upstream
//! doesn't have, so a merge can't drop it while resolving a conflict in the
//! monitor's tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{parse_http_date, ready_after_publication, Asset, GitHubRelease, UpgradeMonitor};
use crate::config::UpgradeChannel;
use crate::upgrade::release_cache::ReleaseCache;
use crate::upgrade::rollout::StagedRollout;
use chrono::{DateTime, TimeDelta, Utc};
use semver::Version;
use std::time::Duration;

const REPO: &str = "Project-DSpace/darwin-node";
const NODE: &[u8] = b"a node's identity";
const RUNNING: &str = "0.23.0+ds.15";
const RELEASE: &str = "0.23.0+ds.16";

/// How close two readings of the time left must be: the test's own run time.
const SLACK: Duration = Duration::from_secs(30);

/// A published release with a binary for this platform and its signature.
fn release(tag: &str, published_at: Option<DateTime<Utc>>) -> GitHubRelease {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    let binary = if cfg!(windows) {
        format!("ant-node-{arch}-{os}.exe")
    } else {
        format!("ant-node-{arch}-{os}")
    };
    GitHubRelease {
        tag_name: format!("v{tag}"),
        name: tag.to_string(),
        body: String::new(),
        prerelease: false,
        published_at,
        assets: vec![
            Asset {
                name: binary.clone(),
                browser_download_url: format!("https://example.com/{tag}"),
                size: Some(17_000_000),
            },
            Asset {
                name: format!("{binary}.sig"),
                browser_download_url: format!("https://example.com/{tag}.sig"),
                size: Some(3309),
            },
        ],
    }
}

/// A node running [`RUNNING`] with a 24-hour rollout window, reading its
/// releases from `cache`, so nothing is fetched from GitHub.
fn node(cache: &ReleaseCache) -> UpgradeMonitor {
    UpgradeMonitor::with_version(
        REPO.to_string(),
        UpgradeChannel::Stable,
        1,
        Version::parse(RUNNING).unwrap(),
    )
    .with_release_cache(cache.clone())
    .with_staged_rollout(NODE, 24)
}

/// This node's rollout offset for [`RELEASE`].
fn offset() -> Duration {
    let offset =
        StagedRollout::new(NODE, 24).calculate_delay_for_version(&Version::parse(RELEASE).unwrap());
    assert!(
        offset > SLACK * 4,
        "pick another NODE: its offset is {offset:?}"
    );
    offset
}

fn delta(duration: Duration) -> TimeDelta {
    TimeDelta::from_std(duration).unwrap()
}

fn assert_near(left: Duration, right: Duration) {
    let gap = left.abs_diff(right);
    assert!(gap <= SLACK, "{left:?} and {right:?} are {gap:?} apart");
}

fn cache_with(
    dir: &tempfile::TempDir,
    releases: &[GitHubRelease],
    github_ahead: Option<TimeDelta>,
) -> ReleaseCache {
    let cache = ReleaseCache::new(dir.path().to_path_buf(), Duration::from_secs(3600));
    cache.write(REPO, releases, github_ahead).unwrap();
    cache
}

#[tokio::test]
async fn after_a_restart_a_node_is_due_at_publication_plus_its_offset() {
    let dir = tempfile::tempdir().unwrap();
    let offset = offset();
    let published = Utc::now() - delta(offset / 2);
    let cache = cache_with(&dir, &[release(RELEASE, Some(published))], None);

    let mut first = node(&cache);
    assert!(first.check_for_ready_upgrade().await.unwrap().is_none());
    let info = first.check_for_updates().await.unwrap().unwrap();
    assert_eq!(info.published_at, Some(published));
    assert_eq!(info.archive_size, Some(17_000_000));
    assert_near(first.time_until_upgrade().unwrap(), offset / 2);

    // The same node after a restart: due at the same time, not a whole offset
    // from now.
    let mut restarted = node(&cache);
    assert!(restarted.check_for_ready_upgrade().await.unwrap().is_none());
    assert_near(restarted.time_until_upgrade().unwrap(), offset / 2);

    // Published longer ago than its offset: due as soon as it is seen.
    let long_ago = Utc::now() - delta(offset) - TimeDelta::minutes(1);
    let cache = cache_with(&dir, &[release(RELEASE, Some(long_ago))], None);
    let mut late = node(&cache);
    let ready = late.check_for_ready_upgrade().await.unwrap();
    assert_eq!(
        ready.map(|info| info.version),
        Some(Version::parse(RELEASE).unwrap())
    );
    assert_eq!(late.time_until_upgrade(), Some(Duration::ZERO));
}

#[tokio::test]
async fn a_missing_or_future_publication_counts_from_first_sight() {
    let offset = offset();
    for published in [None, Some(Utc::now() + TimeDelta::hours(2))] {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with(&dir, &[release(RELEASE, published)], None);
        let mut monitor = node(&cache);
        assert!(monitor.check_for_ready_upgrade().await.unwrap().is_none());
        assert_near(monitor.time_until_upgrade().unwrap(), offset);
        // A second check keeps the first sighting.
        assert!(monitor.check_for_ready_upgrade().await.unwrap().is_none());
        assert_near(monitor.time_until_upgrade().unwrap(), offset);
    }
}

#[tokio::test]
async fn now_is_github_s_clock_when_an_answer_gave_it() {
    let dir = tempfile::tempdir().unwrap();
    let offset = offset();
    // GitHub's clock is a whole offset ahead of this machine's, and the
    // release came out half an offset ago by it: half an offset from now by
    // the local clock, which would count a whole offset from first sight.
    let github_ahead = delta(offset);
    let published = Utc::now() + github_ahead - delta(offset / 2);
    let cache = cache_with(
        &dir,
        &[release(RELEASE, Some(published))],
        Some(github_ahead),
    );
    let mut monitor = node(&cache);
    assert!(monitor.check_for_ready_upgrade().await.unwrap().is_none());
    assert_near(monitor.time_until_upgrade().unwrap(), offset / 2);
}

#[tokio::test]
async fn a_withdrawn_release_resets() {
    let dir = tempfile::tempdir().unwrap();
    let offset = offset();
    let cache = cache_with(&dir, &[release(RELEASE, None)], None);
    let mut monitor = node(&cache);
    assert!(monitor.check_for_ready_upgrade().await.unwrap().is_none());
    assert!(monitor.pending_version().is_some());

    // Withdrawn: nothing pending.
    cache_with(&dir, &[], None);
    assert!(monitor.check_for_ready_upgrade().await.unwrap().is_none());
    assert_eq!(monitor.pending_version(), None);
    assert_eq!(monitor.time_until_upgrade(), None);

    // Published again: tracked afresh, from this sighting.
    cache_with(&dir, &[release(RELEASE, None)], None);
    assert!(monitor.check_for_ready_upgrade().await.unwrap().is_none());
    assert_near(monitor.time_until_upgrade().unwrap(), offset);
}

#[test]
fn the_time_left_counts_from_publication() {
    let now = Utc::now();
    let hour = Duration::from_secs(3600);
    assert_eq!(ready_after_publication(None, now, hour), None);
    assert_eq!(
        ready_after_publication(Some(now + TimeDelta::seconds(1)), now, hour),
        None,
        "a publication in the future"
    );
    assert_eq!(
        ready_after_publication(Some(now - TimeDelta::minutes(20)), now, hour),
        Some(Duration::from_mins(40))
    );
    assert_eq!(
        ready_after_publication(Some(now - TimeDelta::hours(2)), now, hour),
        Some(Duration::ZERO)
    );
    assert_eq!(ready_after_publication(Some(now), now, hour), Some(hour));
}

#[test]
fn github_s_date_header_is_read_as_utc() {
    assert_eq!(
        parse_http_date("Sun, 25 Oct 2026 14:03:07 GMT"),
        Some(
            DateTime::parse_from_rfc3339("2026-10-25T14:03:07Z")
                .unwrap()
                .with_timezone(&Utc)
        )
    );
    assert_eq!(parse_http_date("yesterday"), None);
}
