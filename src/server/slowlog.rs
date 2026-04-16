//! In-memory slow-command log.
//!
//! Records commands whose execution exceeded a configurable wall-clock
//! threshold. Powers `SLOWLOG GET / LEN / RESET / HELP`, which give
//! operators visibility into latency outliers without needing to scrape
//! Prometheus histograms.
//!
//! # Design
//!
//! The log is a fixed-capacity ring keyed by a monotonically-increasing
//! id. Old entries are silently overwritten when the ring is full —
//! matching Redis behavior. Storage is bounded so a runaway log can't
//! exhaust memory.
//!
//! All access goes through a `Mutex<VecDeque>` — slow log is naturally
//! low-volume (only commands above the threshold) so contention is
//! negligible compared to FDB latency.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;

/// One slow-log entry. Cloned out on `SLOWLOG GET` so the lock isn't
/// held during RESP encoding.
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// Monotonic id (never reused). Mirrors Redis ENTRY\[0\].
    pub id: u64,
    /// Wall-clock UNIX seconds at which the command finished.
    pub timestamp_secs: u64,
    /// Wall-clock duration of the command in microseconds.
    pub duration_us: u64,
    /// Command name + arguments. Truncated to `max_argc` arguments
    /// and each argument truncated to `max_arg_len` bytes to bound
    /// memory use under pathological loads.
    pub argv: Vec<Bytes>,
    /// Peer address of the client that issued the command.
    pub client_addr: SocketAddr,
    /// `CLIENT SETNAME` value at log time. Empty if unset.
    pub client_name: Bytes,
}

/// Configuration for the slow log. All fields are atomic so they can be
/// adjusted at runtime via `CONFIG SET slowlog-log-slower-than ...`.
pub struct SlowLogConfig {
    /// Threshold in microseconds. Commands taking >= this duration are
    /// recorded. A negative value disables the log; zero records every
    /// command (useful for debugging, dangerous in production).
    pub threshold_us: AtomicI64,
    /// Maximum entries kept. Older entries are overwritten when full.
    pub max_len: AtomicUsize,
}

impl SlowLogConfig {
    /// Redis defaults: 10ms threshold, 128 entries.
    pub const DEFAULT_THRESHOLD_US: i64 = 10_000;
    pub const DEFAULT_MAX_LEN: usize = 128;
}

impl Default for SlowLogConfig {
    fn default() -> Self {
        Self {
            threshold_us: AtomicI64::new(Self::DEFAULT_THRESHOLD_US),
            max_len: AtomicUsize::new(Self::DEFAULT_MAX_LEN),
        }
    }
}

/// The shared slow log. Cheap to clone (it's an `Arc`).
pub struct SlowLog {
    cfg: SlowLogConfig,
    next_id: AtomicU64,
    /// Buffer holds at most `max_len` entries; oldest at front.
    buf: Mutex<std::collections::VecDeque<SlowLogEntry>>,
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::new()
    }
}

impl SlowLog {
    /// Per-argument truncation: matches Redis SLOWLOG_ENTRY_MAX_STRING.
    pub const MAX_ARG_LEN: usize = 128;
    /// Per-command argv truncation: matches Redis SLOWLOG_ENTRY_MAX_ARGC.
    pub const MAX_ARGC: usize = 32;

    pub fn new() -> Self {
        Self::with_config(SlowLogConfig::default())
    }

    pub fn with_config(cfg: SlowLogConfig) -> Self {
        let max_len = cfg.max_len.load(Ordering::Relaxed);
        Self {
            cfg,
            next_id: AtomicU64::new(0),
            buf: Mutex::new(std::collections::VecDeque::with_capacity(max_len)),
        }
    }

    /// Current threshold in microseconds. Negative disables logging.
    pub fn threshold_us(&self) -> i64 {
        self.cfg.threshold_us.load(Ordering::Relaxed)
    }

    /// Set the threshold in microseconds.
    pub fn set_threshold_us(&self, value: i64) {
        self.cfg.threshold_us.store(value, Ordering::Relaxed);
    }

    /// Maximum entries kept.
    pub fn max_len(&self) -> usize {
        self.cfg.max_len.load(Ordering::Relaxed)
    }

    /// Resize the log. Truncates oldest entries if shrinking.
    pub fn set_max_len(&self, max: usize) {
        self.cfg.max_len.store(max, Ordering::Relaxed);
        let mut buf = self.buf.lock().unwrap();
        while buf.len() > max {
            buf.pop_front();
        }
    }

    /// Record a command if its duration exceeds the threshold.
    /// Returns `true` if the entry was logged.
    pub fn maybe_record(
        &self,
        cmd_name: &Bytes,
        args: &[Bytes],
        duration_us: u64,
        client_addr: SocketAddr,
        client_name: Bytes,
    ) -> bool {
        let threshold = self.threshold_us();
        if threshold < 0 {
            return false;
        }
        if (duration_us as i64) < threshold {
            return false;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timestamp_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Build argv with bounded capacity. Always include the command name.
        let mut argv = Vec::with_capacity(1 + args.len().min(Self::MAX_ARGC));
        argv.push(truncate_arg(cmd_name));
        for arg in args.iter().take(Self::MAX_ARGC.saturating_sub(1)) {
            argv.push(truncate_arg(arg));
        }
        // If we dropped extra args, append a marker so operators know.
        if args.len() + 1 > Self::MAX_ARGC {
            argv.push(Bytes::from(format!(
                "... ({} more arguments)",
                args.len() + 1 - Self::MAX_ARGC
            )));
        }

        let entry = SlowLogEntry {
            id,
            timestamp_secs,
            duration_us,
            argv,
            client_addr,
            client_name,
        };

        let mut buf = self.buf.lock().unwrap();
        let max = self.cfg.max_len.load(Ordering::Relaxed).max(1);
        if buf.len() >= max {
            buf.pop_front();
        }
        buf.push_back(entry);
        true
    }

    /// Number of entries currently stored.
    pub fn len(&self) -> usize {
        self.buf.lock().unwrap().len()
    }

    /// Returns true if no entries.
    pub fn is_empty(&self) -> bool {
        self.buf.lock().unwrap().is_empty()
    }

    /// Drop all stored entries. The next id continues to grow.
    pub fn reset(&self) {
        self.buf.lock().unwrap().clear();
    }

    /// Snapshot up to `count` entries, newest first.
    pub fn snapshot(&self, count: usize) -> Vec<SlowLogEntry> {
        let buf = self.buf.lock().unwrap();
        buf.iter().rev().take(count).cloned().collect()
    }
}

/// Cap a single argument at `MAX_ARG_LEN` bytes, appending a length
/// marker so operators see what was elided.
fn truncate_arg(arg: &Bytes) -> Bytes {
    if arg.len() <= SlowLog::MAX_ARG_LEN {
        return arg.clone();
    }
    let mut out = Vec::with_capacity(SlowLog::MAX_ARG_LEN + 32);
    out.extend_from_slice(&arg[..SlowLog::MAX_ARG_LEN]);
    out.extend_from_slice(format!("... ({} bytes total)", arg.len()).as_bytes());
    Bytes::from(out)
}

/// Type alias used everywhere that needs the slow log.
pub type SharedSlowLog = Arc<SlowLog>;

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_addr() -> SocketAddr {
        "127.0.0.1:1234".parse().unwrap()
    }

    #[test]
    fn does_not_log_below_threshold() {
        let log = SlowLog::new();
        log.set_threshold_us(1000);
        let recorded = log.maybe_record(
            &Bytes::from_static(b"GET"),
            &[Bytes::from_static(b"k")],
            500,
            fake_addr(),
            Bytes::new(),
        );
        assert!(!recorded);
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn logs_at_or_above_threshold() {
        let log = SlowLog::new();
        log.set_threshold_us(1000);
        let recorded = log.maybe_record(
            &Bytes::from_static(b"GET"),
            &[Bytes::from_static(b"k")],
            1000,
            fake_addr(),
            Bytes::new(),
        );
        assert!(recorded);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn negative_threshold_disables() {
        let log = SlowLog::new();
        log.set_threshold_us(-1);
        let recorded = log.maybe_record(
            &Bytes::from_static(b"GET"),
            &[],
            1_000_000_000,
            fake_addr(),
            Bytes::new(),
        );
        assert!(!recorded);
    }

    #[test]
    fn ring_overwrites_oldest() {
        let log = SlowLog::new();
        log.set_threshold_us(0);
        log.set_max_len(3);
        for i in 0..5 {
            log.maybe_record(&Bytes::from(format!("CMD{i}")), &[], 10, fake_addr(), Bytes::new());
        }
        assert_eq!(log.len(), 3);
        let snap = log.snapshot(10);
        assert_eq!(snap.len(), 3);
        // Newest first.
        assert_eq!(snap[0].argv[0].as_ref(), b"CMD4");
        assert_eq!(snap[2].argv[0].as_ref(), b"CMD2");
    }

    #[test]
    fn truncates_oversized_arg() {
        let log = SlowLog::new();
        log.set_threshold_us(0);
        let big = Bytes::from(vec![b'x'; SlowLog::MAX_ARG_LEN * 4]);
        log.maybe_record(&Bytes::from_static(b"SET"), &[big], 1, fake_addr(), Bytes::new());
        let snap = log.snapshot(1);
        let arg = &snap[0].argv[1];
        assert!(arg.len() < SlowLog::MAX_ARG_LEN * 4);
        assert!(arg.starts_with(&[b'x'; SlowLog::MAX_ARG_LEN]));
    }

    #[test]
    fn reset_clears_entries_but_id_continues() {
        let log = SlowLog::new();
        log.set_threshold_us(0);
        log.maybe_record(&Bytes::from_static(b"A"), &[], 1, fake_addr(), Bytes::new());
        log.maybe_record(&Bytes::from_static(b"B"), &[], 1, fake_addr(), Bytes::new());
        let id_before = log.snapshot(1)[0].id;
        log.reset();
        assert_eq!(log.len(), 0);
        log.maybe_record(&Bytes::from_static(b"C"), &[], 1, fake_addr(), Bytes::new());
        let id_after = log.snapshot(1)[0].id;
        assert!(id_after > id_before, "id should be monotonic across reset");
    }

    #[test]
    fn extra_args_marker() {
        let log = SlowLog::new();
        log.set_threshold_us(0);
        let many: Vec<Bytes> = (0..(SlowLog::MAX_ARGC + 5))
            .map(|i| Bytes::from(format!("a{i}")))
            .collect();
        log.maybe_record(&Bytes::from_static(b"MSET"), &many, 1, fake_addr(), Bytes::new());
        let snap = log.snapshot(1);
        let last = snap[0].argv.last().unwrap();
        let s = String::from_utf8_lossy(last);
        assert!(s.starts_with("..."), "expected truncation marker, got {s:?}");
    }
}
