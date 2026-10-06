// =============================================================================
// Ping execution engine with DNS resolution, metrics, and CSV export.
// =============================================================================

#![allow(non_snake_case)]

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::net::{IpAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(windows))]
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
#[cfg(windows)]
use std::time::Instant;

use chrono::{DateTime, Utc};
use eframe::egui;
#[cfg(not(windows))]
use pinger::{PingOptions, PingResult};

#[derive(Debug, Clone)]
pub struct PingRecord {
        pub _timestamp: DateTime<Utc>,
        pub _pingMs: Option<f64>,
        pub _isTimeout: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PingStats {
        pub total_sent: usize,
        pub total_received: usize,
        pub total_timeouts: usize,
        pub min_ms: Option<f64>,
        pub max_ms: Option<f64>,
        pub avg_ms: Option<f64>,
        pub current_ms: Option<f64>,
        pub jitter_ms: Option<f64>,
}

pub enum SessionEvent {
        Record(PingRecord),
        Error(String),
        Finished(Option<String>),
}

pub struct PingSession {
        _stopSignal: Arc<AtomicBool>,
        _running: Arc<AtomicBool>,
        _events: Receiver<SessionEvent>,
        _handle: Option<thread::JoinHandle<()>>,
        _target: String,
        _resolvedIp: IpAddr,
        _csvPath: PathBuf,
}

#[cfg(not(windows))]
const POLL_TIMEOUT_MS: u64 = 100;
#[cfg(windows)]
const STOP_POLL_MS: u64 = 100;
static TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

impl PingRecord {
        pub fn getTimestamp(&self) -> DateTime<Utc> {
                self._timestamp
        }

        pub fn getPingMs(&self) -> Option<f64> {
                self._pingMs
        }

        pub fn isTimeout(&self) -> bool {
                self._isTimeout
        }

        pub fn new(
                timestamp: DateTime<Utc>,
                ping_ms: Option<f64>,
                _raw_msg: String,
                is_timeout: bool,
        ) -> Self {
                Self {
                        _timestamp: timestamp,
                        _pingMs: ping_ms,
                        _isTimeout: is_timeout,
                }
        }
}

impl PingStats {
        pub fn getPacketLossPct(&self) -> f64 {
                if self.total_sent == 0 {
                        return 0.0;
                }
                (self.total_timeouts as f64 / self.total_sent as f64) * 100.0
        }
}

#[derive(Default)]
pub struct StatsAccumulator {
        stats: PingStats,
        latency_sum: f64,
        jitter_sum: f64,
        jitter_samples: usize,
        previous_latency: Option<f64>,
}

impl StatsAccumulator {
        pub fn push(&mut self, record: &PingRecord) {
                self.stats.total_sent += 1;
                if record._isTimeout {
                        self.stats.total_timeouts += 1;
                        return;
                }

                let Some(ms) = record._pingMs else {
                        return;
                };
                self.stats.total_received += 1;
                self.stats.current_ms = Some(ms);
                self.stats.min_ms =
                        Some(self.stats.min_ms.map_or(ms, |v| v.min(ms)));
                self.stats.max_ms =
                        Some(self.stats.max_ms.map_or(ms, |v| v.max(ms)));
                self.latency_sum += ms;
                self.stats.avg_ms =
                        Some(self.latency_sum
                                / self.stats.total_received as f64);

                if let Some(previous) = self.previous_latency {
                        self.jitter_sum += (ms - previous).abs();
                        self.jitter_samples += 1;
                        self.stats.jitter_ms =
                                Some(self.jitter_sum
                                        / self.jitter_samples as f64);
                }
                self.previous_latency = Some(ms);
        }

        pub fn snapshot(&self) -> PingStats {
                self.stats.clone()
        }
}

impl PingSession {
        pub fn getTarget(&self) -> &str {
                &self._target
        }

        pub fn getResolvedIp(&self) -> IpAddr {
                self._resolvedIp
        }

        pub fn isRunning(&self) -> bool {
                self._running.load(Ordering::Acquire)
        }

        pub fn csvPath(&self) -> &Path {
                &self._csvPath
        }

        pub fn tryRecv(&self) -> Result<SessionEvent, mpsc::TryRecvError> {
                self._events.try_recv()
        }

        pub fn Start(
                target: String,
                resolved_ip: IpAddr,
                interval: Duration,
                ctx: egui::Context,
        ) -> Result<Self, String> {
                let (csv_path, file) = CreateSessionCsv()?;
                let mut writer = BufWriter::new(file);
                if let Err(error) = writeln!(writer, "timestamp,ping")
                        .and_then(|_| writer.flush())
                {
                        drop(writer);
                        let _ = fs::remove_file(&csv_path);
                        return Err(format!(
                                "Failed to initialize session CSV: {error}"
                        ));
                }

                let stop_signal = Arc::new(AtomicBool::new(false));
                let running = Arc::new(AtomicBool::new(true));
                let (event_tx, event_rx) = mpsc::channel();
                let worker_stop = Arc::clone(&stop_signal);
                let worker_running = Arc::clone(&running);

                let handle = thread::spawn(move || {
                        let result = std::panic::catch_unwind(
                                std::panic::AssertUnwindSafe(|| {
                                        ExecutePingLoop(
                                                worker_stop,
                                                &mut writer,
                                                event_tx.clone(),
                                                ctx.clone(),
                                                resolved_ip,
                                                interval,
                                        )
                                }),
                        );

                        if result.is_err() {
                                let _ = event_tx.send(SessionEvent::Error(
                                        "Ping worker panicked unexpectedly."
                                                .to_string(),
                                ));
                                let _ = event_tx
                                        .send(SessionEvent::Finished(Some(
                                        "Ping worker panicked unexpectedly."
                                                .to_string(),
                                )));
                        }
                        worker_running.store(false, Ordering::Release);
                        ctx.request_repaint();
                });

                Ok(Self {
                        _stopSignal: stop_signal,
                        _running: running,
                        _events: event_rx,
                        _handle: Some(handle),
                        _target: target,
                        _resolvedIp: resolved_ip,
                        _csvPath: csv_path,
                })
        }

        pub fn Stop(&mut self) {
                self._stopSignal.store(true, Ordering::Release);
                self._handle.take();
        }
}

impl Drop for PingSession {
        fn drop(&mut self) {
                self.Stop();
        }
}

pub fn ResolveTarget(target: &str) -> Result<(String, IpAddr), String> {
        let trimmed = target.trim();
        if trimmed.is_empty() {
                return Err("Address cannot be empty".to_string());
        }
        if trimmed.len() > 253 {
                return Err(
                        "Address must be 253 characters or fewer".to_string()
                );
        }
        if let Ok(ip) = trimmed.parse::<IpAddr>() {
                return Ok((trimmed.to_string(), ip));
        }

        ValidateTargetSyntax(trimmed)?;
        let addr_spec = format!("{}:0", trimmed);
        let mut addrs = addr_spec.to_socket_addrs().map_err(|e| {
                format!("Failed to resolve '{}': {}", trimmed, e)
        })?;
        let socket_addr = addrs.next().ok_or_else(|| {
                format!("No addresses found for '{}'", trimmed)
        })?;
        Ok((trimmed.to_string(), socket_addr.ip()))
}

pub fn ValidateTargetSyntax(host: &str) -> Result<(), String> {
        if host.is_empty() {
                return Err("Address cannot be empty".to_string());
        }
        if host.len() > 253 {
                return Err(
                        "Address must be 253 characters or fewer".to_string()
                );
        }
        if host.parse::<IpAddr>().is_ok() {
                return Ok(());
        }
        let hostname = host.strip_suffix('.').unwrap_or(host);
        if hostname.is_empty()
                || !hostname.is_ascii()
                || hostname.split('.').any(|label| {
                        label.is_empty()
                                || label.len() > 63
                                || label.starts_with('-')
                                || label.ends_with('-')
                                || !label.bytes().all(|b| {
                                        b.is_ascii_alphanumeric() || b == b'-'
                                })
                })
        {
                return Err("Enter a valid IP address or hostname.".to_string());
        }
        Ok(())
}

pub fn CalculateStats(records: &[PingRecord]) -> PingStats {
        let mut accumulator = StatsAccumulator::default();
        for record in records {
                accumulator.push(record);
        }
        accumulator.snapshot()
}

pub fn ExportRecordsToCsv(
        records: &[PingRecord],
        path: &Path,
) -> io::Result<()> {
        WriteCsvAtomically(path, |writer| {
                writeln!(writer, "timestamp,ping")?;
                for record in records {
                        WriteCsvRecord(writer, record)?;
                }
                Ok(())
        })
}

pub fn ExportSessionCsv(staged_path: &Path, path: &Path) -> io::Result<()> {
        WriteCsvAtomically(path, |writer| {
                let mut source = File::open(staged_path)?;
                io::copy(&mut source, writer)?;
                Ok(())
        })
}

fn WriteCsvAtomically(
        path: &Path,
        write_contents: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
) -> io::Result<()> {
        let temp_path = TemporarySiblingPath(path);
        let result = (|| {
                let file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temp_path)?;
                let mut writer = BufWriter::new(file);
                write_contents(&mut writer)?;
                writer.flush()?;
                writer.get_ref().sync_all()?;
                fs::rename(&temp_path, path)
        })();
        if result.is_err() {
                let _ = fs::remove_file(&temp_path);
        }
        result
}

fn TemporarySiblingPath(path: &Path) -> PathBuf {
        let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("ping-export.csv");
        path.with_file_name(format!(
                ".{file_name}.{}.{}.tmp",
                std::process::id(),
                id
        ))
}

fn CreateSessionCsv() -> Result<(PathBuf, File), String> {
        loop {
                let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                        "project-ping-{}-{id}.csv",
                        std::process::id()
                ));
                match OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)
                {
                        Ok(file) => return Ok((path, file)),
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                                continue;
                        }
                        Err(e) => {
                                return Err(format!(
                                        "Failed to create session log: {e}"
                                ));
                        }
                }
        }
}

fn ExecutePingLoop(
        stop_signal: Arc<AtomicBool>,
        writer: &mut BufWriter<File>,
        events: Sender<SessionEvent>,
        ctx: egui::Context,
        ip: IpAddr,
        interval: Duration,
) {
        #[cfg(windows)]
        ExecuteWindowsPingLoop(stop_signal, writer, events, ctx, ip, interval);

        #[cfg(not(windows))]
        ExecutePingerLoop(stop_signal, writer, events, ctx, ip, interval);
}

#[cfg(not(windows))]
fn ExecutePingerLoop(
        stop_signal: Arc<AtomicBool>,
        writer: &mut BufWriter<File>,
        events: Sender<SessionEvent>,
        ctx: egui::Context,
        ip: IpAddr,
        interval: Duration,
) {
        let ping_opts = PingOptions::new(ip.to_string(), interval, None);
        let rx = match pinger::ping(ping_opts) {
                Ok(rx) => rx,
                Err(e) => {
                        let error = format!("Failed to initiate ping: {e}");
                        let _ = events
                                .send(SessionEvent::Finished(Some(error)));
                        ctx.request_repaint();
                        return;
                }
        };

        let mut exit_error = None;
        let mut log_failed = false;
        loop {
                if stop_signal.load(Ordering::Acquire) {
                        break;
                }
                let ping_res = match rx
                        .recv_timeout(Duration::from_millis(POLL_TIMEOUT_MS))
                {
                        Ok(res) => res,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => {
                                if !stop_signal.load(Ordering::Acquire) {
                                        exit_error = Some(
                                                "Ping result stream disconnected unexpectedly."
                                                        .to_string(),
                                        );
                                }
                                break;
                        }
                };

                if let PingResult::PingExited(status, stderr) = ping_res {
                        let process_error = if status.success() {
                                None
                        } else if stderr.trim().is_empty() {
                                Some(format!("Ping exited with {status}."))
                        } else {
                                Some(format!(
                                        "Ping exited with {status}: {}",
                                        stderr.trim()
                                ))
                        };
                        let error =
                                process_error.or_else(|| exit_error.clone());
                        let _ = events.send(SessionEvent::Finished(error));
                        ctx.request_repaint();
                        return;
                }

                let Some(record) = ConvertPingResult(ping_res) else {
                        continue;
                };
                if !RecordAndPublish(
                        writer,
                        &events,
                        &ctx,
                        record,
                        &mut log_failed,
                        &mut exit_error,
                ) {
                        break;
                }
        }

        let _ = events.send(SessionEvent::Finished(exit_error));
        ctx.request_repaint();
}

#[cfg(windows)]
fn ExecuteWindowsPingLoop(
        stop_signal: Arc<AtomicBool>,
        writer: &mut BufWriter<File>,
        events: Sender<SessionEvent>,
        ctx: egui::Context,
        ip: IpAddr,
        interval: Duration,
) {
        let pinger = match winping::Pinger::new() {
                Ok(pinger) => pinger,
                Err(winping::CreateError::NoV4(pinger)) if ip.is_ipv6() => {
                        pinger
                }
                Err(winping::CreateError::NoV6(pinger)) if ip.is_ipv4() => {
                        pinger
                }
                Err(error) => {
                        let _ = events.send(SessionEvent::Finished(Some(
                                format!("Failed to initialize ICMP: {error}"),
                        )));
                        ctx.request_repaint();
                        return;
                }
        };
        let mut buffer = winping::Buffer::new();
        let mut log_failed = false;
        let mut exit_error = None;

        while !stop_signal.load(Ordering::Acquire) {
                let send_started = Instant::now();
                let timestamp = Utc::now();
                let record = match pinger.send(ip, &mut buffer) {
                        Ok(rtt_ms) => PingRecord::new(
                                timestamp,
                                Some(rtt_ms as f64),
                                String::new(),
                                false,
                        ),
                        Err(_) => PingRecord::new(
                                timestamp,
                                None,
                                String::new(),
                                true,
                        ),
                };
                if !RecordAndPublish(
                        writer,
                        &events,
                        &ctx,
                        record,
                        &mut log_failed,
                        &mut exit_error,
                ) {
                        break;
                }

                let deadline = send_started + interval;
                while !stop_signal.load(Ordering::Acquire) {
                        let remaining = deadline
                                .saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                                break;
                        }
                        thread::sleep(
                                remaining.min(Duration::from_millis(
                                        STOP_POLL_MS,
                                )),
                        );
                }
        }

        let _ = events.send(SessionEvent::Finished(exit_error));
        ctx.request_repaint();
}

fn RecordAndPublish(
        writer: &mut BufWriter<File>,
        events: &Sender<SessionEvent>,
        ctx: &egui::Context,
        record: PingRecord,
        log_failed: &mut bool,
        exit_error: &mut Option<String>,
) -> bool {
        if !*log_failed {
                if let Err(error) = WriteCsvRecord(writer, &record)
                        .and_then(|_| writer.flush())
                {
                        let message =
                                format!("Session CSV write failed: {error}");
                        let _ = events
                                .send(SessionEvent::Error(message.clone()));
                        *exit_error = Some(message);
                        *log_failed = true;
                }
        }
        if events.send(SessionEvent::Record(record)).is_err() {
                return false;
        }
        ctx.request_repaint();
        true
}

#[cfg(not(windows))]
fn ConvertPingResult(res: PingResult) -> Option<PingRecord> {
        match res {
                PingResult::Pong(duration, _) => {
                        let ms = duration.as_secs_f64() * 1000.0;
                        let timestamp = Utc::now()
                                - chrono::Duration::from_std(duration)
                                        .unwrap_or_default();
                        Some(PingRecord::new(
                                timestamp,
                                Some(ms),
                                String::new(),
                                false,
                        ))
                }
                PingResult::Timeout(_) => Some(PingRecord::new(
                        Utc::now(),
                        None,
                        String::new(),
                        true,
                )),
                PingResult::Unknown(_) => Some(PingRecord::new(
                        Utc::now(),
                        None,
                        String::new(),
                        false,
                )),
                PingResult::PingExited(_, _) => None,
        }
}

fn WriteCsvRecord(
        writer: &mut impl Write,
        record: &PingRecord,
) -> io::Result<()> {
        let timestamp = record._timestamp.to_rfc3339();
        if let Some(ms) = record._pingMs {
                writeln!(writer, "{timestamp},{ms:.3}")
        } else {
                writeln!(writer, "{timestamp},timeout")
        }
}

#[cfg(test)]
mod tests {
        use super::*;
        use std::io::Read;

        #[test]
        fn test_resolve_direct_ipv4() {
                assert_eq!(
                        ResolveTarget("127.0.0.1").unwrap().1.to_string(),
                        "127.0.0.1"
                );
        }

        #[test]
        fn test_resolve_direct_ipv6() {
                assert_eq!(ResolveTarget("::1").unwrap().1.to_string(), "::1");
        }

        #[test]
        fn test_resolve_dns_localhost() {
                assert!(ResolveTarget("localhost").is_ok());
        }

        #[test]
        fn test_reject_invalid_hostname_before_dns() {
                assert!(ResolveTarget("-invalid.example").is_err());
                assert!(ResolveTarget(&"a".repeat(254)).is_err());
        }

        #[test]
        fn test_compute_stats() {
                let t0 = Utc::now();
                let records = vec![
                        PingRecord::new(t0, Some(10.0), String::new(), false),
                        PingRecord::new(t0, Some(20.0), String::new(), false),
                        PingRecord::new(t0, None, String::new(), true),
                        PingRecord::new(t0, Some(30.0), String::new(), false),
                ];
                let stats = CalculateStats(&records);
                assert_eq!(stats.total_sent, 4);
                assert_eq!(stats.total_received, 3);
                assert_eq!(stats.total_timeouts, 1);
                assert_eq!(stats.getPacketLossPct(), 25.0);
                assert_eq!(stats.min_ms, Some(10.0));
                assert_eq!(stats.max_ms, Some(30.0));
                assert_eq!(stats.avg_ms, Some(20.0));
                assert_eq!(stats.current_ms, Some(30.0));
                assert_eq!(stats.jitter_ms, Some(10.0));
        }

        #[test]
        fn test_csv_export() {
                let path = std::env::temp_dir().join(format!(
                        "test_export_{}_{}.csv",
                        std::process::id(),
                        TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed)
                ));
                let t0 = Utc::now();
                let records = vec![
                        PingRecord::new(t0, Some(15.234), String::new(), false),
                        PingRecord::new(t0, None, String::new(), true),
                ];
                ExportRecordsToCsv(&records, &path)
                        .expect("CSV export succeeds");
                let mut content = String::new();
                File::open(&path)
                        .unwrap()
                        .read_to_string(&mut content)
                        .unwrap();
                assert!(content.starts_with("timestamp,ping\n"));
                assert!(content.contains(&format!("{:.3}", 15.234)));
                assert!(content.contains("timeout"));
                let _ = fs::remove_file(path);
        }

        #[test]
        fn test_export_staged_session_csv() {
                let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
                let staged = std::env::temp_dir().join(format!(
                        "project-ping-stage-{}-{id}.csv",
                        std::process::id()
                ));
                let exported = std::env::temp_dir().join(format!(
                        "project-ping-export-{}-{id}.csv",
                        std::process::id()
                ));
                fs::write(
                        &staged,
                        "timestamp,ping\n2026-01-01T00:00:00Z,12.000\n",
                )
                .unwrap();

                ExportSessionCsv(&staged, &exported)
                        .expect("staged CSV export succeeds");
                assert_eq!(
                        fs::read_to_string(&exported).unwrap(),
                        "timestamp,ping\n2026-01-01T00:00:00Z,12.000\n"
                );

                let _ = fs::remove_file(staged);
                let _ = fs::remove_file(exported);
        }
}
