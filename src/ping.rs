// =============================================================================
// =============================================================================
// Ping execution engine with DNS resolution, metrics, and CSV export.
// =============================================================================
// =============================================================================

#![allow(non_snake_case)]

// =============================================================================
// Platform and Runtime Libraries
// =============================================================================

use std::fs::File;
use std::io::{self, Write};
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

// =============================================================================
// Third-Party Libraries
// =============================================================================

use chrono::{DateTime, Utc};
use pinger::{PingOptions, PingResult};

// =============================================================================
// Types and Interfaces
// =============================================================================

// Single network ping measurement record.
#[derive(Debug, Clone)]
pub struct PingRecord {
        pub _timestamp: DateTime<Utc>,
        pub _pingMs: Option<f64>,
        pub _rawMsg: String,
        pub _isTimeout: bool,
}

// Aggregated statistical summary for ping measurements.
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

// Active ping session running on a background worker thread.
pub struct PingSession {
        _stopSignal: Arc<AtomicBool>,
        _records: Arc<Mutex<Vec<PingRecord>>>,
        _handle: Option<thread::JoinHandle<()>>,
        _target: String,
        _resolvedIp: IpAddr,
}

// =============================================================================
// Constants and Limits
// =============================================================================

const POLL_TIMEOUT_MS: u64 = 200;

// =============================================================================
// Public Accessors
// =============================================================================

impl PingRecord {
        // Return measurement timestamp.
        pub fn getTimestamp(&self) -> DateTime<Utc> {
                self._timestamp
        }

        // Return measurement latency in milliseconds.
        pub fn getPingMs(&self) -> Option<f64> {
                self._pingMs
        }

        // Return whether this measurement timed out.
        pub fn isTimeout(&self) -> bool {
                self._isTimeout
        }
}

impl PingStats {
        // Calculate packet loss percentage.
        pub fn getPacketLossPct(&self) -> f64 {
                if self.total_sent == 0 {
                        return 0.0;
                }
                (self.total_timeouts as f64 / self.total_sent as f64) * 100.0
        }
}

impl PingSession {
        // Return target hostname or input string.
        pub fn getTarget(&self) -> &str {
                &self._target
        }

        // Return resolved IP address.
        pub fn getResolvedIp(&self) -> IpAddr {
                self._resolvedIp
        }

        // Return whether session worker thread is active.
        pub fn isRunning(&self) -> bool {
                !self._stopSignal.load(Ordering::Relaxed)
        }

        // Return snapshot copy of all recorded measurements.
        pub fn getRecords(&self) -> Vec<PingRecord> {
                self._records
                        .lock()
                        .map(|list| list.clone())
                        .unwrap_or_default()
        }
}

// =============================================================================
// Constructors and Initializers
// =============================================================================

impl PingRecord {
        // Construct a new PingRecord instance.
        pub fn new(
                timestamp: DateTime<Utc>,
                ping_ms: Option<f64>,
                raw_msg: String,
                is_timeout: bool,
        ) -> Self {
                Self {
                        _timestamp: timestamp,
                        _pingMs: ping_ms,
                        _rawMsg: raw_msg,
                        _isTimeout: is_timeout,
                }
        }
}

impl PingSession {
        // Start background ping session for the target address.
        pub fn Start(
                target: String,
                resolved_ip: IpAddr,
                interval: Duration,
        ) -> Result<Self, String> {
                let stop_signal = Arc::new(AtomicBool::new(false));
                let records = Arc::new(Mutex::new(Vec::new()));

                let stop_clone = Arc::clone(&stop_signal);
                let records_clone = Arc::clone(&records);
                let ip_str = resolved_ip.to_string();

                let ping_opts = PingOptions::new(ip_str, interval, None);
                let rx = pinger::ping(ping_opts).map_err(|e| {
                        format!("Failed to initiate ping: {}", e)
                })?;

                let handle = thread::spawn(move || {
                        ExecutePingLoop(stop_clone, records_clone, rx);
                });

                Ok(Self {
                        _stopSignal: stop_signal,
                        _records: records,
                        _handle: Some(handle),
                        _target: target,
                        _resolvedIp: resolved_ip,
                })
        }
}

// =============================================================================
// Primary Operations
// =============================================================================

impl PingSession {
        // Stop background ping session and wait for worker thread.
        pub fn Stop(&mut self) {
                self._stopSignal.store(true, Ordering::Relaxed);
                if let Some(h) = self._handle.take() {
                        let _ = h.join();
                }
        }

        // Calculate statistics for all session measurements.
        pub fn CalculateStats(&self) -> PingStats {
                let records = self.getRecords();
                CalculateStats(&records)
        }
}

impl Drop for PingSession {
        // Ensure worker thread is stopped when session drops.
        fn drop(&mut self) {
                self.Stop();
        }
}

// Resolve user input string to valid IP address through DNS or parser.
pub fn ResolveTarget(target: &str) -> Result<(String, IpAddr), String> {
        let trimmed = target.trim();
        if trimmed.is_empty() {
                return Err("Address cannot be empty".to_string());
        }

        if let Ok(ip) = trimmed.parse::<IpAddr>() {
                return Ok((trimmed.to_string(), ip));
        }

        let addr_spec = format!("{}:0", trimmed);
        let mut addrs = addr_spec.to_socket_addrs().map_err(|e| {
                format!("Failed to resolve '{}': {}", trimmed, e)
        })?;

        let socket_addr = addrs.next().ok_or_else(|| {
                format!("No addresses found for '{}'", trimmed)
        })?;

        Ok((trimmed.to_string(), socket_addr.ip()))
}

// Calculate statistical metrics from ping records.
pub fn CalculateStats(records: &[PingRecord]) -> PingStats {
        let mut stats = PingStats::default();
        stats.total_sent = records.len();
        if records.is_empty() {
                return stats;
        }

        let mut valid_pings: Vec<f64> = Vec::new();
        for r in records {
                if r._isTimeout {
                        stats.total_timeouts += 1;
                } else if let Some(ms) = r._pingMs {
                        stats.total_received += 1;
                        valid_pings.push(ms);
                }
        }

        PopulateValidStats(&mut stats, &valid_pings);
        stats
}

// Export ping records to comma-separated values file.
pub fn ExportRecordsToCsv(
        records: &[PingRecord],
        path: &Path,
) -> io::Result<()> {
        let mut file = File::create(path)?;
        writeln!(file, "timestamp,ping")?;

        for record in records {
                let ts_str = record._timestamp.to_rfc3339();
                if let Some(ms) = record._pingMs {
                        writeln!(file, "{},{:.3}", ts_str, ms)?;
                } else {
                        writeln!(file, "{},timeout", ts_str)?;
                }
        }

        file.flush()?;
        Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

// Run background loop to poll ping results and store records.
fn ExecutePingLoop(
        stop_signal: Arc<AtomicBool>,
        records: Arc<Mutex<Vec<PingRecord>>>,
        rx: mpsc::Receiver<PingResult>,
) {
        let poll_dur = Duration::from_millis(POLL_TIMEOUT_MS);
        while !stop_signal.load(Ordering::Relaxed) {
                let ping_res = match rx.recv_timeout(poll_dur) {
                        Ok(res) => res,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };

                let maybe_record = ConvertPingResult(ping_res);
                let Some(record) = maybe_record else {
                        break;
                };
                AppendRecord(&records, record);
        }
}

// Convert pinger result enum to PingRecord struct.
fn ConvertPingResult(res: PingResult) -> Option<PingRecord> {
        let now = Utc::now();
        match res {
                PingResult::Pong(duration, line) => {
                        let ms = duration.as_secs_f64() * 1000.0;
                        Some(PingRecord::new(now, Some(ms), line, false))
                }
                PingResult::Timeout(line) => {
                        Some(PingRecord::new(now, None, line, true))
                }
                PingResult::Unknown(line) => {
                        Some(PingRecord::new(now, None, line, false))
                }
                PingResult::PingExited(_, _) => None,
        }
}

// Append new record safely to thread-safe vector.
fn AppendRecord(records: &Arc<Mutex<Vec<PingRecord>>>, record: PingRecord) {
        if let Ok(mut list) = records.lock() {
                list.push(record);
        }
}

// Populate statistical metrics from valid ping values.
fn PopulateValidStats(stats: &mut PingStats, valid_pings: &[f64]) {
        if valid_pings.is_empty() {
                return;
        }

        let min_val = valid_pings.iter().copied().fold(f64::INFINITY, f64::min);
        let max_val = valid_pings
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
        let sum_val: f64 = valid_pings.iter().sum();
        let count = valid_pings.len() as f64;

        stats.current_ms = valid_pings.last().copied();
        stats.min_ms = Some(min_val);
        stats.max_ms = Some(max_val);
        stats.avg_ms = Some(sum_val / count);
        stats.jitter_ms = CalculateJitter(valid_pings);
}

// Calculate mean jitter from consecutive latency samples.
fn CalculateJitter(valid_pings: &[f64]) -> Option<f64> {
        if valid_pings.len() < 2 {
                return None;
        }

        let sum_diff: f64 =
                valid_pings.windows(2).map(|w| (w[1] - w[0]).abs()).sum();
        let count = (valid_pings.len() - 1) as f64;
        Some(sum_diff / count)
}

// =============================================================================
// Format Conversions and Tests
// =============================================================================

#[cfg(test)]
mod tests {
        use super::*;
        use std::io::Read;

        #[test]
        fn test_resolve_direct_ipv4() {
                let res = ResolveTarget("127.0.0.1");
                assert!(res.is_ok());
                let (_, ip) = res.unwrap();
                assert_eq!(ip.to_string(), "127.0.0.1");
        }

        #[test]
        fn test_resolve_direct_ipv6() {
                let res = ResolveTarget("::1");
                assert!(res.is_ok());
                let (_, ip) = res.unwrap();
                assert_eq!(ip.to_string(), "::1");
        }

        #[test]
        fn test_resolve_dns_localhost() {
                let res = ResolveTarget("localhost");
                assert!(res.is_ok());
        }

        #[test]
        fn test_resolve_invalid_empty() {
                let res = ResolveTarget("   ");
                assert!(res.is_err());
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
        }

        #[test]
        fn test_csv_export() {
                let dir = std::env::temp_dir();
                let path = dir.join("test_export.csv");

                let t0 = Utc::now();
                let records = vec![
                        PingRecord::new(t0, Some(15.234), String::new(), false),
                        PingRecord::new(t0, None, String::new(), true),
                ];

                ExportRecordsToCsv(&records, &path)
                        .expect("export should succeed");

                let mut content = String::new();
                File::open(&path)
                        .unwrap()
                        .read_to_string(&mut content)
                        .unwrap();

                assert!(content.starts_with("timestamp,ping\n"));
                assert!(content.contains(&format!("{:.3}", 15.234)));
                assert!(content.contains("timeout"));

                let _ = std::fs::remove_file(path);
        }
}
