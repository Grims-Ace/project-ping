// =============================================================================
// =============================================================================
// Graphical interface implementation for Project Ping latency monitor.
// =============================================================================
// =============================================================================

#![allow(non_snake_case)]

// =============================================================================
// Platform and Runtime Libraries
// =============================================================================

use std::collections::VecDeque;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::Duration;

// =============================================================================
// Third-Party Libraries
// =============================================================================

use chrono::{DateTime, Local, Utc};
use eframe::egui::{self, Color32, FontId, RichText, Stroke, Vec2};
use egui_plot::{
        Legend, Line, Plot, PlotBounds, PlotPoint, PlotPoints, Points,
};

// =============================================================================
// Domain Modules
// =============================================================================

use crate::ping::{
        ExportSessionCsv, PingRecord, PingSession, PingStats, ResolveTarget,
        SessionEvent, StatsAccumulator, ValidateTargetSyntax,
};

// =============================================================================
// Types and Interfaces
// =============================================================================

// Lifecycle states of the ping application.
#[derive(Debug, PartialEq, Eq)]
enum AppState {
        Config,
        Running,
        Stopped,
}

// Main application state container for the graphical interface.
pub struct PingApp {
        _targetInput: String,
        _resolvedIp: Option<IpAddr>,
        _resolutionError: Option<String>,
        _intervalSecs: f64,
        _state: AppState,
        _session: Option<PingSession>,
        _startTimeUtc: Option<DateTime<Utc>>,
        _stopElapsedSecs: Option<f64>,
        _collectedRecords: VecDeque<PingRecord>,
        _totalRecords: usize,
        _lastStats: PingStats,
        _statsAccumulator: StatsAccumulator,
        _latencyPoints: VecDeque<PlotPoint>,
        _timeoutPoints: VecDeque<PlotPoint>,
        _maxPing: f64,
        _resolutionRx: Option<Receiver<Result<(String, IpAddr), String>>>,
        _resolvingTarget: Option<String>,
        _pendingStart: bool,
        _workerError: Option<String>,
        _saveStatusMsg: Option<(String, bool)>,
        _lastSavedPath: Option<PathBuf>,
}

// =============================================================================
// Constants and Limits
// =============================================================================

const DEFAULT_TARGET: &str = "1.1.1.1";
const DEFAULT_INTERVAL_SECS: f64 = 1.0;
const MIN_INTERVAL_SECS: f64 = 0.5;
const MAX_INTERVAL_SECS: f64 = 60.0;
const STEP_INTERVAL_SECS: f64 = 0.5;
const DEFAULT_GRID_TIME_SECS: f64 = 60.0;
const DEFAULT_GRID_PING_MS: f64 = 100.0;
const MAX_LIVE_RECORDS: usize = 10_000;
const MAX_PLOT_POINTS: usize = 5_000;

// =============================================================================
// Constructors and Initializers
// =============================================================================

impl Default for PingApp {
        // Create default application configuration.
        fn default() -> Self {
                Self {
                        _targetInput: DEFAULT_TARGET.to_string(),
                        _resolvedIp: Some(DEFAULT_TARGET.parse().unwrap()),
                        _resolutionError: None,
                        _intervalSecs: DEFAULT_INTERVAL_SECS,
                        _state: AppState::Config,
                        _session: None,
                        _startTimeUtc: None,
                        _stopElapsedSecs: None,
                        _collectedRecords: VecDeque::new(),
                        _totalRecords: 0,
                        _lastStats: PingStats::default(),
                        _statsAccumulator: StatsAccumulator::default(),
                        _latencyPoints: VecDeque::new(),
                        _timeoutPoints: VecDeque::new(),
                        _maxPing: 0.0,
                        _resolutionRx: None,
                        _resolvingTarget: None,
                        _pendingStart: false,
                        _workerError: None,
                        _saveStatusMsg: None,
                        _lastSavedPath: None,
                }
        }
}

impl PingApp {
        // Construct application instance for eframe initialization.
        pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
                Self::default()
        }
}

// =============================================================================
// Primary Operations
// =============================================================================

impl eframe::App for PingApp {
        // Render application user interface frame.
        fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
                self.RefreshSessionData(ui.ctx());

                egui::Frame::central_panel(ui.style()).show(ui, |ui| {
                        RenderHeader(ui);
                        ui.separator();

                        match self._state {
                                AppState::Config => {
                                        self.RenderConfigView(ui);
                                }
                                AppState::Running => {
                                        self.RenderRunningView(ui);
                                }
                                AppState::Stopped => {
                                        self.RenderStoppedView(ui);
                                }
                        }
                });
        }
}

impl PingApp {
        fn RefreshSessionData(&mut self, ctx: &egui::Context) {
                if let Some(receiver) = &self._resolutionRx {
                        match receiver.try_recv() {
                                Ok(result) => {
                                        self._resolutionRx = None;
                                        let target =
                                                self._resolvingTarget.take();
                                        if target.as_deref()
                                                != Some(self
                                                        ._targetInput
                                                        .trim())
                                        {
                                                self._pendingStart = false;
                                        } else {
                                                match result {
                                                        Ok((_, ip)) => {
                                                                self._resolvedIp =
                                                                        Some(ip);
                                                                self._resolutionError =
                                                                        None;
                                                                if self._pendingStart {
                                                                        self._pendingStart =
                                                                                false;
                                                                        self.StartResolvedTest(
                                                                                ip,
                                                                                ctx.clone(),
                                                                        );
                                                                }
                                                        }
                                                        Err(error) => {
                                                                self._resolvedIp =
                                                                        None;
                                                                self._resolutionError =
                                                                        Some(error);
                                                                self._pendingStart =
                                                                        false;
                                                        }
                                                }
                                        }
                                }
                                Err(TryRecvError::Disconnected) => {
                                        self._resolutionRx = None;
                                        self._resolvingTarget = None;
                                        self._pendingStart = false;
                                        self._resolutionError = Some(
                                                "Address resolution worker stopped unexpectedly."
                                                        .to_string(),
                                        );
                                }
                                Err(TryRecvError::Empty) => {}
                        }
                }

                let mut events = Vec::new();
                if let Some(session) = &self._session {
                        loop {
                                match session.tryRecv() {
                                        Ok(event) => events.push(event),
                                        Err(TryRecvError::Empty)
                                        | Err(TryRecvError::Disconnected) => {
                                                break;
                                        }
                                }
                        }
                }
                for event in events {
                        match event {
                                SessionEvent::Record(record) => {
                                        self._totalRecords += 1;
                                        self._statsAccumulator.push(&record);
                                        self._lastStats = self
                                                ._statsAccumulator
                                                .snapshot();
                                        let start = self
                                                ._startTimeUtc
                                                .unwrap_or_else(Utc::now);
                                        let elapsed = (record.getTimestamp()
                                                - start)
                                                .num_milliseconds()
                                                as f64
                                                / 1000.0;
                                        if let Some(ms) = record.getPingMs() {
                                                self._maxPing =
                                                        self._maxPing.max(ms);
                                                PushBounded(
                                                        &mut self
                                                                ._latencyPoints,
                                                        PlotPoint::new(
                                                                elapsed.max(
                                                                        0.0,
                                                                ),
                                                                ms,
                                                        ),
                                                );
                                        } else if record.isTimeout() {
                                                PushBounded(
                                                        &mut self
                                                                ._timeoutPoints,
                                                        PlotPoint::new(
                                                                elapsed.max(
                                                                        0.0,
                                                                ),
                                                                0.0,
                                                        ),
                                                );
                                        }
                                        if self._collectedRecords.len()
                                                == MAX_LIVE_RECORDS
                                        {
                                                self._collectedRecords
                                                        .pop_front();
                                        }
                                        self._collectedRecords
                                                .push_back(record);
                                }
                                SessionEvent::Error(error) => {
                                        self._workerError = Some(error);
                                }
                                SessionEvent::Finished(error) => {
                                        if let Some(error) = error {
                                                self._workerError = Some(error);
                                        }
                                        if self._state == AppState::Running {
                                                self.SetStoppedTime();
                                                self._state = AppState::Stopped;
                                        }
                                }
                        }
                }

                if self._state == AppState::Running {
                        ctx.request_repaint_after(Duration::from_secs(1));
                }
        }

        fn StartTest(&mut self, ctx: &egui::Context) {
                self._pendingStart = true;
                self._resolvedIp = None;
                self.TryResolve(ctx);
        }

        fn StartResolvedTest(&mut self, ip: IpAddr, ctx: egui::Context) {
                self._collectedRecords.clear();
                self._totalRecords = 0;
                self._statsAccumulator = StatsAccumulator::default();
                self._lastStats = PingStats::default();
                self._latencyPoints.clear();
                self._timeoutPoints.clear();
                self._maxPing = 0.0;
                self._workerError = None;
                self._saveStatusMsg = None;
                self._lastSavedPath = None;
                self._stopElapsedSecs = None;

                let dur = Duration::from_secs_f64(self._intervalSecs);
                let target = self._targetInput.trim().to_string();
                match PingSession::Start(target, ip, dur, ctx) {
                        Ok(session) => {
                                self._session = Some(session);
                                self._startTimeUtc = Some(Utc::now());
                                self._state = AppState::Running;
                        }
                        Err(error) => {
                                self._workerError = Some(error);
                                self._state = AppState::Stopped;
                        }
                }
        }

        fn StopTest(&mut self) {
                if let Some(session) = &mut self._session {
                        session.Stop();
                }
                self.SetStoppedTime();
                self._state = AppState::Stopped;
        }

        fn SetStoppedTime(&mut self) {
                if self._stopElapsedSecs.is_some() {
                        return;
                }
                let start_utc = self._startTimeUtc.unwrap_or_else(Utc::now);
                let diff = Utc::now() - start_utc;
                let elapsed = diff.num_milliseconds() as f64 / 1000.0;
                self._stopElapsedSecs = Some(elapsed);
        }

        fn SaveCsv(&mut self) {
                if self._collectedRecords.is_empty() {
                        let err_text = "No ping records to save.".to_string();
                        self._saveStatusMsg = Some((err_text, false));
                        return;
                }

                let file_name = FormatDefaultCsvName(&self._targetInput);
                let dialog = rfd::FileDialog::new()
                        .set_file_name(&file_name)
                        .add_filter("CSV files (*.csv)", &["csv"]);

                let Some(path) = dialog.save_file() else {
                        return;
                };
                if self._session.as_ref().is_some_and(PingSession::isRunning) {
                        self._saveStatusMsg = Some((
                                "Wait for the session log to finish before exporting."
                                        .to_string(),
                                false,
                        ));
                        return;
                }
                let path = EnsureCsvExtension(path);
                let result = self
                        ._session
                        .as_ref()
                        .ok_or_else(|| {
                                std::io::Error::other(
                                        "Session log is unavailable",
                                )
                        })
                        .and_then(|session| {
                                ExportSessionCsv(session.csvPath(), &path)
                        });
                match result {
                        Ok(_) => {
                                let msg = format!(
                                        "Saved {} rows to:\n{}",
                                        self._totalRecords,
                                        path.display()
                                );
                                self._lastSavedPath = Some(path);
                                self._saveStatusMsg = Some((msg, true));
                        }
                        Err(e) => {
                                let err_msg = format!("CSV error: {}", e);
                                self._saveStatusMsg = Some((err_msg, false));
                        }
                }
        }

        fn TryResolve(&mut self, ctx: &egui::Context) {
                let target = self._targetInput.trim().to_string();
                if let Err(error) = ValidateTargetSyntax(&target) {
                        self._resolvedIp = None;
                        self._resolutionError = Some(error);
                        self._pendingStart = false;
                        return;
                }
                let (tx, rx) = mpsc::channel();
                let repaint_ctx = ctx.clone();
                let worker_target = target.clone();
                thread::spawn(move || {
                        let _ = tx.send(ResolveTarget(&worker_target));
                        repaint_ctx.request_repaint();
                });
                self._resolutionRx = Some(rx);
                self._resolvingTarget = Some(target);
                self._resolutionError = None;
        }
}

// =============================================================================
// Helper Functions - UI View Components
// =============================================================================

// Render header title and description.
fn RenderHeader(ui: &mut egui::Ui) {
        ui.vertical_centered(|ui| {
                ui.add_space(8.0);
                ui.heading(RichText::new("Project Ping").size(24.0).strong());
                ui.label(RichText::new(
                        "A latency monitor, with dns resolution and csv logging.",
                )
                .size(13.0)
                .color(Color32::from_gray(180)));
                ui.add_space(8.0);
        });
}

impl PingApp {
        // Render centered target configuration card and run button.
        fn RenderConfigView(&mut self, ui: &mut egui::Ui) {
                ui.add_space(20.0);
                ui.vertical_centered(|ui| {
                        let card_width = 720.0;
                        ui.allocate_ui(Vec2::new(card_width, 0.0), |ui| {
                                egui::Frame::group(ui.style())
                                        .inner_margin(24.0)
                                        .show(ui, |ui| {
                                                self.RenderCardContents(ui);
                                        });
                        });

                        ui.add_space(24.0);
                        self.RenderBigRunButton(ui);
                });
        }

        // Render contents of configuration card with 2x text scale.
        fn RenderCardContents(&mut self, ui: &mut egui::Ui) {
                let title = RichText::new("Target Configuration")
                        .size(36.0)
                        .strong();
                ui.label(title);
                ui.add_space(12.0);

                self.RenderTargetInputRow(ui);
                ui.add_space(10.0);

                self.RenderPresetsRow(ui);
                ui.add_space(12.0);

                self.RenderDnsStatusRow(ui);
                ui.add_space(16.0);

                self.RenderSliderRow(ui);
        }

        // Render target address input field and manual resolve trigger.
        fn RenderTargetInputRow(&mut self, ui: &mut egui::Ui) {
                let prompt = RichText::new(
                        "Enter an IP address or hostname to test:",
                )
                .size(28.0);
                ui.label(prompt);
                ui.add_space(4.0);

                ui.horizontal(|ui| {
                        let font = FontId::proportional(26.0);
                        let text_edit = egui::TextEdit::singleline(
                                &mut self._targetInput,
                        )
                        .font(font)
                        .char_limit(253)
                        .hint_text("e.g. 1.1.1.1, 8.8.8.8");

                        let text_box = ui.add_sized([440.0, 48.0], text_edit);
                        if text_box.changed() {
                                self._resolvedIp = None;
                                self._resolutionError = None;
                                self._resolutionRx = None;
                                self._resolvingTarget = None;
                                self._pendingStart = false;
                        }

                        let btn_text = RichText::new("Resolve DNS").size(24.0);
                        let btn = egui::Button::new(btn_text)
                                .min_size(Vec2::new(180.0, 48.0));
                        if ui.add_enabled(self._resolutionRx.is_none(), btn)
                                .clicked()
                        {
                                self.TryResolve(ui.ctx());
                        }
                });
        }

        // Render quick preset selection buttons.
        fn RenderPresetsRow(&mut self, ui: &mut egui::Ui) {
                ui.horizontal_wrapped(|ui| {
                        let label = RichText::new("Quick Presets:").size(26.0);
                        ui.label(label);
                        let presets = [
                                ("Cloudflare (1.1.1.1)", "1.1.1.1"),
                                ("Google (8.8.8.8)", "8.8.8.8"),
                                ("Localhost (127.0.0.1)", "127.0.0.1"),
                        ];
                        for (name, host) in presets {
                                let btn_text = RichText::new(name).size(22.0);
                                if ui.button(btn_text).clicked() {
                                        self._targetInput = host.to_string();
                                        self._resolvedIp = None;
                                        self._resolutionError = None;
                                        self._resolutionRx = None;
                                        self._resolvingTarget = None;
                                        self._pendingStart = false;
                                }
                        }
                });
        }

        // Render DNS validation result message.
        fn RenderDnsStatusRow(&self, ui: &mut egui::Ui) {
                if self._resolutionRx.is_some() {
                        ui.label(RichText::new("Resolving address...")
                                .size(24.0));
                } else if let Some(ip) = self._resolvedIp {
                        ui.horizontal(|ui| {
                                let green = Color32::from_rgb(50, 200, 80);
                                let prefix = RichText::new("Resolved IP:")
                                        .size(26.0)
                                        .color(green)
                                        .strong();
                                ui.label(prefix);

                                let ip_text = RichText::new(ip.to_string())
                                        .size(26.0)
                                        .strong()
                                        .monospace();
                                ui.label(ip_text);
                        });
                } else if let Some(ref err) = self._resolutionError {
                        let red = Color32::from_rgb(255, 100, 100);
                        let err_text = RichText::new(err).size(24.0).color(red);
                        ui.label(err_text);
                }
        }

        // Render ping interval duration slider.
        fn RenderSliderRow(&mut self, ui: &mut egui::Ui) {
                ui.horizontal(|ui| {
                        let label = RichText::new("Ping Interval:").size(26.0);
                        ui.label(label);
                        let range = MIN_INTERVAL_SECS..=MAX_INTERVAL_SECS;
                        let slider = egui::Slider::new(
                                &mut self._intervalSecs,
                                range,
                        )
                        .step_by(STEP_INTERVAL_SECS)
                        .suffix(" s");
                        ui.add(slider);
                });
        }

        // Render large prominent start test button.
        fn RenderBigRunButton(&mut self, ui: &mut egui::Ui) {
                let can_run = !self._targetInput.trim().is_empty()
                        && self._resolutionRx.is_none();
                let btn_text = RichText::new("RUN PING TEST")
                        .size(24.0)
                        .color(Color32::WHITE)
                        .strong();

                let green = Color32::from_rgb(46, 125, 50);
                let btn = egui::Button::new(btn_text)
                        .min_size(Vec2::new(320.0, 60.0))
                        .fill(green);

                if ui.add_enabled(can_run, btn).clicked() {
                        self.StartTest(ui.ctx());
                }
        }

        // Render active test status bar, stop button, and plot.
        fn RenderRunningView(&mut self, ui: &mut egui::Ui) {
                ui.horizontal(|ui| {
                        self.RenderRunningStatusText(ui);
                        self.RenderStopButton(ui);
                });

                ui.add_space(8.0);
                self.RenderStatsCards(ui);
                ui.add_space(8.0);
                self.RenderPlot(ui);
        }

        // Render target info and sample count during active run.
        fn RenderRunningStatusText(&self, ui: &mut egui::Ui) {
                ui.vertical(|ui| {
                        ui.horizontal(|ui| {
                                ui.label(RichText::new("Target:").strong());
                                let text_bold =
                                        RichText::new(&self._targetInput)
                                                .monospace()
                                                .strong();
                                ui.label(text_bold);
                                if let Some(ip) = self._resolvedIp {
                                        let gray = Color32::GRAY;
                                        let text = format!("({})", ip);
                                        ui.label(
                                                RichText::new(text).color(gray)
                                        );
                                }
                        });
                        ui.horizontal(|ui| {
                                let green = Color32::from_rgb(50, 205, 50);
                                ui.label(RichText::new("TEST RUNNING")
                                        .color(green)
                                        .strong());
                                let info = format!(
                                        "Samples: {} | Interval: {:.1}s",
                                        self._totalRecords, self._intervalSecs,
                                );
                                ui.label(info);
                        });
                        if let Some(error) = &self._workerError {
                                ui.label(RichText::new(error).color(
                                        Color32::from_rgb(255, 100, 100),
                                ));
                        }
                        if let Some(session) = &self._session {
                                ui.small(format!(
                                        "Live CSV log: {}",
                                        session.csvPath().display()
                                ));
                        }
                });
        }

        // Render red stop button to halt test.
        fn RenderStopButton(&mut self, ui: &mut egui::Ui) {
                let right_layout =
                        egui::Layout::right_to_left(egui::Align::Center);
                ui.with_layout(right_layout, |ui| {
                        let btn_text = RichText::new("STOP TEST")
                                .size(16.0)
                                .color(Color32::WHITE)
                                .strong();

                        let red = Color32::from_rgb(198, 40, 40);
                        let stop_btn = egui::Button::new(btn_text)
                                .min_size(Vec2::new(160.0, 42.0))
                                .fill(red);

                        if ui.add(stop_btn).clicked() {
                                self.StopTest();
                        }
                });
        }

        // Render stopped test summary, save option, and plot.
        fn RenderStoppedView(&mut self, ui: &mut egui::Ui) {
                self.RenderStoppedBanner(ui);
                ui.add_space(8.0);
                self.RenderStatsCards(ui);
                ui.add_space(8.0);
                self.RenderPlot(ui);
        }

        // Render stopped session controls and CSV save options.
        fn RenderStoppedBanner(&mut self, ui: &mut egui::Ui) {
                let bg_color = Color32::from_rgb(30, 30, 35);
                egui::Frame::group(ui.style())
                        .fill(bg_color)
                        .inner_margin(12.0)
                        .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                        self.RenderStoppedSummaryText(ui);
                                        self.RenderStoppedActionButtons(ui);
                                });
                                self.RenderSaveFeedback(ui);
                        });
        }

        // Render text summary for completed test.
        fn RenderStoppedSummaryText(&self, ui: &mut egui::Ui) {
                ui.vertical(|ui| {
                        let orange = Color32::from_rgb(255, 180, 50);
                        ui.label(RichText::new("Test Stopped")
                                .size(16.0)
                                .color(orange)
                                .strong());
                        let count_text = format!(
                                "Target: {} ({} samples captured)",
                                self._targetInput, self._totalRecords
                        );
                        ui.label(count_text);
                        if let Some(error) = &self._workerError {
                                ui.label(RichText::new(error).color(
                                        Color32::from_rgb(255, 100, 100),
                                ));
                        }
                        if let Some(session) = &self._session {
                                ui.small(format!(
                                        "Live CSV log: {}",
                                        session.csvPath().display()
                                ));
                        }
                });
        }

        // Render restart and CSV export buttons in stopped banner.
        fn RenderStoppedActionButtons(&mut self, ui: &mut egui::Ui) {
                let right_layout =
                        egui::Layout::right_to_left(egui::Align::Center);
                ui.with_layout(right_layout, |ui| {
                        if ui.button("New Test").clicked() {
                                self._state = AppState::Config;
                        }

                        let save_text = RichText::new("Save CSV Data")
                                .size(15.0)
                                .color(Color32::WHITE)
                                .strong();

                        let blue = Color32::from_rgb(33, 150, 243);
                        let save_btn = egui::Button::new(save_text)
                                .min_size(Vec2::new(170.0, 36.0))
                                .fill(blue);

                        let export_ready = !self
                                ._session
                                .as_ref()
                                .is_some_and(PingSession::isRunning);
                        if ui.add_enabled(export_ready, save_btn).clicked() {
                                self.SaveCsv();
                        }
                });
        }

        // Render notification after CSV file save.
        fn RenderSaveFeedback(&self, ui: &mut egui::Ui) {
                let Some((ref msg, success)) = self._saveStatusMsg else {
                        return;
                };

                ui.add_space(6.0);
                let color = if success {
                        Color32::from_rgb(100, 220, 100)
                } else {
                        Color32::from_rgb(255, 100, 100)
                };
                ui.label(RichText::new(msg).color(color).strong());
        }

        // Render horizontal row of metric summary cards.
        fn RenderStatsCards(&self, ui: &mut egui::Ui) {
                ui.horizontal_wrapped(|ui| {
                        let cur_str = FormatMs(self._lastStats.current_ms);
                        RenderStatCard(ui, "Current", &cur_str, Color32::WHITE);

                        let min_str = FormatMs(self._lastStats.min_ms);
                        let green = Color32::from_rgb(100, 220, 120);
                        RenderStatCard(ui, "Min", &min_str, green);

                        let avg_str = FormatMs(self._lastStats.avg_ms);
                        let blue = Color32::from_rgb(100, 180, 255);
                        RenderStatCard(ui, "Average", &avg_str, blue);

                        let max_str = FormatMs(self._lastStats.max_ms);
                        let orange = Color32::from_rgb(255, 140, 90);
                        RenderStatCard(ui, "Max", &max_str, orange);

                        let jit_str = FormatMs(self._lastStats.jitter_ms);
                        RenderStatCard(ui, "Jitter", &jit_str, Color32::WHITE);

                        let loss = self._lastStats.getPacketLossPct();
                        let loss_str = format!("{:.1}%", loss);
                        let loss_color = if loss > 5.0 {
                                Color32::from_rgb(255, 80, 80)
                        } else {
                                Color32::from_rgb(120, 220, 120)
                        };
                        RenderStatCard(ui, "Loss", &loss_str, loss_color);
                });
        }

        // Render interactive latency graph pinned at (0, 0) with autoscaling.
        fn RenderPlot(&mut self, ui: &mut egui::Ui) {
                let avail_h = ui.available_height().max(250.0);
                let start_utc = self._startTimeUtc.unwrap_or_else(Utc::now);

                let elapsed_now = match self._stopElapsedSecs {
                        Some(secs) => secs,
                        None => {
                                let diff = Utc::now() - start_utc;
                                diff.num_milliseconds() as f64 / 1000.0
                        }
                };

                let x_max = elapsed_now.max(DEFAULT_GRID_TIME_SECS);
                let y_max = (self._maxPing * 1.05).max(DEFAULT_GRID_PING_MS);
                let latency_points = self._latencyPoints.make_contiguous();
                let timeout_points = self._timeoutPoints.make_contiguous();
                let has_latency = !latency_points.is_empty();
                let has_timeouts = !timeout_points.is_empty();

                let plot = Plot::new("latency_plot")
                        .legend(Legend::default())
                        .x_axis_label("Elapsed Time (seconds)")
                        .y_axis_label("Latency (ms)")
                        .height(avail_h)
                        .show_x(false)
                        .show_y(false)
                        .show_crosshair(false)
                        .allow_zoom(false)
                        .allow_drag(false)
                        .allow_scroll(false)
                        .show_grid(true);

                plot.show(ui, |plot_ui| {
                        let bounds = PlotBounds::from_min_max(
                                [0.0, 0.0],
                                [x_max, y_max],
                        );
                        plot_ui.set_plot_bounds(bounds);
                        if has_latency {
                                DrawLatencySeries(plot_ui, latency_points);
                        }
                        if has_timeouts {
                                DrawTimeoutSeries(plot_ui, timeout_points);
                        }
                });
        }
}

// Render individual statistic metric card.
fn RenderStatCard(
        ui: &mut egui::Ui,
        title: &str,
        value: &str,
        val_color: Color32,
) {
        let card_size = Vec2::new(125.0, 50.0);
        egui::Frame::group(ui.style())
                .inner_margin(6.0)
                .show(ui, |ui| {
                        ui.set_min_size(card_size);
                        let gray = Color32::GRAY;
                        ui.label(RichText::new(title).size(11.0).color(gray));
                        ui.label(RichText::new(value)
                                .size(16.0)
                                .strong()
                                .color(val_color));
                });
}

// Draw line and sample dots for valid latency measurements.
fn DrawLatencySeries<'a>(
        plot_ui: &mut egui_plot::PlotUi<'a>,
        pts: &'a [PlotPoint],
) {
        let blue = Color32::from_rgb(33, 150, 243);
        let line = Line::new("Ping Latency (ms)", PlotPoints::Borrowed(pts))
                .color(blue)
                .stroke(Stroke::new(2.0, blue));
        plot_ui.line(line);

        let light_blue = Color32::from_rgb(100, 200, 255);
        let dots = Points::new("Samples", PlotPoints::Borrowed(pts))
                .radius(3.0)
                .color(light_blue);
        plot_ui.points(dots);
}

// Draw red indicator points along baseline for timed-out packets.
fn DrawTimeoutSeries<'a>(
        plot_ui: &mut egui_plot::PlotUi<'a>,
        timeouts: &'a [PlotPoint],
) {
        let red = Color32::from_rgb(255, 60, 60);
        let points =
                Points::new("Timeouts (Lost)", PlotPoints::Borrowed(timeouts))
                        .radius(4.5)
                        .color(red);
        plot_ui.points(points);
}

// Format optional millisecond latency value to display string.
fn FormatMs(val: Option<f64>) -> String {
        match val {
                Some(ms) => format!("{:.1} ms", ms),
                None => "--".to_string(),
        }
}

// Format default file name for CSV export with timestamp.
fn FormatDefaultCsvName(target: &str) -> String {
        let trimmed = target.trim();
        let clean_target: String = trimmed
                .chars()
                .map(|ch| {
                        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'
                        {
                                ch
                        } else {
                                '_'
                        }
                })
                .collect();
        let clean_target = clean_target.trim_matches('_');
        let now_str = Local::now().format("%Y%m%d_%H%M%S");
        format!("ping_{}_{}.csv", clean_target, now_str)
}

fn EnsureCsvExtension(mut path: PathBuf) -> PathBuf {
        path.set_extension("csv");
        path
}

fn PushBounded<T>(items: &mut VecDeque<T>, item: T) {
        if items.len() == MAX_PLOT_POINTS {
                items.pop_front();
        }
        items.push_back(item);
}

#[cfg(test)]
mod tests {
        use super::*;

        #[test]
        fn csv_filename_sanitizes_target_and_trims_spaces() {
                let file_name = FormatDefaultCsvName("  ../foo:bar  ");
                assert!(file_name.starts_with("ping_foo_bar_"));
                assert!(file_name.ends_with(".csv"));
                assert!(file_name.chars().all(|ch| ch.is_ascii_alphanumeric()
                        || ch == '_'
                        || ch == '-'
                        || ch == '.'));
        }

        #[test]
        fn csv_extension_is_enforced() {
                assert_eq!(
                        EnsureCsvExtension(PathBuf::from("results.txt")),
                        PathBuf::from("results.csv")
                );
        }
}
