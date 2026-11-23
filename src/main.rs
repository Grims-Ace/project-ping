// =============================================================================
// =============================================================================
// Application entry point for Project Ping network latency monitor.
// =============================================================================
// =============================================================================

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#![allow(non_snake_case)]

// =============================================================================
// Platform and Runtime Libraries
// =============================================================================

use std::fs::OpenOptions;
use std::io::Write;

// =============================================================================
// Third-Party Libraries
// =============================================================================

use eframe::NativeOptions;
use eframe::egui::ViewportBuilder;

// =============================================================================
// Domain Modules
// =============================================================================

use project_ping::PingApp;

// =============================================================================
// Primary Operations
// =============================================================================

// Start the native graphical interface application.
fn main() -> eframe::Result<()> {
        InstallPanicLog();
        let viewport = ViewportBuilder::default()
                .with_inner_size([920.0, 640.0])
                .with_min_inner_size([650.0, 450.0])
                .with_title("Project Ping");

        let native_options = NativeOptions {
                viewport,
                ..Default::default()
        };

        eframe::run_native(
                "Project Ping",
                native_options,
                Box::new(|cc| Ok(Box::new(PingApp::new(cc)))),
        )
}

// Write panic details to a file if no console is available.
fn InstallPanicLog() {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
                if let Ok(mut file) = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(std::env::temp_dir()
                                .join("project-ping-panic.log"))
                {
                        let _ = writeln!(file, "{panic_info}");
                }
                default_hook(panic_info);
        }));
}
