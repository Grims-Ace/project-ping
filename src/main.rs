// =============================================================================
// =============================================================================
// Application entry point for Project Ping network latency monitor.
// =============================================================================
// =============================================================================

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#![allow(non_snake_case)]

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
