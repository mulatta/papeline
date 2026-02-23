//! Logging utilities with indicatif integration

use indicatif::MultiProgress;

/// ANSI color code and padded label for a log level.
fn level_style(level: log::Level, color: bool) -> (&'static str, &'static str, &'static str) {
    let label = match level {
        log::Level::Error => "ERROR",
        log::Level::Warn => "WARN ",
        log::Level::Info => "INFO ",
        log::Level::Debug => "DEBUG",
        log::Level::Trace => "TRACE",
    };
    if !color {
        return ("", label, "");
    }
    let ansi = match level {
        log::Level::Error => "\x1b[31m",
        log::Level::Warn => "\x1b[33m",
        log::Level::Info => "\x1b[32m",
        log::Level::Debug => "\x1b[36m",
        log::Level::Trace => "\x1b[35m",
    };
    (ansi, label, "\x1b[0m")
}

/// Logger that prints through indicatif MultiProgress to avoid mixing with progress bars.
pub struct IndicatifLogger {
    inner: env_logger::Logger,
    multi: MultiProgress,
}

impl IndicatifLogger {
    pub fn new(inner: env_logger::Logger, multi: MultiProgress) -> Self {
        Self { inner, multi }
    }
}

impl log::Log for IndicatifLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        if self.inner.enabled(record.metadata()) {
            // TTY path — always has color (IndicatifLogger only used in TTY mode)
            let (pre, label, post) = level_style(record.level(), true);
            let line = format!("[{pre}{label}{post}] {}", record.args());
            self.multi.suspend(|| eprintln!("{line}"));
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

/// Initialize logging with optional TTY mode (indicatif integration)
pub fn init_logging(quiet: bool, debug: bool, multi: Option<&MultiProgress>) {
    use std::io::Write;

    let default_level = if debug {
        "debug"
    } else if quiet {
        "warn"
    } else {
        "info"
    };

    if let Some(multi) = multi {
        let logger = env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or(default_level),
        )
        .format_timestamp_millis()
        .build();
        let max_level = logger.filter();

        log::set_boxed_logger(Box::new(IndicatifLogger::new(logger, multi.clone())))
            .expect("failed to init logger");
        log::set_max_level(max_level);
    } else {
        // Non-TTY: no ANSI colors, timestamp for log aggregation
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_level))
            .format(|buf, record| {
                let (_, label, _) = level_style(record.level(), false);
                writeln!(buf, "[{label}] {}", record.args())
            })
            .init();
    }
}
