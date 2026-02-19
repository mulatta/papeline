//! Logging utilities with indicatif integration

use indicatif::MultiProgress;

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
            let (color, level_str) = match record.level() {
                log::Level::Error => ("\x1b[31m", "ERROR"),
                log::Level::Warn => ("\x1b[33m", "WARN "),
                log::Level::Info => ("\x1b[32m", "INFO "),
                log::Level::Debug => ("\x1b[36m", "DEBUG"),
                log::Level::Trace => ("\x1b[35m", "TRACE"),
            };
            let reset = "\x1b[0m";
            let line = format!("[{color}{level_str}{reset}] {}", record.args());
            self.multi.suspend(|| eprintln!("{line}"));
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

/// Initialize logging with optional TTY mode (indicatif integration)
pub fn init_logging(quiet: bool, verbose: bool, multi: Option<&MultiProgress>) {
    use std::io::Write;

    let default_level = if verbose {
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
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_level))
            .format(|buf, record| {
                let (color, level_str) = match record.level() {
                    log::Level::Error => ("\x1b[31m", "ERROR"),
                    log::Level::Warn => ("\x1b[33m", "WARN "),
                    log::Level::Info => ("\x1b[32m", "INFO "),
                    log::Level::Debug => ("\x1b[36m", "DEBUG"),
                    log::Level::Trace => ("\x1b[35m", "TRACE"),
                };
                writeln!(buf, "[{color}{level_str}\x1b[0m] {}", record.args())
            })
            .init();
    }
}
