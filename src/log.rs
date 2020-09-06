//! Better Logging
//!
//! Supports color and redirection to stdout and stderr

use ansiform::ansi;
use chrono::{SecondsFormat, Utc};
#[allow(unused_imports)]
use fomat_macros::{epintln, fomat, pintln};
use log::{Level, LevelFilter, Metadata, Record, SetLoggerError};

static LOGGER: Logger = Logger;

pub fn init(level: LevelFilter) -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| ::log::set_max_level(level))
}

pub struct Logger;

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Trace
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Some(module) = record.module_path() {
                // if module.starts_with(env!("CARGO_PKG_NAME")) {
                // }
                if !module.starts_with("hyper") {
                    let level = record.level();
                    // let utc = Utc::now().to_rfc2822();
                    let utc = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
                    match level {
                        Level::Warn => epintln!(
                            {
                            ansi!("{} {;yellow}  {} > {}"),
                            utc,
                            level,
                            module,
                            record.args()
                            }
                        ),
                        Level::Error => epintln!(
                            {
                            ansi!("{} {;red} {} > {}"),
                            utc,
                            level,
                            module,
                            record.args()
                            }
                        ),
                        Level::Info => pintln!(
                            {
                            ansi!("{} {;blue}  {} > {}"),
                            utc,
                            level,
                            module,
                            record.args()
                            }
                        ),
                        Level::Debug => pintln!(
                            {
                            ansi!("{} {;green} {} > {}"),
                            utc,
                            level,
                            module,
                            record.args()
                            }
                        ),
                        Level::Trace => pintln!(
                            {
                            ansi!("{} {;magenta} {} > {}"),
                            utc,
                            level,
                            module,
                            record.args()
                            }
                        ),
                    };
                }
            }
        }
    }

    fn flush(&self) {}
}
