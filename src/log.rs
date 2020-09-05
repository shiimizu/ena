//! Better Logging
//!
//! Supports color and redirection to stdout and stderr

use ansiform::ansi;
use chrono::{SecondsFormat, Utc};
#[allow(unused_imports)]
use fomat_macros::{epintln, fomat, pintln};
use log::{Level, Metadata, Record, SetLoggerError, LevelFilter};

static LOGGER: Logger =  Logger;

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
            let level = record.level();
            // let utc = Utc::now().to_rfc2822();
            let utc = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            match level {
                Level::Warn => epintln!(
                    {
                    ansi!("{} {:;yellow}  {} > {}"),
                    utc,
                    level,
                    record.module_path().unwrap(),
                    record.args()
                    }
                ),
                Level::Error => epintln!(
                    {
                    ansi!("{} {:;red} {} > {}"),
                    utc,
                    level,
                    record.module_path().unwrap(),
                    record.args()
                    }
                ),
                Level::Info => pintln!(
                    {
                    ansi!("{} {:;blue}  {} > {}"),
                    utc,
                    level,
                    record.module_path().unwrap(),
                    record.args()
                    }
                ),
                Level::Debug => pintln!(
                    {
                    ansi!("{} {:;green} {} > {}"),
                    utc,
                    level,
                    record.module_path().unwrap(),
                    record.args()
                    }
                ),
                Level::Trace => pintln!(
                    {
                    ansi!("{} {:;magenta} {} > {}"),
                    utc,
                    level,
                    record.module_path().unwrap(),
                    record.args()
                    }
                ),
            };
        }
    }

    fn flush(&self) {}
}
