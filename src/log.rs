use fomat_macros::witeln;
use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
};

use env_logger::{
    fmt::{Color, Style, StyledValue},
    Builder,
};
use log::Level;

/// Initialized the global logger with a timed pretty env logger, with a custom variable name.
///
/// This should be called early in the execution of a Rust program, and the
/// global logger may only be initialized once. Future initialization attempts
/// will return an error.
///
/// # Errors
///
/// This function fails to set the global logger if one has already been set.
pub fn try_init_timed_custom_env(environment_variable_name: &str) -> Result<(), log::SetLoggerError> {
    let mut builder = formatted_timed_builder();

    if let Ok(s) = ::std::env::var(environment_variable_name) {
        builder.parse_filters(&s);
    }

    builder.try_init()
}

/// Returns a `env_logger::Builder` for further customization.
///
/// This method will return a colored and time formatted `env_logger::Builder`
/// for further customization. Refer to env_logger::Build crate documentation
/// for further details and usage.
pub fn formatted_timed_builder() -> Builder {
    let mut builder = Builder::new();

    builder.format(|f, record| {
        use std::io::Write;
        let target = record.target();
        let max_width = max_target_width(target);

        let mut style = f.style();
        let level = colored_level(&mut style, record.level());

        let mut style = f.style();
        let target = style.set_bold(true).value(Padded { value: target, width: max_width });

        let time = f.timestamp_millis();

        witeln!(
            f,
            " "(time)" "(level)" "(target)" > "(record.args())
        )
    });

    builder
}

fn colored_level<'a>(style: &'a mut Style, level: Level) -> StyledValue<'a, &'static str> {
    match level {
        Level::Trace => style.set_color(Color::Magenta).value("TRACE"),
        Level::Debug => style.set_color(Color::Blue).value("DEBUG"),
        Level::Info => style.set_color(Color::Green).value("INFO "),
        Level::Warn => style.set_color(Color::Yellow).value("WARN "),
        Level::Error => style.set_color(Color::Red).value("ERROR"),
    }
}

struct Padded<T> {
    value: T,
    width: usize,
}
impl<T: fmt::Display> fmt::Display for Padded<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{: <width$}", self.value, width = self.width)
    }
}

static MAX_MODULE_WIDTH: AtomicUsize = AtomicUsize::new(0);

fn max_target_width(target: &str) -> usize {
    let max_width = MAX_MODULE_WIDTH.load(Ordering::Relaxed);
    if max_width < target.len() {
        MAX_MODULE_WIDTH.store(target.len(), Ordering::Relaxed);
        target.len()
    } else {
        max_width
    }
}
