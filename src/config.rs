use anyhow::{anyhow, Result};
use fomat_macros::fomat;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    fs::create_dir_all,
    io::Read,
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
};
use structopt::StructOpt;
use url::Url;

// Semaphores to limit concurrency
// Boards currently limit to 1, so it's sequential
pub(crate) static SEMAPHORE_AMOUNT_BOARDS: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(1));
pub(crate) static SEMAPHORE_AMOUNT_BOARDS_ARCHIVE: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(1));
pub(crate) static SEMAPHORE_AMOUNT_THREADS: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(0));
pub(crate) static SEMAPHORE_AMOUNT_MEDIA: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(0));
pub(crate) static SEMAPHORE_BOARDS: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, SEMAPHORE_AMOUNT_BOARDS.load(Ordering::SeqCst) as usize));
pub(crate) static SEMAPHORE_BOARDS_ARCHIVE: Lazy<futures_intrusive::sync::Semaphore> =
    Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, SEMAPHORE_AMOUNT_BOARDS_ARCHIVE.load(Ordering::SeqCst) as usize));
pub(crate) static SEMAPHORE_THREADS: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, SEMAPHORE_AMOUNT_THREADS.load(Ordering::SeqCst) as usize));
pub(crate) static SEMAPHORE_THREADS_ARCHIVE: Lazy<futures_intrusive::sync::Semaphore> =
    Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, SEMAPHORE_AMOUNT_THREADS.load(Ordering::SeqCst) as usize));
pub(crate) static SEMAPHORE_MEDIA: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, SEMAPHORE_AMOUNT_MEDIA.load(Ordering::SeqCst) as usize));
pub(crate) static SEMAPHORE_MEDIA_TEST: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, 1));

// default_value must be closely tied to Default::default() for sanity.
// Since structopt doesn't use Default::default...
#[derive(Debug, StructOpt, PartialEq, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Board {
    #[structopt(skip)]
    pub id: u16,

    #[structopt(skip)]
    #[serde(rename = "board")]
    pub name: String,

    // #[structopt(long, default_value, env, hide_env_values = true)]
    /// Retry number of HTTP GET requests
    #[structopt(display_order(5), long, default_value("3"), env, hide_env_values = true)]
    pub retry_attempts: u8,

    /// Delay (ms) between each board
    #[structopt(display_order(5), long, default_value("30000"), env, hide_env_values = true)]
    pub interval_boards: u16,

    /// Delay (ms) between each thread
    #[structopt(display_order(5), long, default_value("1000"), env, hide_env_values = true)]
    pub interval_threads: u16,

    /// Add 5s on intervals for each NOT_MODIFIED. Capped.
    #[structopt(display_order(6), long)]
    pub interval_dynamic: bool,

    /// Skip checking if a board is valid
    #[structopt(display_order(7), long)]
    pub skip_board_check: bool,

    /// Grab threads from threads.json
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub with_threads: bool,

    /// Grab threads from archive.json
    // #[structopt(long, env, hide_env_values(true))]
    #[structopt(display_order(5), long)]
    pub with_archives: bool,

    /// Prefer to use tail json if available
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub with_tail: bool,

    /// Download full media as well
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub with_full_media: bool,

    /// Download thumbnails as well
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub with_thumbnails: bool,

    /// Enable archiving the boards
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub watch_boards: bool,

    /// Enable archiving the live threads until deleted or archived (only applies to threadslist)
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub watch_threads: bool,
}

impl Default for Board {
    fn default() -> Self {
        Self {
            id:               0,
            name:             String::new(),
            retry_attempts:   3,
            interval_boards:  30000,
            interval_threads: 1000,
            interval_dynamic: false,
            skip_board_check: false,
            with_threads:     false,
            with_archives:    false,
            with_tail:        false,
            with_full_media:  false,
            with_thumbnails:  false,
            watch_boards:     false,
            watch_threads:    false,
        }
    }
}

fn boards_cli_string(board: &str) -> Result<Board> {
    let board_name = board.to_lowercase();
    if !board_name.is_empty() {
        // This will get patched at post processing so it's OK to use default here
        let mut board = Board::default();
        board.name = board_name;
        Ok(board)
    } else {
        Err(anyhow!("Invalid board format `{}`", board_name))
    }
}

fn threads_cli_string(thread: &str) -> Result<String> {
    let split: Vec<&str> = thread.split('/').filter(|s| !s.is_empty()).collect();
    if split.len() == 2 {
        let no = split[1].parse::<u64>();
        match no {
            Ok(n) => Ok(thread.into()),
            Err(e) => Err(anyhow!("Invalid thread `{}` for `/{}/{}`", split[1], split[0], split[1])),
        }
    } else {
        Err(anyhow!("Invalid thread format `{}`", thread))
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use pretty_assertions::{assert_eq, assert_ne};
    use url::Url;
    #[test]
    fn url() {
        let u = Url::parse("mysql://username:@localhost:3306/db_name?pool_min=1&pool_max=3").unwrap();
        let host = u.host_str();
        let port = u.port();
        let username = u.username();
        let password = u.password();
        let database = u.path();
        let query = u.query();
        let parsed = Url::parse(&format!(
            "mysql://{username}:{password}@{host}:{port}/{database}?{query}",
            username = username,
            password = password.unwrap_or_default(),
            host = host.unwrap_or_default(),
            port = port.unwrap_or(3306),
            database = database.trim_matches('/'),
            query = query.unwrap_or_default(),
        ))
        .unwrap()
        .to_string();
        assert_eq!(parsed, u.to_string());
    }
    #[ignore]
    #[test]
    fn cli_threads() {
        // Comma delimited: /a/12345,/a/1487823,/b/134654,/c/13478,/d/134798
        // Space delimited: /a/12345 /a/1487823 /b/134654 /c/13478 /d/134798
        let threads = "a,1234,b,34534,24354,c,346654,3332,,,5,,,6,,6,];.],][//['],]-=00-9c,c,c";
        // assert_eq!(crate::config::threads_cli_string(threads), vec!["a", "1234", "b", "34534",
        // "24354", "c", "346654", "3332", "5", "6", "c"]);
    }
    #[ignore]
    #[test]
    fn cli_boards() {
        let threads = "a,1234,b,a,34534,24354,c,346654,3332,,,5,,,6,,6,];.],][//['],]-=00-9c,c,c";
        // assert_eq!(super::boards_cli_string(threads), vec!["a", "b", "c"]);
    }
}

#[derive(Debug, StructOpt, Deserialize, Serialize, Clone)]
#[structopt(about)]
#[serde(default)]
// CLI Options
pub struct Opt {
    /// Activate debug mode
    #[structopt(short, long, hidden(true))]
    pub debug: bool,

    /// Download sequentially rather than concurrently. This sets limit to 1.
    #[structopt(long, display_order(5))]
    pub strict: bool,

    /// Download everything in the beginning with no limits and then throttle
    // #[structopt(long, display_order(5))]
    #[structopt(long, hidden(true))]
    pub quickstart: bool,

    /// Download from external archives in the beginning
    // #[structopt(long, display_order(5))]
    #[structopt(skip)]
    pub start_with_archives: bool,

    /// Config file or `-` for stdin
    #[structopt(display_order(1), short, long, parse(from_os_str), default_value = "config.yml", env, hide_env_values = true)]
    pub config: PathBuf,

    /// Get boards [example: a,b,c]
    #[structopt(display_order(2), short, long,  multiple(true), required_unless("config"),  use_delimiter = true, parse(try_from_str = boards_cli_string),  env, hide_env_values = true)]
    pub boards: Vec<Board>,

    /// Exclude boards [example: a,b,c]
    ///
    /// (Only applies to boards from boardslist, not threadslist)
    #[structopt(display_order(2), short("e"), long("exclude-boards"),  multiple(true), required(false),  use_delimiter = true, parse(try_from_str = boards_cli_string),  env, hide_env_values = true)]
    pub boards_excluded: Vec<Board>,

    /// Get threads
    ///
    /// First specify the board, then all the threads belonging to that board.
    /// Comma delimited: /a/12345,/a/1487823,/b/134654,/c/13478,/d/134798
    /// Space delimited: /a/12345 /a/1487823 /b/134654 /c/13478 /d/134798
    #[structopt(verbatim_doc_comment, display_order(3), short, long, multiple(true), required_unless("config"), use_delimiter = true, parse(try_from_str = threads_cli_string),  env, hide_env_values = true )]
    pub threads: Vec<String>,

    /// Set site
    #[structopt(hidden(true), display_order(4), short, long, default_value = "4chan", env)]
    pub site: String,

    /// Limit concurrency getting threads
    #[structopt(display_order(5), long, default_value = "151", env, hide_env_values = true)]
    pub limit: u32,

    /// Media download location
    #[structopt(display_order(5), short, long, parse(from_os_str), default_value = "assets/media", env, hide_env_values = true, hide_default_value(true))]
    pub media_dir: PathBuf,

    /// Media storage type
    ///
    /// Possible values: flatfiles, database, seaweedfs
    // #[structopt(display_order(5),  long, possible_values(&[MediaStorage::FlatFiles.into(),MediaStorage::Database.into(),MediaStorage::SeaweedFS.into()]), default_value, env, hide_env_values = true,
    // hide_possible_values(true))]
    #[structopt(skip)]
    pub media_storage: MediaStorage,

    /// Limit concurrency getting media
    #[structopt(display_order(5), long, default_value = "151", env, hide_env_values = true)]
    pub limit_media: u32,

    /// Set user agent
    #[structopt(
        display_order(7),
        short = "A",
        long,
        hide_default_value(true),
        default_value = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
        env,
        hide_env_values = true
    )]
    pub user_agent: String,

    /// Set api endpoint url
    #[structopt(display_order(8), long, default_value("https://a.4cdn.org"), hide_default_value(true), parse(try_from_str = url::Url::parse), env = "API_URL", hide_env_values = true)]
    pub api_url: Url,

    /// Set media endpoint url
    #[structopt(display_order(9), long,  default_value("https://i.4cdn.org"), hide_default_value(true), parse(try_from_str = url::Url::parse), env, hide_env_values = true)]
    pub media_url: Url,

    #[structopt(flatten)]
    pub board_settings: Board,

    #[structopt(flatten)]
    pub database: DatabaseOpt,

    #[structopt(flatten)]
    pub asagi: AsagiOpt,

    #[structopt(skip)]
    pub timescaledb: Option<TimescaleSettings>,

    #[structopt(skip)]
    pub proxies: Option<Vec<ProxySettings>>,
}

impl Opt {
    pub fn asagi(&self) -> bool {
        self.asagi.r#use
    }

    pub fn to_string(&mut self) -> Result<String> {
        let mut url = url::Url::parse(self.database.url.as_ref().unwrap())?;
        url.set_password(Some("*****")).unwrap();
        self.database.password = url.password().unwrap().to_string();
        self.database.url = Some(url.to_string());
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl Default for Opt {
    fn default() -> Self {
        Self {
            debug:               false,
            strict:              false,
            quickstart:          false,
            start_with_archives: false,
            config:              "config.yml".into(),
            boards:              vec![],
            boards_excluded:     vec![],
            threads:             vec![],
            site:                "4chan".into(),
            limit:               151,
            media_dir:           "assets/media".into(),
            media_storage:       MediaStorage::FlatFiles,
            limit_media:         151,
            user_agent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0".into(),
            api_url:             "https://a.4cdn.org".parse().unwrap(),
            media_url:           "https://i.4cdn.org".parse().unwrap(),
            board_settings:      Board::default(),
            database:            DatabaseOpt::default(),
            asagi:               AsagiOpt::default(),
            timescaledb:         None,
            proxies:             None,
        }
    }
}

#[derive(Debug, StructOpt, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct AsagiOpt {
    /// Use Ena as an Asagi drop-in replacement
    #[structopt(display_order(5), long("asagi"))]
    pub r#use: bool,

    /// Add and use `utc_timestamp_expired`, `utc_timestamp`, in board tables and
    /// `utc_time_archived` in `{board}_threads` tables
    #[structopt(hidden(true), long, env)]
    pub with_utc_timestamps: bool,

    /// Add any extra columns to `exif`
    #[structopt(hidden(true), long, env)]
    pub with_extra_columns: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct TimescaleSettings {
    pub column: String,
    pub every:  String,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct ProxySettings {
    pub url:      String,
    pub username: Option<String>,
    pub password: Option<String>,
}
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum MediaStorage {
    FlatFiles,
    Database,
    SeaweedFS,
}

impl Default for MediaStorage {
    fn default() -> MediaStorage {
        MediaStorage::FlatFiles
    }
}

impl std::str::FromStr for MediaStorage {
    type Err = anyhow::Error;

    fn from_str(storage: &str) -> Result<Self, Self::Err> {
        if storage == &MediaStorage::FlatFiles.to_string() {
            Ok(MediaStorage::FlatFiles)
        } else if storage == &MediaStorage::Database.to_string() {
            Ok(MediaStorage::Database)
        } else if storage == &MediaStorage::SeaweedFS.to_string() {
            Ok(MediaStorage::SeaweedFS)
        } else {
            Err(anyhow!("Unknown MediaStorage: {}", storage))
        }
    }
}

impl From<MediaStorage> for &str {
    fn from(storage: MediaStorage) -> Self {
        match storage {
            MediaStorage::FlatFiles => "flatfiles",
            MediaStorage::Database => "database",
            MediaStorage::SeaweedFS => "seaweedfs",
        }
    }
}

impl From<&str> for MediaStorage {
    fn from(storage: &str) -> Self {
        if storage == &MediaStorage::FlatFiles.to_string() {
            MediaStorage::FlatFiles
        } else if storage == &MediaStorage::Database.to_string() {
            MediaStorage::Database
        } else if storage == &MediaStorage::SeaweedFS.to_string() {
            MediaStorage::SeaweedFS
        } else {
            panic!(anyhow!("Unkown MediaStorage: {}", storage))
        }
    }
}

impl std::fmt::Display for MediaStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MediaStorage::FlatFiles => write!(f, "flatfiles"),
            MediaStorage::Database => write!(f, "database"),
            MediaStorage::SeaweedFS => write!(f, "seaweedfs"),
        }
    }
}
#[derive(Debug, StructOpt, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct DatabaseOpt {
    /// Set database url
    #[structopt(hidden(true), display_order(10), long = "db-url", env = "ENA_DATABASE_URL", hide_env_values = true, hide_default_value(true))]
    pub url: Option<String>,

    /// Set database engine
    #[structopt(display_order(11), long, default_value("postgresql"), env = "ENA_DATABASE_ENGINE", hide_env_values = true, hide_default_value(true))]
    pub engine: String,

    /// Set database name
    #[structopt(display_order(12), long, default_value("ena"), env = "ENA_DATABASE_NAME", hide_env_values = true, hide_default_value(true))]
    #[serde(rename = "database")]
    pub name: String,

    /// Set database schema
    #[structopt(display_order(13), long, default_value("public"), env = "ENA_DATABASE_SCHEMA", hide_env_values = true, hide_default_value(true))]
    pub schema: String,

    /// Set database host
    #[structopt(display_order(14), long, default_value("localhost"), env = "ENA_DATABASE_HOST", hide_env_values = true, hide_default_value(true))]
    pub host: String,

    /// Set database port
    #[structopt(display_order(15), long, default_value("5432"), env = "ENA_DATABASE_PORT", hide_env_values = true, hide_default_value(true))]
    pub port: u16,

    /// Set database user
    #[structopt(display_order(16), long, default_value("postgres"), env = "ENA_DATABASE_USERNAME", hide_env_values = true, hide_default_value(true))]
    pub username: String,

    /// Set database password
    #[structopt(display_order(17), long, default_value("zxc"), env = "ENA_DATABASE_PASSWORD", hide_env_values = true, hide_default_value(true))]
    pub password: String,

    /// Set database charset
    #[structopt(display_order(18), long, default_value("utf8"), env = "ENA_DATABASE_CHARSET", hide_env_values = true, hide_default_value(true))]
    pub charset: String,

    /// Set database charset
    #[structopt(display_order(18), long, default_value("utf8"), env = "ENA_DATABASE_COLLATE", hide_env_values = true, hide_default_value(true))]
    pub collate: String,
}

impl Default for DatabaseOpt {
    fn default() -> Self {
        Self {
            url:      None,
            engine:   "postgresql".into(),
            name:     "ena".into(),
            schema:   "public".into(),
            host:     "localhost".into(),
            port:     5432,
            username: "postgres".into(),
            password: "zxc".into(),
            charset:  "utf8".into(),
            collate:  "utf8".into(),
        }
    }
}

/// Display an ascii art with the crate version
pub fn display() {
    println!(
        r#"
    ⣿⡇⣿⣿⣿⠛⠁⣴⣿⡿⠿⠧⠹⠿⠘⣿⣿⣿⡇⢸⡻⣿⣿⣿⣿⣿⣿⣿
    ⢹⡇⣿⣿⣿⠄⣞⣯⣷⣾⣿⣿⣧⡹⡆⡀⠉⢹⡌⠐⢿⣿⣿⣿⡞⣿⣿⣿
    ⣾⡇⣿⣿⡇⣾⣿⣿⣿⣿⣿⣿⣿⣿⣄⢻⣦⡀⠁⢸⡌⠻⣿⣿⣿⡽⣿⣿       ____
    ⡇⣿⠹⣿⡇⡟⠛⣉⠁⠉⠉⠻⡿⣿⣿⣿⣿⣿⣦⣄⡉⠂⠈⠙⢿⣿⣝⣿      /\  _`\
    ⠤⢿⡄⠹⣧⣷⣸⡇⠄⠄⠲⢰⣌⣾⣿⣿⣿⣿⣿⣿⣶⣤⣤⡀⠄⠈⠻⢮      \ \ \L\_     ___      __
    ⠄⢸⣧⠄⢘⢻⣿⡇⢀⣀⠄⣸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⡀⠄⢀       \ \  __\  /' _ `\  /'__`\
    ⠄⠈⣿⡆⢸⣿⣿⣿⣬⣭⣴⣿⣿⣿⣿⣿⣿⣿⣯⠝⠛⠛⠙⢿⡿⠃⠄⢸        \ \ \___\/\ \/\ \/\ \L\.\_
    ⠄⠄⢿⣿⡀⣿⣿⣿⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣿⣿⣿⣿⡾⠁⢠⡇⢀         \ \____/\ \_\ \_\ \__/.\_\
    ⠄⠄⢸⣿⡇⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣏⣫⣻⡟⢀⠄⣿⣷⣾          \/___/  \/_/\/_/\/__/\/_/   v{}
    ⠄⠄⢸⣿⡇⠄⠈⠙⠿⣿⣿⣿⣮⣿⣿⣿⣿⣿⣿⣿⣿⡿⢠⠊⢀⡇⣿⣿
    ⠒⠤⠄⣿⡇⢀⡲⠄⠄⠈⠙⠻⢿⣿⣿⠿⠿⠟⠛⠋⠁⣰⠇⠄⢸⣿⣿⣿
    ⠄⠄⠄⣿⡇⢬⡻⡇⡄⠄⠄⠄⡰⢖⠔⠉⠄⠄⠄⠄⣼⠏⠄⠄⢸⣿⣿⣿        A lightweight 4chan archiver (¬ ‿ ¬ )
    ⠄⠄⠄⣿⡇⠄⠙⢌⢷⣆⡀⡾⡣⠃⠄⠄⠄⠄⠄⣼⡟⠄⠄⠄⠄⢿⣿⣿
    "#,
        version()
    );
}

pub fn display_asagi() {
    println!(
        r#"
    ⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⣿⡿⠿⢿⣿⣿⣿⣿⣿⣿
    ⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠛⠛⠉⠉⠉⠙⠻⣅⠀⠈⢧⠀⠈⠛⠉⠉⢻⣿⣿       ____
    ⣿⣿⣿⣿⣿⣿⠿⠋⠀⠀⠀⠀⠀⠀⠀⠀⣤⡶⠟⠀⠀⣈⠓⢤⣶⡶⠿⠛⠻⣿      /\  _`\
    ⣿⣿⣿⣿⣿⢣⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⢿⣀⣴⠶⠿⠿⢷⡄⠀⠀⢀⣤⣾⣿      \ \ \L\_     ___      __
    ⣿⣿⣿⣿⣡⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣦⣤⣤⡀⠀⢷⡀⠀⠀⣻⣿⣿       \ \  __\  /' _ `\  /'__`\
    ⣿⣿⣿⣿⠇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡈⠛⠶⠛⠃⠈⠈⢿⣿⣿        \ \ \___\/\ \/\ \/\ \L\.\_
    ⣿⣿⠟⠘⠀⠀⠀⠀⠀⠀⠀⠀⢀⡆⠀⠀⠀⠀⠀⠀⣧⠀⠀⠀⠀⠀⠀⠈⣿⣿         \ \____/\ \_\ \_\ \__/.\_\
    ⣿⠏⠀⠁⠀⠀⠀⠀⠀⠀⠀⢀⣶⡄⠀⠀⠀⠀⠀⠀⣡⣄⣿⡆⠀⠀⠀⠀⣿⣿          \/___/  \/_/\/_/\/__/\/_/   v{}
    ⡏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠚⠛⠛⢛⣲⣶⣿⣷⣉⠉⢉⣥⡄⠀⠀⠀⠨⣿⣿
    ⡇⢠⡆⠀⠀⢰⠀⠀⠀⠀⢸⣿⣧⣠⣿⣿⣿⣿⣿⣿⣷⣾⣿⡅⠀⠀⡄⠠⢸⣿        You're using Asagi when you could be using Ena instead!
    ⣧⠸⣇⠀⠀⠘⣤⡀⠀⠀⠘⣿⣿⣿⣿⣿⠟⠛⠻⣿⣿⣿⡿⢁⠀⠀⢰⠀⢸⣿
    ⣿⣷⣽⣦⠀⠀⠙⢷⡀⠀⠀⠙⠻⠿⢿⣷⣾⣿⣶⠾⢟⣥⣾⣿⣧⠀⠂⢀⣿⣿        You're unbelievable!
    ⣿⣿⣿⣿⣷⣆⣠⣤⣤⣤⣀⣀⡀⠀⠒⢻⣶⣾⣿⣿⣿⣿⣿⣿⣿⢀⣀⣾⣿⣿
    "#,
        version()
    );
}

/// Return the current version of the crate
///
/// # Example
///
/// ```
/// use ena::config;
/// let version = config::version();
/// ```
pub fn version() -> String {
    option_env!("CARGO_PKG_VERSION").unwrap_or("?.?.?").to_string()
}

#[rustfmt::skip]
pub fn get_opt() -> Result<Opt> {
    let mut opt = Opt::from_args();
    let default = Board::default();
    for b in opt.boards.iter_mut() {
        // Patch CLI opt boards to use its board_settings
        if b.retry_attempts     == default.retry_attempts   { b.retry_attempts      = opt.board_settings.retry_attempts; }
        if b.interval_boards    == default.interval_boards  { b.interval_boards     = opt.board_settings.interval_boards; }
        if b.interval_threads   == default.interval_threads { b.interval_threads    = opt.board_settings.interval_threads; }
        if b.with_threads       == default.with_threads     { b.with_threads        = opt.board_settings.with_threads; }
        if b.with_archives      == default.with_archives    { b.with_archives       = opt.board_settings.with_archives; }
        if b.with_tail          == default.with_tail        { b.with_tail           = opt.board_settings.with_tail; }
        if b.with_full_media    == default.with_full_media  { b.with_full_media     = opt.board_settings.with_full_media; }
        if b.with_thumbnails    == default.with_thumbnails  { b.with_thumbnails     = opt.board_settings.with_thumbnails; }
        if b.watch_boards       == default.watch_boards     { b.watch_boards        = opt.board_settings.watch_boards; }
        if b.watch_threads      == default.watch_threads    { b.watch_threads       = opt.board_settings.watch_threads; }
    }
    
    opt.boards = opt.boards.iter()
        .filter(|board| !opt.boards_excluded.iter().any(|board_excluded| board.name == board_excluded.name) )
        .cloned()
        .collect::<Vec<Board>>();
    // https://stackoverflow.com/a/55150936
    let mut opt = {
        if let Some(config_file) = &opt.config.to_str() {
            if config_file.trim().is_empty() {
                opt
            } else {
                let content =
                if config_file == &"-" {
                    let mut content = String::new();
                    std::io::stdin().lock().read_to_string(&mut content)?;
                    content
                } else {
                    let res = std::fs::read_to_string(config_file).map_err(|e|
                        anyhow!("Error accessing config `{}` [{}]", &config_file, e)
                    )?;
                    res
                };
                let mut q = 
                serde_yaml::from_str::<Opt>(&content).map_err(|e|anyhow!("Error parsing config `{}` [{}]", &config_file, e))?;
                let default_opt = Opt::default();
                        let default = Board::default(); // This has to call Self::default(), not &default_opt.board_settings (to prevent incorrect values)
                        let default_database = DatabaseOpt::default();
                        let default_asagi = AsagiOpt::default();
                        if opt.board_settings.retry_attempts    != default.retry_attempts   { q.board_settings.retry_attempts = opt.board_settings.retry_attempts; }
                        if opt.board_settings.interval_boards   != default.interval_boards  { q.board_settings.interval_boards = opt.board_settings.interval_boards; }
                        if opt.board_settings.interval_threads  != default.interval_threads { q.board_settings.interval_threads = opt.board_settings.interval_threads; }
                        if opt.board_settings.interval_dynamic  != default.interval_dynamic { q.board_settings.interval_dynamic = opt.board_settings.interval_dynamic; }
                        if opt.board_settings.skip_board_check  != default.skip_board_check { q.board_settings.skip_board_check = opt.board_settings.skip_board_check; }
                        if opt.board_settings.with_threads      != default.with_threads     { q.board_settings.with_threads = opt.board_settings.with_threads; }
                        if opt.board_settings.with_archives     != default.with_archives    { q.board_settings.with_archives = opt.board_settings.with_archives; }
                        if opt.board_settings.with_tail         != default.with_tail        { q.board_settings.with_tail = opt.board_settings.with_tail; }
                        if opt.board_settings.with_full_media   != default.with_full_media  { q.board_settings.with_full_media = opt.board_settings.with_full_media; }
                        if opt.board_settings.with_thumbnails   != default.with_thumbnails  { q.board_settings.with_thumbnails = opt.board_settings.with_thumbnails; }
                        if opt.board_settings.watch_boards      != default.watch_boards     { q.board_settings.watch_boards = opt.board_settings.watch_boards; }
                        if opt.board_settings.watch_threads     != default.watch_threads    { q.board_settings.watch_threads = opt.board_settings.watch_threads; }


                        let boards_excluded_combined: Vec<Board>  = q.boards_excluded.iter().chain(opt.boards_excluded.iter()).cloned().collect();
                        let threads_combined: Vec<String> = q.threads.iter().chain(opt.threads.iter()).cloned().collect();
                        let boards_combined: Vec<Board> = q.boards.iter().chain(opt.boards.iter())
                        .filter(|board| !boards_excluded_combined.iter().any(|board_excluded| board.name == board_excluded.name) )
                        .cloned()
                        .collect();
                        q.boards_excluded = boards_excluded_combined;
                        q.boards = boards_combined;
                        q.threads = threads_combined;
                        for  b in q.boards.iter_mut() {
                            // Patch config.yaml to use its board_settings
                            if b.retry_attempts     == default.retry_attempts   { b.retry_attempts = q.board_settings.retry_attempts; }
                            if b.interval_boards    == default.interval_boards  { b.interval_boards = q.board_settings.interval_boards; }
                            if b.interval_threads   == default.interval_threads { b.interval_threads = q.board_settings.interval_threads; }
                            if b.with_threads       == default.with_threads     { b.with_threads = q.board_settings.with_threads; }
                            if b.interval_dynamic   == default.interval_dynamic { b.interval_dynamic = q.board_settings.interval_dynamic; }
                            if b.skip_board_check   == default.skip_board_check { b.skip_board_check = q.board_settings.skip_board_check; }
                            if b.with_archives      == default.with_archives    { b.with_archives =  q.board_settings.with_archives; }
                            if b.with_tail          == default.with_tail        { b.with_tail = q.board_settings.with_tail; }
                            if b.with_full_media    == default.with_full_media  { b.with_full_media = q.board_settings.with_full_media; }
                            if b.with_thumbnails    == default.with_thumbnails  { b.with_thumbnails = q.board_settings.with_thumbnails; }
                            if b.watch_boards       == default.watch_boards     { b.watch_boards = q.board_settings.watch_boards; }
                            if b.watch_threads      == default.watch_threads    { b.watch_threads = q.board_settings.watch_threads; }
                        }

                        // Finally patch the yaml's board_settings with CLI opts
                        if q.debug                  == default_opt.debug                { q.debug = opt.debug;                                      }
                        if q.strict                 == default_opt.strict               { q.strict = opt.strict;                                    }
                        if q.quickstart             == default_opt.quickstart           { q.quickstart = opt.quickstart;                            }
                        if q.start_with_archives    == default_opt.start_with_archives  { q.start_with_archives = opt.start_with_archives;          }
                        if q.config                 == default_opt.config               { q.config = opt.config;                                    }
                        if q.site                   == default_opt.site                 { q.site =  opt.site;                                       }
                        if q.limit                  == default_opt.limit                { q.limit = opt.limit;                                      }
                        if q.media_dir              == default_opt.media_dir            { q.media_dir = opt.media_dir;                              }
                        if q.media_storage          == default_opt.media_storage        { q.media_storage = opt.media_storage;                      }
                        if q.limit_media            == default_opt.limit_media          { q.limit_media = opt.limit_media;                          }
                        if q.user_agent             == default_opt.user_agent           { q.user_agent = opt.user_agent;                            }
                        if q.api_url                == default_opt.api_url              { q.api_url = opt.api_url;                                  }
                        if q.media_url              == default_opt.media_url            { q.media_url = opt.media_url;                              }

                        // Database
                        if q.database.url           == default_database.url             { q.database.url        = opt.database.url.clone();         }
                        if q.database.engine        == default_database.engine          { q.database.engine     = opt.database.engine.clone();      }
                        if q.database.name          == default_database.name            { q.database.name       = opt.database.name.clone();        }
                        if q.database.schema        == default_database.schema          { q.database.schema     = opt.database.schema.clone();      }
                        if q.database.port          == default_database.port            { q.database.port       = opt.database.port;                }
                        if q.database.username      == default_database.username        { q.database.username   = opt.database.username.clone();    }
                        if q.database.password      == default_database.password        { q.database.password   = opt.database.password.clone();    }
                        if q.database.charset       == default_database.charset         { q.database.charset    = opt.database.charset.clone();     }
                        if q.database.collate       == default_database.collate         { q.database.collate    = opt.database.collate;             }
                        
                        // Asagi Options
                        if q.asagi()                   == default_asagi.r#use               { q.asagi.r#use               = opt.asagi.r#use;               }
                        if q.asagi.with_utc_timestamps == default_asagi.with_utc_timestamps { q.asagi.with_utc_timestamps = opt.asagi.with_utc_timestamps; }
                        if q.asagi.with_extra_columns  == default_asagi.with_extra_columns  { q.asagi.with_extra_columns  = opt.asagi.with_extra_columns;  }
                        


                        if q.board_settings.retry_attempts      == default.retry_attempts   { q.board_settings.retry_attempts = opt.board_settings.retry_attempts; }
                        if q.board_settings.interval_boards     == default.interval_boards  { q.board_settings.interval_boards  = opt.board_settings.interval_boards; };
                        if q.board_settings.interval_threads    == default.interval_threads { q.board_settings.interval_threads = opt.board_settings.interval_threads; }
                        if q.board_settings.interval_dynamic    == default.interval_dynamic { q.board_settings.interval_dynamic = opt.board_settings.interval_dynamic; }
                        if q.board_settings.skip_board_check    == default.skip_board_check { q.board_settings.skip_board_check = opt.board_settings.skip_board_check; }
                        if q.board_settings.with_threads        == default.with_threads     { q.board_settings.with_threads = opt.board_settings.with_threads; }
                        if q.board_settings.with_archives       == default.with_archives    { q.board_settings.with_archives = opt.board_settings.with_archives; }
                        if q.board_settings.with_tail           == default.with_tail        { q.board_settings.with_tail = opt.board_settings.with_tail; }
                        if q.board_settings.with_full_media     == default.with_full_media  { q.board_settings.with_full_media = opt.board_settings.with_full_media; }
                        if q.board_settings.with_thumbnails     == default.with_thumbnails  { q.board_settings.with_thumbnails  = opt.board_settings.with_thumbnails; }
                        if q.board_settings.watch_boards        == default.watch_boards     { q.board_settings.watch_boards = opt.board_settings.watch_boards; }
                        if q.board_settings.watch_threads       == default.watch_threads    { q.board_settings.watch_threads = opt.board_settings.watch_threads; }

                        q.clone()
            }
        } else {
            opt
        }
    };


    if let Some(db_url) = &opt.database.url {
        // Parse the database url so it's correct
        // Then update the indivual fields accordingly
        let u = url::Url::parse(db_url).unwrap();
        opt.database.username = u.username().to_string();
        opt.database.password = u.password().unwrap_or_default().to_string();
        opt.database.host = u.host_str().unwrap_or_default().to_string();
        opt.database.port = u.port().unwrap_or(if &opt.database.engine.as_str().to_lowercase() != "postgresql" { 3306 } else { 5432 });
        opt.database.name = u.path().trim_matches('/').to_string();
        opt.database.url = Some(u.to_string())
    } else {
        let db_url = format!(
            "{engine}://{user}:{password}@{host}:{port}/{database}",
            engine = if &opt.database.engine.as_str().to_lowercase() != "postgresql" { "mysql" } else { &opt.database.engine },
            user = &opt.database.username,
            password = &opt.database.password,
            host = &opt.database.host,
            port = &opt.database.port,
            database = &opt.database.name,
        );
        let url = url::Url::parse(&db_url).unwrap().to_string();
        opt.database.url = Some(url);
    }

    if opt.asagi() && !opt.database.url.as_ref().unwrap().contains("mysql") {
        return Err(anyhow!("Asagi mode must be used with a MySQL database. Did you mean to disable --asagi ?"));
    }

    if !opt.asagi() && !opt.database.url.as_ref().unwrap().contains("postgresql") {
        return Err(anyhow!("Ena must be used with a PostgreSQL database. Did you mean to enable --asagi ?"));
    }

    // Patch concurrency limit
    if opt.strict { opt.limit = 1; }
    SEMAPHORE_AMOUNT_THREADS.fetch_add(opt.limit, Ordering::SeqCst);
    SEMAPHORE_AMOUNT_MEDIA.fetch_add(opt.limit_media, Ordering::SeqCst);

    // &opt.threads.dedup();

    // Afterwards dedup boards
    use itertools::Itertools;
    opt.boards = (&opt.boards).into_iter().unique_by(|board| board.name.as_str()).cloned().collect();


    // Trim media dir
    opt.media_dir = opt.media_dir.to_str().map(|v| v.trim_matches('/').trim_matches('\\').into()).unwrap_or(opt.media_dir);

    // Media Directories
    if !&opt.media_dir.is_dir() {
        create_dir_all(&opt.media_dir)?;
    }

    if !opt.asagi() {
        let tmp = fomat!( (&opt.media_dir.display())"/tmp" );
        if !std::path::Path::new(&tmp).is_dir() {
            create_dir_all(&tmp)?;
        }
    } else {
        let default_database = DatabaseOpt::default();
        // If a user actually wanted the default (utf8), this would overwrite their setting to utf8mb4.. it's ok right?
        // Since utf8mb4 is actually the real UTF8.
        if opt.database.charset           == default_database.charset             { opt.database.charset        = "utf8mb4".into();         }
        if opt.database.collate           == default_database.collate             { opt.database.collate        = "utf8mb4_unicode_ci".into();         }
        opt.database.schema = opt.database.name.clone();
        opt.timescaledb = None;
    }

    Ok(opt)
}
