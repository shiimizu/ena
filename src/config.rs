// use anyhow::{anyhow, Result};
use color_eyre::eyre::{eyre, Result};
// use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
// clap::{Arg, ArgGroup},
// use structopt::{clap::ArgGroup, StructOpt};
use structopt::StructOpt;
use url::Url;

fn toggle_bool(i: u64) -> bool {
    i == 0
}

// default_value must be closely tied to Default::default() for sanity.
// Since structopt doesn't use Default::default...
#[derive(Debug, StructOpt, PartialEq, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Board {
    #[structopt(skip)]
    pub id: u16,

    #[structopt(skip)]
    pub board: String,

    // #[structopt(long, default_value, env, hide_env_values = true)]
    #[structopt(display_order(5), long, default_value("3"), env, hide_env_values = true)]
    pub retry_attempts: u8,

    /// Delay (ms) between fetching each board
    #[structopt(display_order(5), long, default_value("30000"), env, hide_env_values = true)]
    pub interval_boards: u16,

    /// Delay (ms) between fetching each thread
    #[structopt(display_order(5), long, default_value("1000"), env, hide_env_values = true)]
    pub interval_threads: u16,

    /// Download live threads as well
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub with_threads: bool,

    /// Download archived threads as well
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

    /// Enable archiving the live threads until it's deleted or archived
    // #[structopt(long, env, hide_env_values = true)]
    #[structopt(display_order(5), long)]
    pub watch_threads: bool,
}

impl Default for Board {
    fn default() -> Self {
        Self {
            id:               0,
            board:            String::new(),
            retry_attempts:   3,
            interval_boards:  30000,
            interval_threads: 1000,
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
// #[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
// #[serde(default)]
// pub(crate) struct OptInner {
//     pub board_settings: Board,
// }

// impl Default for OptInner {
//     fn default() -> Self {
//         Self {
//             board_settings: Board::default(),
//         }
//     }
// }

fn boards_cli_string(board: &str) -> Result<Board> {
    let board = board.to_lowercase();
    // if !board.is_empty() && (board.chars().all(|c| c.is_ascii_alphabetic()) || board == "3" || board
    // == "r9k" || board == "s4s") {
    if !board.is_empty() {
        // This will get patched at post processing so it's ok to use default here
        let mut b = Board::default();
        b.board = board;
        Ok(b)
    } else {
        Err(eyre!("Invalid board format `{}`", board))
    }
}

fn threads_cli_string(thread: &str) -> Result<String> {
    let split: Vec<&str> = thread.split('/').filter(|s| !s.is_empty()).collect();
    if split.len() == 2 {
        let no = split[1].parse::<u64>();
        match no {
            Ok(n) => Ok(thread.into()),
            Err(e) => Err(eyre!("Invalid thread `{}` for `/{}/{}`", split[1], split[0], split[1])),
        }
    } else {
        Err(eyre!("Invalid thread format `{}`", thread))
    }
}

// fn threads_cli(threads: &str) -> Vec<&str> {
//     let mut v: Vec<&str> = threads.split(",").filter(|s| s.chars().all(|c|
// c.is_ascii_alphanumeric())).filter(|s| !s.is_empty()).collect();     v.dedup();
//     v
// }

// fn boards_cli(threads: &str) -> Vec<&str> {
//     let mut v: Vec<&str> = threads.split(",").filter(|s| s.chars().all(|c|
// c.is_ascii_alphabetic())).filter(|s| !s.is_empty()).collect();     v.sort();
//     v.dedup();
//     v
// }

// fn with(arg: &str) -> Result<bool> {
//     Ok(true)
// }

#[cfg(test)]
mod tests {
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

#[derive(Debug, StructOpt, PartialEq, Deserialize, Serialize, Clone)]
// #[structopt(about = "Experimental archiver")]
#[structopt(about)]
#[serde(default)]
// CLI Options
pub struct Opt {
    // Activate debug mode
    // #[structopt(short, long, hidden(true))]
    // pub debug: bool,
    /// Download sequentially rather than concurrently. This sets limit to 1.
    // TODO interleave between boards
    #[structopt(long, display_order(5))]
    pub strict: bool,

    /// Enable Asagi mode to use Ena as an Asagi drop-in
    #[structopt(long("asagi"), display_order(5))]
    pub asagi_mode: bool,

    /// Download everything in the beginning with no limits and then throttle
    #[structopt(long, display_order(5))]
    pub quickstart: bool,

    /// Download from external archives in the beginning
    // #[structopt(long, display_order(5))]
    #[structopt(skip)]
    pub start_with_archives: bool,

    /// Use config file or pass `-` to read from stdin
    #[structopt(display_order(1), short, long, parse(from_os_str), default_value = "config.yml", env, hide_env_values = true)]
    pub config: PathBuf,

    /// Get boards [example: a,b,c]
    #[structopt(display_order(2), short, long,  multiple(true), required_unless("config"),  use_delimiter = true, parse(try_from_str = boards_cli_string),  env, hide_env_values = true)]
    pub boards: Vec<Board>,
    
    /// Exclude boards (Only applies to boards from boardslist, not threadslist) [example: a,b,c]
    #[structopt(display_order(2), long("exclude-boards"),  multiple(true), required(false),  use_delimiter = true, parse(try_from_str = boards_cli_string),  env, hide_env_values = true)]
    pub boards_excluded: Vec<Board>,

    /// Get threads
    ///
    /// First specify the board, then all the threads belonging to that board.  
    /// Comma delimited: /a/12345,/a/1487823,/b/134654,/c/13478,/d/134798   
    /// Space delimited: /a/12345 /a/1487823 /b/134654 /c/13478 /d/134798  
    ///   
    #[structopt(verbatim_doc_comment, display_order(3), short, long, multiple(true), required_unless("config"), use_delimiter = true, parse(try_from_str = threads_cli_string),  env, hide_env_values = true )]
    pub threads: Vec<String>,

    /// Set site
    #[structopt(hidden(true), display_order(4), short, long, default_value = "4chan", env)]
    pub site: String,

    /// Limit concurrency
    #[structopt(display_order(5), long, default_value = "151", env, hide_env_values = true)]
    pub limit: u32,

    /// Media download location
    #[structopt(display_order(5), short, long, parse(from_os_str), default_value = "assets/media", env, hide_env_values = true, hide_default_value(true))]
    pub media_dir: PathBuf,

    /// Media storage type
    ///
    /// Possible values: flatfiles, database, seaweedfs
    // #[structopt(display_order(5),  long, possible_values(&[MediaStorage::FlatFiles.into(),MediaStorage::Database.into(),MediaStorage::SeaweedFS.into()]), default_value, env, hide_env_values = true, hide_possible_values(true))]
    #[structopt(skip)]
    pub media_storage: MediaStorage,

    /// # of greenthreads to get media
    #[structopt(display_order(5), short = "j", long, default_value = "151", env, hide_env_values = true)]
    pub media_threads: u32,

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

    #[structopt(skip)]
    pub timescaledb: Option<TimescaleSettings>,

    #[structopt(skip)]
    pub proxies: Option<Vec<ProxySettings>>,
    /*
    /// Set speed
    // we don't want to name it "speed", need to look smart
    #[structopt(short = "v", long = "velocity", default_value = "42")]
    speed: f64,

    /// Input file
    #[structopt(parse(from_os_str))]
    input: PathBuf,

    /// Output file, stdout if not present
    #[structopt(parse(from_os_str))]
    output: Option<PathBuf>,

    /// Where to write the output: to `stdout` or `file`
    #[structopt(short)]
    out_type: String,

    /// File name: only required when `out` is set to `file`
    #[structopt(name = "FILE", required_if("out_type", "file"))]
    file_name: Option<String>,*/
}

impl Default for Opt {
    fn default() -> Self {
        Self {
            strict:              false,
            asagi_mode:          false,
            quickstart:          false,
            start_with_archives: false,
            config:              "config.yml".into(),
            boards:              vec![],
            boards_excluded:              vec![],
            threads:             vec![],
            site:                "4chan".into(),
            limit:               151,
            media_dir:           "assets/media".into(),
            media_storage:       MediaStorage::FlatFiles,
            media_threads:       151,
            user_agent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0".into(),
            api_url:             "https://a.4cdn.org".parse().unwrap(),
            media_url:           "https://i.4cdn.org".parse().unwrap(),
            board_settings:      Board::default(),
            database:            DatabaseOpt::default(),
            timescaledb:         None,
            proxies:             None,
        }
    }
}
#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct TimescaleSettings {
    pub column: String,
    pub every:  String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Default)]
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
    type Err = color_eyre::Report;

    fn from_str(storage: &str) -> Result<Self, Self::Err> {
        if storage == &MediaStorage::FlatFiles.to_string() {
            Ok(MediaStorage::FlatFiles)
        } else if storage == &MediaStorage::Database.to_string() {
            Ok(MediaStorage::Database)
        } else if storage == &MediaStorage::SeaweedFS.to_string() {
            Ok(MediaStorage::SeaweedFS)
        } else {
            Err(eyre!("Unknown MediaStorage: {}", storage))
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
            panic!(eyre!("Unkown MediaStorage: {}", storage))
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


#[derive(Debug, StructOpt, PartialEq, Deserialize, Serialize, Clone)]
pub struct DatabaseOpt {
    /// Set database url
    #[structopt(display_order(10), long = "db-url", env = "ENA_DATABASE_URL", hide_env_values = true, hide_default_value(true))]
    pub url: Option<String>,

    /// Set database engine
    #[structopt(display_order(11), long, default_value("postgresql"), env = "ENA_DATABASE_ENGINE", hide_env_values = true, hide_default_value(true))]
    pub engine: String,

    /// Set database name
    #[structopt(display_order(12), long, default_value("ena"), env = "ENA_DATABASE_NAME", hide_env_values = true, hide_default_value(true))]
    #[serde(rename = "database")]
    pub name: String,

    /// Set database schema
    #[structopt(display_order(13), long, default_value("public"), env = "ENA_DATABASE_SCHEMA", hide_env_values = true,  hide_default_value(true))]
    pub schema: String,

    /// Set database host
    #[structopt(display_order(14), long, default_value("localhost"), env = "ENA_DATABASE_HOST", hide_env_values = true, hide_default_value(true))]
    pub host: String,

    /// Set database port
    #[structopt(display_order(15), long, default_value("5432"), env = "ENA_DATABASE_PORT", hide_env_values = true,  hide_default_value(true))]
    pub port: u16,

    /// Set database user
    #[structopt(display_order(16), long, default_value("postgres"), env = "ENA_DATABASE_USERNAME", hide_env_values = true, hide_default_value(true))]
    pub username: String,

    /// Set database password
    #[structopt(display_order(17), long, default_value("zxc"), env = "ENA_DATABASE_PASSWORD", hide_env_values = true,  hide_default_value(true))]
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
            url:      None, //Some("postgresql://postgres:zxc@localhost:5432/ena".into()),
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
