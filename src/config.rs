//! Configuration.
//!
//! Used to parse the config file or read from environment variables.  
//! Also supplied are various helper functions for a CLI program.
use crate::{
    enums::{StringExt, YotsubaBoard},
    sql::Database
};
use anyhow::{Context, Result};
use enum_iterator::IntoEnumIterator;
use serde::{self, Deserialize, Serialize};
use std::{
    env::var,
    iter::{Chain, Repeat, StepBy, Take}
};

use once_cell::sync::OnceCell;
pub static CONFIG_CONTENTS: OnceCell<String> = OnceCell::new();

/// Display an ascii art with the crate version
pub fn display() {
    println!(
        r#"
    ⣿⠟⣽⣿⣿⣿⣿⣿⢣⠟⠋⡜⠄⢸⣿⣿⡟⣬⢁⠠⠁⣤⠄⢰⠄⠇⢻⢸
    ⢏⣾⣿⣿⣿⠿⣟⢁⡴⡀⡜⣠⣶⢸⣿⣿⢃⡇⠂⢁⣶⣦⣅⠈⠇⠄⢸⢸
    ⣹⣿⣿⣿⡗⣾⡟⡜⣵⠃⣴⣿⣿⢸⣿⣿⢸⠘⢰⣿⣿⣿⣿⡀⢱⠄⠨⢸       ____
    ⣿⣿⣿⣿⡇⣿⢁⣾⣿⣾⣿⣿⣿⣿⣸⣿⡎⠐⠒⠚⠛⠛⠿⢧⠄⠄⢠⣼      /\  _`\
    ⣿⣿⣿⣿⠃⠿⢸⡿⠭⠭⢽⣿⣿⣿⢂⣿⠃⣤⠄⠄⠄⠄⠄⠄⠄⠄⣿⡾      \ \ \L\_     ___      __
    ⣼⠏⣿⡏⠄⠄⢠⣤⣶⣶⣾⣿⣿⣟⣾⣾⣼⣿⠒⠄⠄⠄⡠⣴⡄⢠⣿⣵       \ \  __\  /' _ `\  /'__`\
    ⣳⠄⣿⠄⠄⢣⠸⣹⣿⡟⣻⣿⣿⣿⣿⣿⣿⡿⡻⡖⠦⢤⣔⣯⡅⣼⡿⣹        \ \ \___\/\ \/\ \/\ \L\.\_
    ⡿⣼⢸⠄⠄⣷⣷⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣕⡜⡌⡝⡸⠙⣼⠟⢱⠏         \ \____/\ \_\ \_\ \__/.\_\
    ⡇⣿⣧⡰⡄⣿⣿⣿⣿⡿⠿⠿⠿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣋⣪⣥⢠⠏⠄          \/___/  \/_/\/_/\/__/\/_/   v{}
    ⣧⢻⣿⣷⣧⢻⣿⣿⣿⡇⠄⢀⣀⣀⡙⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠂⠄⠄
    ⢹⣼⣿⣿⣿⣧⡻⣿⣿⣇⣴⣿⣿⣿⣷⢸⣿⣿⣿⣿⣿⣿⣿⣿⣰⠄⠄⠄
    ⣼⡟⡟⣿⢸⣿⣿⣝⢿⣿⣾⣿⣿⣿⢟⣾⣿⣿⣿⣿⣿⣿⣿⣿⠟⠄⡀⡀        A lightweight 4chan archiver (¬ ‿ ¬ )
    ⣿⢰⣿⢹⢸⣿⣿⣿⣷⣝⢿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠛⠉⠄⠄⣸⢰⡇
    ⣿⣾⣹⣏⢸⣿⣿⣿⣿⣿⣷⣍⡻⣛⣛⣛⡉⠁⠄⠄⠄⠄⠄⠄⢀⢇⡏⠄
    "#,
        version()
    )
}

/// Upper level configuration hold json fields and values
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)] // https://github.com/serde-rs/serde/pull/780
pub struct Config {
    pub settings:       Settings,
    pub board_settings: BoardSettings,
    pub boards:         Vec<BoardSettings>
}

impl Default for Config {
    fn default() -> Self {
        Self {
            settings:       Settings::default(),
            board_settings: BoardSettings::default(),
            boards:         vec![]
        }
    }
}

#[allow(dead_code)]
impl Config {
    /// Return a pretty json by calling [`serde_json::to_string_pretty`]
    pub fn pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    /// Return the json as a string by calling [`serde_json::to_string`]
    pub fn string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

/// Archiver settings
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct Settings {
    pub engine:      Database,
    pub database:    String,
    pub schema:      String,
    pub host:        String,
    pub port:        u32,
    pub username:    String,
    pub password:    String,
    pub charset:     String,
    pub path:        String,
    pub db_url:      String,
    pub user_agent:  String,
    pub api_url:     String,
    pub media_url:   String,
    pub asagi_mode:  bool,
    pub strict_mode: bool
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            engine:      var("ENA_DATABASE")
                .ok()
                .filter(|s| {
                    Database::into_enum_iter()
                        .any(|z| z.to_string().to_lowercase() == *s.to_lowercase())
                })
                .map(|s| s.into())
                .unwrap_or(Database::PostgreSQL),
            database:    var("ENA_DATABASE_NAME").unwrap_or("archive_ena".into()),
            schema:      var("ENA_DATABASE_SCHEMA")
                .ok()
                .filter(|s| !String::is_empty(s))
                .unwrap_or("public".into()),
            host:        var("ENA_DATABASE_HOST").unwrap_or("localhost".into()),
            port:        var("ENA_DATABASE_PORT")
                .ok()
                .map(|a| a.parse::<u32>().ok())
                .flatten()
                .unwrap_or(5432),
            username:    var("ENA_DATABASE_USERNAME").unwrap_or("postgres".into()),
            password:    var("ENA_DATABASE_PASSWORD").unwrap_or("pass".into()),
            charset:     var("ENA_DATABASE_CHARSET").unwrap_or("utf8".into()),
            path:        var("ENA_PATH")
                .unwrap_or("./archive".into())
                .trim_end_matches('/')
                .trim_end_matches('\\')
                .into(),
            db_url:      var("ENA_DB_URL").unwrap_or("".into()),
            user_agent:  format!(
                "{}/{}",
                var("CARGO_PKG_NAME").unwrap_or("ena".into()),
                var("CARGO_PKG_VERSION").unwrap_or("0.0.0".into())
            ),
            api_url:     var("ENA_API_URL").unwrap_or("http://a.4cdn.org".into()),
            media_url:   var("ENA_MEDIA_URL").unwrap_or("http://i.4cdn.org".into()),
            asagi_mode:  var("ENA_ASAGI_MODE")
                .ok()
                .map(|a| a.parse::<bool>().ok())
                .flatten()
                .unwrap_or(false),
            strict_mode: var("ENA_STRICT_MODE")
                .ok()
                .map(|a| a.parse::<bool>().ok())
                .flatten()
                .unwrap_or(true)
        }
    }
}

/// Settings for a board
#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct BoardSettings {
    pub board:               YotsubaBoard,
    pub retry_attempts:      u16,
    pub refresh_delay:       u16,
    pub throttle_millisec:   u32,
    pub download_archives:   bool,
    pub download_media:      bool,
    pub download_thumbnails: bool,
    pub keep_media:          bool,
    pub keep_thumbnails:     bool
}

/// This default method will call if there's any missing fields. That's why the board field is
/// ommited in the config json. This is why Yotsuba::None will go through, its not being
/// deserialized or parsed by serde_json, the default is merely being inserted, and no checks are
/// done. See the overriden impl of `Deserialize` for `YotsubaBoard`.  
/// It's overridden to ignore `YotsubaBoard::None`.
impl Default for BoardSettings {
    fn default() -> Self {
        // Use a deserialized BoardSettings as base for all other boards
        let new: BoardSettingsInner = serde_json::from_str::<ConfigInner>(
            &CONFIG_CONTENTS.get().expect("Config is not initialized")
        )
        .unwrap()
        .board_settings;
        // read_json::<ConfigInner>().board_settings;
        BoardSettings {
            board:               new.board,
            retry_attempts:      new.retry_attempts,
            refresh_delay:       new.refresh_delay,
            throttle_millisec:   new.throttle_millisec,
            download_archives:   new.download_archives,
            download_media:      new.download_media,
            download_thumbnails: new.download_thumbnails,
            keep_media:          new.keep_media,
            keep_thumbnails:     new.keep_thumbnails
        }
    }
}

/// Use a deserialized BoardSettings as a base for all other BoardSettings
/// Another struct is used to prevent a recursive loop in deserialization
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
struct ConfigInner {
    settings:       Settings,
    board_settings: BoardSettingsInner,
    boards:         Vec<BoardSettingsInner>
}

impl Default for ConfigInner {
    fn default() -> Self {
        Self {
            settings:       Settings::default(),
            board_settings: BoardSettingsInner::default(),
            boards:         vec![]
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
struct BoardSettingsInner {
    board:               YotsubaBoard,
    retry_attempts:      u16,
    refresh_delay:       u16,
    throttle_millisec:   u32,
    download_archives:   bool,
    download_media:      bool,
    download_thumbnails: bool,
    keep_media:          bool,
    keep_thumbnails:     bool
}

impl Default for BoardSettingsInner {
    fn default() -> Self {
        Self {
            board:               YotsubaBoard::None,
            retry_attempts:      3,
            refresh_delay:       20,
            throttle_millisec:   1000,
            download_archives:   true,
            download_media:      false,
            download_thumbnails: false,
            keep_media:          false,
            keep_thumbnails:     false
        }
    }
}

/// Read a [`Config`] file
#[allow(unused_assignments)]
pub fn read_config(c: Config) -> Config {
    // Normally we'd be done here
    // read_json(config_path)

    // This is all to accomodate for a DB URL
    let mut config: Config = c; //read_json(config_path);
    let mut settings = config.settings;
    let mut result: Option<Config> = None;

    // Parse db url and extract contents from it
    let mut s = settings.clone().db_url;
    s.clone().as_str().matches(|c: char| !c.is_alphanumeric()).for_each(|c| {
        s = s.replace(c, " ");
    });
    let v: Vec<String> = s.split_ascii_whitespace().map(str::to_string).collect();

    // we have db url, try to extract it to use its values to update the rest
    // we dont have db url, use our values to update the db url
    //
    // Go here if there's a DB URL
    if v.len() == 6 && s != "" {
        // TRY to update our values from the DB URL
        let mut iter = v.iter();
        settings.engine = iter.next().unwrap_or(&settings.engine.to_string()).to_owned().into();
        settings.username = iter.next().unwrap_or(&settings.username.to_string()).into();
        settings.password = iter.next().unwrap_or(&settings.password).into();
        settings.host = iter.next().unwrap_or(&settings.host).into();
        settings.port = iter
            .next()
            .unwrap_or(&settings.port.to_string())
            .parse::<u32>()
            .unwrap_or(settings.port);
        settings.database = iter.next().unwrap_or(&settings.database).into();
        println!("INSIDE {}", settings.engine);
        config.settings = settings;
        result = Some(config);
    } else {
        // Improper DB URL, use values from our own to construct it
        // No DB URL found, update our DB URL with our own values
        settings.db_url = format!(
            "{engine}://{username}:{password}@{host}:{port}/{database}",
            engine = &settings.engine.base().to_string().to_lowercase(),
            username = &settings.username,
            password = &settings.password,
            host = &settings.host,
            port = &settings.port,
            database = &settings.database
        );
        config.settings = settings;
        result = Some(config);
    }

    let mut json: serde_json::Value = serde_json::to_value(result.unwrap()).unwrap();

    // Remove the `None` board in the example BoardSettings to prevent the deserializer from
    // touching it.
    let bs = json.get_mut("boardSettings").unwrap().as_object_mut().unwrap();
    bs.remove("board").unwrap();

    let mut computed_config: Config = serde_json::from_value(json).unwrap();
    if computed_config.settings.asagi_mode
        || computed_config.settings.engine.base() == Database::MySQL
    {
        computed_config.settings.schema = computed_config.settings.database.clone();
    }
    computed_config
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

/// Return the value of the `ENA_RESUME` environment variable
///
/// # Example
///
/// ```
/// use ena::config;
/// let resume = config::ena_resume();
/// ```
pub fn ena_resume() -> bool {
    var("ENA_RESUME").ok().map(|a| a.parse::<bool>().ok()).flatten().unwrap_or(false)
}

/// Check to see if the first argument passed to the program is `-v` or `--version`  
///
/// Then display build information
///
/// # Example
///
/// ```
/// use ena::config;
/// config::check_version();
/// ```
pub fn check_version() {
    std::env::args().nth(1).filter(|arg| matches!(arg.as_str(), "-v" | "--version")).map(|_| {
        display_full_version();
        std::process::exit(0)
    });
}

/// Display the build information
///
/// # Example
///
/// ```
/// use ena::config;
/// config::display_full_version();
/// ```
pub fn display_full_version() {
    println!(
        "{} v{}-{}\n    {}",
        env!("CARGO_PKG_NAME").to_string().capitalize(),
        env!("CARGO_PKG_VERSION"),
        env!("VERGEN_SHA_SHORT"),
        env!("CARGO_PKG_DESCRIPTION")
    );
    println!("\nBUILD-INFO:");
    println!("    target                  {}", env!("VERGEN_TARGET_TRIPLE"));
    println!("    timestamp               {}", env!("VERGEN_BUILD_TIMESTAMP"));
    println!("    revision                {}", env!("VERGEN_SHA"));
}

pub fn display_help() {
    println!(
        "{main} {version}",
        main = env!("CARGO_PKG_NAME"),
        version = env!("CARGO_PKG_VERSION")
    );
    println!("\nFLAGS:");
    println!("    -h, --help              Prints help information");
    println!("    -V, --version           Prints version information");
    println!("\nOPTIONS:");
    println!("    -c, --config            Specify a config file or pass '-' for stdin");
}

/// Create an iterator that mimics the thread refresh system  
///
/// It repeats indefintely so `take` is required to limit how many `step_by`.  
/// If the stream has reached or passed its last value, it will keep repeating that last value.  
/// If an initial `refreshDelay` was set to `20`, `5` is added to each and subsequent requests
/// that return `NOT_MODIFIED`. If the next request is `OK`, the stream can be reset back to it's
/// initial value by calling `clone()` on it.
///
/// # Arguments
///
/// * `initial` - Initial value in seconds
/// * `step_by` - Add this much every time `next()` is called
/// * `take`    - Limit this to how many additions to make from `step_by`
///
/// # Example
///
/// ```
/// use ena::config;
/// let orig = config::refresh_rate(20, 5, 10);
/// let mut rate = orig.clone();
/// rate.next(); // 20
/// rate.next(); // 25
/// rate.next(); // 30
/// //
/// /* continued calls to rate.next(); */
/// rate.next(); // 75
/// rate.next(); // 75 .. repeating
///
/// rate = orig.clone();
/// rate.next(); // 20
/// ```
pub fn refresh_rate(
    initial: u16, step_by: usize, take: usize
) -> Chain<Take<StepBy<std::ops::RangeFrom<u16>>>, Repeat<u16>> {
    let base = (initial..).step_by(step_by).take(take);
    let repeat = std::iter::repeat(base.clone().last().unwrap());
    let ratelimit = base.chain(repeat);
    ratelimit
}

/// Safe read a json file
#[allow(dead_code)]
pub fn read_json_try<T>(path: &str) -> Option<T>
where T: serde::de::DeserializeOwned {
    std::fs::File::open(path)
        .and_then(|file| Ok(std::io::BufReader::new(file)))
        .ok()
        .and_then(|reader| serde_json::from_reader(reader).ok())
        .flatten()
}

/// Read a json file
///
/// # Arguments
///
/// * `path` - The path to the file
///
/// # Example
///
/// ```
/// use ena::config::*;
/// let config: Config = read_json(config_path);
/// ```
pub fn read_json<T>(path: &str) -> T
where T: serde::de::DeserializeOwned {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);
    let res = serde_json::from_reader(reader)
        .context(format!("\nPlease check your settings."))
        .expect("Reading configuration");
    res
}

/// Create the default headers for [`reqwest::Client`]
///
/// This is used to create one with a user agent.
/// # Arguments
///
/// * `user_agent` - A specified user agent
///
/// # Example
///
/// ```
/// use ena::config::*;
/// let headers =
///     default_headers("Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0");
/// ```
pub fn default_headers(
    user_agent: &str
) -> Result<reqwest::header::HeaderMap, reqwest::header::InvalidHeaderValue> {
    let mut hm = reqwest::header::HeaderMap::with_capacity(2);
    hm.insert(reqwest::header::USER_AGENT, reqwest::header::HeaderValue::from_str(user_agent)?);
    Ok(hm)
}
