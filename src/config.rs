use crate::{sql::Database, YotsubaBoard};
use anyhow::{Context, Result};
use enum_iterator::IntoEnumIterator;
use serde::{self, Deserialize, Serialize};
use std::env::var;

pub fn version() -> String {
    option_env!("CARGO_PKG_VERSION").unwrap_or("?.?.?").to_string()
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)] // https://github.com/serde-rs/serde/pull/780
pub struct Config {
    pub settings:       Settings,
    pub board_settings: BoardSettings,
    pub boards:         Vec<BoardSettings>
}

impl Config {
    pub fn pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}

/// This is for patching user boards
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)] // https://github.com/serde-rs/serde/pull/780
struct ConfigInner {
    settings:       Settings,
    board_settings: BoardSettingsInner,
    boards:         Vec<BoardSettingsInner>
}

impl Default for ConfigInner {
    fn default() -> Self {
        Self { settings:       Settings::default(),
               board_settings: BoardSettingsInner::default(),
               boards:         vec![] }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self { settings:       Settings::default(),
               board_settings: BoardSettings::default(),
               boards:         vec![] }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct Settings {
    pub engine:     Database,
    pub database:   String,
    pub schema:     String,
    pub host:       String,
    pub port:       u32,
    pub username:   String,
    pub password:   String,
    pub charset:    String,
    pub path:       String,
    pub user_agent: String,
    pub api_url:    String,
    pub media_url:  String,
    pub asagi_mode: bool
}

impl Default for Settings {
    fn default() -> Self {
        Self { engine:     var("ENA_DATABASE").ok()
                                              .filter(|s| {
                                                  Database::into_enum_iter().any(|z| {
                                                                                z.to_string() == *s
                                                                            })
                                              })
                                              .unwrap_or(Database::PostgreSQL.into())
                                              .into(),
               database:   var("ENA_DATABASE_NAME").unwrap_or("archive_ena".into()),
               schema:     var("ENA_DATABASE_SCHEMA").ok()
                                                     .filter(|s| !String::is_empty(s))
                                                     .unwrap_or("public".into()),
               host:       var("ENA_DATABASE_HOST").unwrap_or("localhost".into()),
               port:       var("ENA_DATABASE_PORT").ok()
                                                   .map(|a| a.parse::<u32>().ok())
                                                   .flatten()
                                                   .unwrap_or(5432),
               username:   var("ENA_DATABASE_USERNAME").unwrap_or("postgres".into()),
               password:   var("ENA_DATABASE_PASSWORD").unwrap_or("pass".into()),
               charset:    var("ENA_DATABASE_CHARSET").unwrap_or("utf8".into()),
               path:       var("ENA_PATH").unwrap_or("./archive".into())
                                          .trim_end_matches('/')
                                          .trim_end_matches('\\')
                                          .into(),
               user_agent: format!("{}/{}",
                                   var("CARGO_PKG_NAME").unwrap_or("ena".into()),
                                   var("CARGO_PKG_VERSION").unwrap_or("0.0.0".into())),
               api_url:    var("ENA_API_URL").unwrap_or("http://a.4cdn.org".into()),
               media_url:  var("ENA_MEDIA_URL").unwrap_or("http://i.4cdn.org".into()),
               asagi_mode: var("ENA_ASAGI_MODE").ok()
                                                .map(|a| a.parse::<bool>().ok())
                                                .flatten()
                                                .unwrap_or(false) }
    }
}

pub fn default_headers(
    user_agent: &str)
    -> Result<reqwest::header::HeaderMap, reqwest::header::InvalidHeaderValue> {
    let mut hm = reqwest::header::HeaderMap::with_capacity(2);
    hm.insert(reqwest::header::USER_AGENT, reqwest::header::HeaderValue::from_str(user_agent)?);
    Ok(hm)
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

#[derive(Deserialize, Serialize, Debug, Clone)]
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
impl Default for BoardSettings {
    fn default() -> Self {
        let config: ConfigInner = read_json2("ena_config.json");
        let b = config.board_settings;
        BoardSettings { board:               b.board,
                        retry_attempts:      b.retry_attempts,
                        refresh_delay:       b.refresh_delay,
                        throttle_millisec:   b.throttle_millisec,
                        download_archives:   b.download_archives,
                        download_media:      b.download_media,
                        download_thumbnails: b.download_thumbnails,
                        keep_media:          b.keep_media,
                        keep_thumbnails:     b.keep_thumbnails }
    }
}

impl Default for BoardSettingsInner {
    fn default() -> Self {
        Self { board:               YotsubaBoard::None,
               retry_attempts:      3,
               refresh_delay:       20,
               throttle_millisec:   1000,
               download_archives:   true,
               download_media:      false,
               download_thumbnails: false,
               keep_media:          false,
               keep_thumbnails:     false }
    }
}

/// Reads a config file
pub fn read_config(config_path: &str) -> (Config, String) {
    let config: Config = read_json2(config_path);
    let conn_url = format!("{engine}://{username}:{password}@{host}:{port}/{database}",
                           engine = &config.settings.engine,
                           username = &config.settings.username,
                           password = &config.settings.password,
                           host = &config.settings.host,
                           port = &config.settings.port,
                           database = &config.settings.database);
    (config, conn_url)
}

/// Reads a json file
#[allow(dead_code)]
pub fn read_json<T>(path: &str) -> Option<T>
    where T: serde::de::DeserializeOwned {
    std::fs::File::open(path).and_then(|file| Ok(std::io::BufReader::new(file)))
                             .ok()
                             .and_then(|reader| serde_json::from_reader(reader).ok())
                             .flatten()
}
pub fn read_json2<T>(path: &str) -> T
    where T: serde::de::DeserializeOwned {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);
    let res = serde_json::from_reader(reader).context(format!("\nPlease check your settings."))
                                             .expect("Reading configuration");
    res
}
