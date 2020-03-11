use crate::YotsubaBoard;
use serde::{self, Deserialize, Serialize};
use std::env::var;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)] // https://github.com/serde-rs/serde/pull/780
pub struct Config {
    pub settings: Settings,
    pub board_settings: BoardSettings,
    pub boards: Vec<BoardSettings>
}

impl Default for Config {
    fn default() -> Self {
        Self {
            settings: Settings::default(),
            board_settings: BoardSettings::default(),
            boards: vec![]
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct Settings {
    pub engine: String,
    pub database: String,
    pub schema: String,
    pub host: String,
    pub port: u32,
    pub username: String,
    pub password: String,
    pub charset: String,
    pub path: String,
    pub user_agent: String,
    pub api_url: String,
    pub media_url: String
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            engine: var("ENA_DATABASE").unwrap_or("postgresql".into()),
            database: var("ENA_DATABASE_NAME").unwrap_or("archive_ena".into()),
            schema: var("ENA_DATABASE_SCHEMA")
                .ok()
                .filter(|s| !String::is_empty(s))
                .unwrap_or("public".into()),
            host: var("ENA_DATABASE_HOST").unwrap_or("localhost".into()),
            port: var("ENA_DATABASE_PORT")
                .ok()
                .map(|a| a.parse::<u32>().ok())
                .flatten()
                .unwrap_or(5432),
            username: var("ENA_DATABASE_USERNAME").unwrap_or("postgres".into()),
            password: var("ENA_DATABASE_PASSWORD").unwrap_or("pass".into()),
            charset: var("ENA_DATABASE_CHARSET").unwrap_or("utf8".into()),
            path: var("ENA_PATH")
                .unwrap_or("./archive".into())
                .trim_end_matches('/')
                .trim_end_matches('\\')
                .into(),
            user_agent: format!(
                "{}/{}",
                var("CARGO_PKG_NAME").unwrap_or("ena".into()),
                var("CARGO_PKG_VERSION").unwrap_or("0.0.0".into())
            ),
            api_url: var("ENA_API_URL").unwrap_or("http://a.4cdn.org".into()),
            media_url: var("ENA_MEDIA_URL").unwrap_or("http://i.4cdn.org".into())
        }
    }
}

pub fn default_headers(
    user_agent: &str
) -> Result<reqwest::header::HeaderMap, reqwest::header::InvalidHeaderValue> {
    let mut hm = reqwest::header::HeaderMap::with_capacity(2);
    hm.insert(reqwest::header::USER_AGENT, reqwest::header::HeaderValue::from_str(user_agent)?);
    Ok(hm)
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct BoardSettings {
    pub board: YotsubaBoard,
    pub retry_attempts: u16,
    pub refresh_delay: u16,
    pub throttle_millisec: u32,
    pub download_media: bool,
    pub download_thumbnails: bool,
    pub keep_media: bool,
    pub keep_thumbnails: bool
}
impl Default for BoardSettings {
    fn default() -> Self {
        Self {
            board: YotsubaBoard::None,
            retry_attempts: 3,
            refresh_delay: 20,
            throttle_millisec: 1000,
            download_media: false,
            download_thumbnails: false,
            keep_media: false,
            keep_thumbnails: false
        }
    }
}
/// Reads a config file
pub fn read_config(config_path: &str) -> (Config, String) {
    let config: Config = read_json(config_path).unwrap_or_default();
    let conn_url = format!(
        "{engine}://{username}:{password}@{host}:{port}/{database}",
        engine = &config.settings.engine,
        username = &config.settings.username,
        password = &config.settings.password,
        host = &config.settings.host,
        port = &config.settings.port,
        database = &config.settings.database
    );
    (config, conn_url)
}

/// Reads a json file
pub fn read_json<T>(path: &str) -> Option<T>
where T: serde::de::DeserializeOwned {
    std::fs::File::open(path)
        .and_then(|file| Ok(std::io::BufReader::new(file)))
        .ok()
        .and_then(|reader| serde_json::from_reader(reader).ok())
        .flatten()
}
#[allow(dead_code)]
pub fn read_json2<T>(path: &str) -> T
where T: serde::de::DeserializeOwned {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);
    serde_json::from_reader(reader).unwrap()
}
