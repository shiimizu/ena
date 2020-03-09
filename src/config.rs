use serde::{self, Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    pub settings: InnerSettings,
    pub board_settings: BoardSettings, //pub boards: Vec<BoardSettings>
    pub boards: Vec<BoardSettingsSafe>
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            settings: InnerSettings::default(),
            board_settings: BoardSettings::default(),
            boards: vec![]
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InnerSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub engine: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub database: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub schema: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub host: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub port: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub username: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub password: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub charset: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub user_agent: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub api_url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub media_url: Option<String>
}

impl Default for InnerSettings {
    fn default() -> Self {
        Self {
            engine: Some("postgresql".into()),
            database: Some("archive_ena".into()),
            schema: Some("public".into()),
            host: Some("localhost".into()),
            port: Some(5432),
            username: Some("postgres".into()),
            password: Some("pass".into()),
            charset: Some("utf8".into()),
            path: Some("./archive".into()),
            user_agent: Some(
                [
                    option_env!("CARGO_PKG_NAME").unwrap_or("ena"),
                    "/",
                    option_env!("CARGO_PKG_VERSION").unwrap_or("0.0.0")
                ]
                .concat()
            ),
            api_url: Some(option_env!("ENA_API_URL").unwrap_or("http://a.4cdn.org").into()),
            media_url: Some(option_env!("ENA_MEDIA_URL").unwrap_or("http://i.4cdn.org").into())
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
pub struct BoardSettings {
    pub board: String,
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
            board: "".into(),
            retry_attempts: 4,
            refresh_delay: 30,
            throttle_millisec: 1000,
            download_media: false,
            download_thumbnails: false,
            keep_media: false,
            keep_thumbnails: false
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BoardSettingsSafe {
    pub board: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub retry_attempts: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub refresh_delay: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub throttle_millisec: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub download_media: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub download_thumbnails: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub keep_media: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub keep_thumbnails: Option<bool>
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Boards {
    boards: Vec<BoardSettings>
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
