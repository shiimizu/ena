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
    pub fn pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    pub fn string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

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
/// It's overridden to ignore YotsubaBoard::None.
impl Default for BoardSettings {
    fn default() -> Self {
        // Use a deserialized BoardSettings as base for all other boards
        let new: BoardSettingsInner = read_json2::<ConfigInner>("ena_config.json").board_settings;
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

/// Reads a config file
#[allow(unused_assignments)]
pub fn read_config(config_path: &str) -> Config {
    // Normally we'd be done here
    // read_json2(config_path)

    // This is all to accomodate for a DB URL
    let mut config: Config = read_json2(config_path);
    let mut settings = config.settings;
    let mut result: Option<Config> = None;

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

    let computed_config: Config = serde_json::from_value(json).unwrap();
    computed_config
}

pub fn version() -> String {
    option_env!("CARGO_PKG_VERSION").unwrap_or("?.?.?").to_string()
}

pub fn ena_resume() -> bool {
    var("ENA_RESUME").ok().map(|a| a.parse::<bool>().ok()).flatten().unwrap_or(false)
}

pub fn check_version() {
    std::env::args().nth(1).filter(|arg| matches!(arg.as_str(), "-v" | "--version")).map(|_| {
        display_full_version();
        std::process::exit(0)
    });
}

pub fn display_full_version() {
    println!(
        "{} v{}-{}\n    {}",
        env!("CARGO_PKG_NAME").to_string().capitalize(),
        env!("CARGO_PKG_VERSION"),
        env!("VERGEN_SHA_SHORT"),
        env!("CARGO_PKG_DESCRIPTION")
    );
    println!("\nBuild info:\n    {}", env!("VERGEN_TARGET_TRIPLE"));
    println!("    {}", env!("VERGEN_BUILD_TIMESTAMP"));
    println!("    {}", env!("VERGEN_SHA"));
}

pub fn refresh_rate(
    initial: u16, step_by: usize, take: usize
) -> Chain<Take<StepBy<std::ops::RangeFrom<u16>>>, Repeat<u16>> {
    let base = (initial..).step_by(step_by).take(take);
    let repeat = std::iter::repeat(base.clone().last().unwrap());
    let ratelimit = base.chain(repeat);
    ratelimit
}

/// Reads a json file
#[allow(dead_code)]
pub fn read_json<T>(path: &str) -> Option<T>
where T: serde::de::DeserializeOwned {
    std::fs::File::open(path)
        .and_then(|file| Ok(std::io::BufReader::new(file)))
        .ok()
        .and_then(|reader| serde_json::from_reader(reader).ok())
        .flatten()
}
pub fn read_json2<T>(path: &str) -> T
where T: serde::de::DeserializeOwned {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);
    let res = serde_json::from_reader(reader)
        .context(format!("\nPlease check your settings."))
        .expect("Reading configuration");
    res
}

pub fn default_headers(
    user_agent: &str
) -> Result<reqwest::header::HeaderMap, reqwest::header::InvalidHeaderValue> {
    let mut hm = reqwest::header::HeaderMap::with_capacity(2);
    hm.insert(reqwest::header::USER_AGENT, reqwest::header::HeaderValue::from_str(user_agent)?);
    Ok(hm)
}
