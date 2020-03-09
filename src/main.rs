#![forbid(unsafe_code)]
#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(irrefutable_let_patterns)]
#![allow(clippy::range_plus_one)]

mod config;
mod request;
mod sql;

use crate::{config::BoardSettings, request::*};
use core::sync::atomic::Ordering;
use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom,
    env, fmt,
    path::Path,
    sync::atomic::AtomicBool
};

use futures::stream::{FuturesUnordered, StreamExt as FutureStreamExt};
use reqwest::{self, StatusCode};
use tokio::{
    runtime::Builder,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    time::{delay_for as sleep, Duration}
};

use anyhow::{anyhow, Context, Result};
use chrono::Local;
// use ctrlc;
use log::*;
// use pretty_env_logger;
// use serde_json;
use sha2::{Digest, Sha256};

fn main() {
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
    ⡇⣿⣧⡰⡄⣿⣿⣿⣿⡿⠿⠿⠿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣋⣪⣥⢠⠏⠄          \/___/  \/_/\/_/\/__/\/_/   v{version}
    ⣧⢻⣿⣷⣧⢻⣿⣿⣿⡇⠄⢀⣀⣀⡙⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠂⠄⠄
    ⢹⣼⣿⣿⣿⣧⡻⣿⣿⣇⣴⣿⣿⣿⣷⢸⣿⣿⣿⣿⣿⣿⣿⣿⣰⠄⠄⠄
    ⣼⡟⡟⣿⢸⣿⣿⣝⢿⣿⣾⣿⣿⣿⢟⣾⣿⣿⣿⣿⣿⣿⣿⣿⠟⠄⡀⡀        A lightweight 4chan archiver (¬ ‿ ¬ )
    ⣿⢰⣿⢹⢸⣿⣿⣿⣷⣝⢿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠛⠉⠄⠄⣸⢰⡇
    ⣿⣾⣹⣏⢸⣿⣿⣿⣿⣿⣷⣍⡻⣛⣛⣛⡉⠁⠄⠄⠄⠄⠄⠄⢀⢇⡏⠄
    "#,
        version = option_env!("CARGO_PKG_VERSION").unwrap_or("?.?.?")
    );

    let start_time = Local::now();
    pretty_env_logger::try_init_timed_custom_env("ENA_LOG").unwrap();

    match Builder::new().enable_all().threaded_scheduler().build() {
        Ok(mut runtime) => runtime.block_on(async {
            if let Err(e) = async_main().await {
                error!("{}", e);
            }
        }),
        Err(e) => error!("{}", e)
    }

    info!(
        "\nStarted on:\t{}\nFinished on:\t{}",
        start_time.to_rfc2822(),
        Local::now().to_rfc2822()
    );
}
#[allow(unused_mut)]
async fn async_main() -> Result<u64, tokio_postgres::error::Error> {
    let config_path = "ena_config.json";
    let config: config::Settings = config::read_json(config_path).unwrap_or_default();
    let conn_url = format!(
        "postgresql://{username}:{password}@{host}:{port}/{database}",
        username = option_env!("ENA_DATABASE_USERNAME")
            .unwrap_or(&config.settings.username.as_ref().unwrap()),
        password = option_env!("ENA_DATABASE_PASSWORD")
            .unwrap_or(&config.settings.password.as_ref().unwrap()),
        host = option_env!("ENA_DATABASE_HOST").unwrap_or(&config.settings.host.as_ref().unwrap()),
        port =
            option_env!("ENA_DATABASE_PORT").unwrap_or(&config.settings.port.unwrap().to_string()),
        database =
            option_env!("ENA_DATABASE_NAME").unwrap_or(&config.settings.database.as_ref().unwrap())
    );

    let (db_client, connection) = tokio_postgres::connect(&conn_url, tokio_postgres::NoTls)
        .await
        .context(format!("\nPlease check your settings. Connection url used: {}", conn_url))
        .expect("Connecting to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    let http_client = reqwest::ClientBuilder::new()
        .default_headers(
            config::default_headers(
                option_env!("ENA_USERAGENT")
                    .unwrap_or_else(|| config.settings.user_agent.as_ref().unwrap().as_str())
            )
            .unwrap()
        )
        .build()
        .expect("Error building the HTTP Client");

    let archiver = YotsubaArchiver::new(http_client, db_client, config).await;
    archiver.listen_to_exit();
    archiver.init_type().await?;
    archiver.init_schema().await;
    archiver.init_metadata().await;
    sleep(Duration::from_millis(1200)).await;

    let boards = &archiver.config.boards;

    // Push each board to queue to be run concurrently
    let mut bv: Vec<BoardSettings> = vec![];
    for bss in boards.into_iter() {
        let mut default = archiver.config.board_settings.to_owned();
        default.board.clear();
        default.board.push_str(&bss.board);
        if let Some(i) = bss.retry_attempts {
            default.retry_attempts = i;
        }
        if let Some(i) = bss.refresh_delay {
            default.refresh_delay = i;
        }
        if let Some(i) = bss.throttle_millisec {
            default.throttle_millisec = i;
        }
        if let Some(i) = bss.download_media {
            default.download_media = i;
        }
        if let Some(i) = bss.download_thumbnails {
            default.download_thumbnails = i;
        }
        if let Some(i) = bss.keep_media {
            default.keep_media = i;
        }
        if let Some(i) = bss.keep_thumbnails {
            default.keep_thumbnails = i;
        }
        // Populate new values using the default as base
        bv.push(default);
    }

    let archiver_ref = &archiver;
    let mut fut2 = FuturesUnordered::new();
    for board in bv.iter() {
        archiver_ref.init_board(&board.board).await;
        archiver_ref.init_views(&board.board).await;

        let (mut tx, mut _rx) = mpsc::unbounded_channel();
        fut2.push(archiver_ref.compute(YotsubaType::Archive, board, Some(tx.clone()), None));
        fut2.push(archiver_ref.compute(YotsubaType::Threads, board, Some(tx.clone()), None));
        // fut2.push(archiver_ref.compute(YotsubaType::Media, board, None,
        // Some(_rx)));
    }

    // Waiting for this task causes a loop that's intended.
    // Run each task concurrently
    while let Some(_) = fut2.next().await {}

    Ok(0)
}

#[derive(Clone)]
pub struct MediaInfo {
    board: BoardSettings,
    thread: u32
}

#[derive(Clone, Copy, PartialEq)]
pub enum YotsubaType {
    Archive,
    Threads,
    Media
}

impl fmt::Display for YotsubaType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            YotsubaType::Archive => write!(f, "archive"),
            YotsubaType::Threads => write!(f, "threads"),
            YotsubaType::Media => write!(f, "media")
        }
    }
}

/// A struct to store variables without using global statics.
/// It also allows passing http client as reference.
pub struct YotsubaArchiver<H: request::HttpClient> {
    client: YotsubaHttpClient<H>,
    conn: tokio_postgres::Client,
    config: config::Settings,
    finished: std::sync::Arc<AtomicBool>
}

impl<H> YotsubaArchiver<H>
where H: request::HttpClient
{
    async fn new(
        http_client: H,
        db_client: tokio_postgres::Client,
        config: config::Settings
    ) -> Self
    {
        Self {
            client: YotsubaHttpClient::new(http_client),
            conn: db_client,
            config,
            finished: std::sync::Arc::new(AtomicBool::new(false))
        }
    }

    fn schema(&self) -> &str {
        option_env!("ENA_DATABASE_SCHEMA")
            .unwrap_or_else(|| self.config.settings.schema.as_ref().unwrap().as_str())
    }

    fn get_path(&self) -> &str {
        option_env!("ENA_PATH")
            .unwrap_or_else(|| self.config.settings.path.as_ref().unwrap())
            .trim_end_matches('/')
            .trim_end_matches('\\')
    }

    fn listen_to_exit(&self) {
        let finished_clone = std::sync::Arc::clone(&self.finished);
        ctrlc::set_handler(move || {
            finished_clone.compare_and_swap(false, true, Ordering::Relaxed);
        })
        .expect("Error setting Ctrl-C handler");
    }

    async fn create_statements(
        &self,
        board: &str,
        thread_type: YotsubaType
    ) -> HashMap<String, tokio_postgres::Statement>
    {
        let mut statements: HashMap<String, tokio_postgres::Statement> = HashMap::new();

        // This function is only called by fetch_board so it'll never be media.
        // let thread_type = match thread_type {
        //     YotsubaType::Archive => "archive",
        //     _ => "threads"
        // };

        for func in [
            "upsert_deleted",
            "upsert_deleteds",
            "upsert_thread",
            "upsert_metadata",
            "media_posts",
            "deleted_and_modified_threads",
            "threads_list",
            "check_metadata_col",
            "combined_threads"
        ]
        .iter()
        {
            statements.insert(
                format!("{}_{}_{}", thread_type, func, board),
                match *func {
                    "upsert_deleted" => {
                        self.conn
                            .prepare_typed(sql::upsert_deleted(self.schema(), board).as_str(), &[
                                tokio_postgres::types::Type::INT8
                            ])
                            .await
                    }
                    "upsert_deleteds" => {
                        self.conn
                            .prepare_typed(sql::upsert_deleteds(self.schema(), board).as_str(), &[
                                tokio_postgres::types::Type::JSONB,
                                tokio_postgres::types::Type::INT8
                            ])
                            .await
                    }
                    "upsert_thread" => {
                        self.conn
                            .prepare_typed(sql::upsert_thread(self.schema(), board).as_str(), &[
                                tokio_postgres::types::Type::JSONB
                            ])
                            .await
                    }
                    "upsert_metadata" => {
                        self.conn
                            .prepare_typed(
                                sql::upsert_metadata(self.schema(), thread_type).as_str(),
                                &[
                                    tokio_postgres::types::Type::TEXT,
                                    tokio_postgres::types::Type::JSONB
                                ]
                            )
                            .await
                    }
                    "media_posts" => {
                        self.conn
                            .prepare_typed(sql::media_posts(self.schema(), board).as_str(), &[
                                tokio_postgres::types::Type::INT8
                            ])
                            .await
                    }
                    "deleted_and_modified_threads" => {
                        self.conn
                            .prepare_typed(
                                sql::deleted_and_modified_threads(
                                    self.schema(),
                                    thread_type == YotsubaType::Threads
                                )
                                .as_str(),
                                &[
                                    tokio_postgres::types::Type::TEXT,
                                    tokio_postgres::types::Type::JSONB
                                ]
                            )
                            .await
                    }
                    "threads_list" => {
                        self.conn
                            .prepare_typed(sql::threads_list(), &[
                                tokio_postgres::types::Type::JSONB
                            ])
                            .await
                    }
                    "check_metadata_col" => {
                        self.conn
                            .prepare_typed(
                                sql::check_metadata_col(self.schema(), thread_type).as_str(),
                                &[tokio_postgres::types::Type::TEXT]
                            )
                            .await
                    }
                    "combined_threads" => {
                        self.conn
                            .prepare_typed(
                                sql::combined_threads(
                                    self.schema(),
                                    board,
                                    thread_type == YotsubaType::Threads
                                )
                                .as_str(),
                                &[
                                    tokio_postgres::types::Type::TEXT,
                                    tokio_postgres::types::Type::JSONB
                                ]
                            )
                            .await
                    }
                    _ => unreachable!()
                }
                .unwrap_or_else(|_| panic!("Error creating prepare statement {}", func))
            );
        }
        statements
    }

    async fn init_type(&self) -> Result<u64, tokio_postgres::error::Error> {
        self.conn.execute(sql::init_type(), &[]).await
    }

    async fn init_schema(&self) {
        self.conn
            .execute(sql::init_schema(self.schema()).as_str(), &[])
            .await
            .unwrap_or_else(|_| panic!("Err creating schema: {}", self.schema()));
    }

    async fn init_metadata(&self) {
        self.conn
            .batch_execute(&sql::init_metadata(self.schema()))
            .await
            .expect("Err creating metadata");
    }

    async fn init_board(&self, board: &str) {
        self.conn
            .batch_execute(&sql::init_board(self.schema(), board))
            .await
            .unwrap_or_else(|_| panic!("Err creating schema: {}", board));
    }

    async fn init_views(&self, board: &str) {
        self.conn
            .batch_execute(&sql::init_views(self.schema(), board))
            .await
            .expect("Err create views");
    }

    /// Converts bytes to json object and feeds that into the query
    async fn upsert_metadata(
        &self,
        board: &str,
        item: &[u8],
        statement: &tokio_postgres::Statement
    ) -> Result<u64>
    {
        Ok(self
            .conn
            .execute(statement, &[&board, &serde_json::from_slice::<serde_json::Value>(item)?])
            .await?)
        //.expect("Err executing sql: upsert_metadata");
    }

    async fn get_media_posts(
        &self,
        thread: u32,
        statement: &tokio_postgres::Statement
    ) -> Result<Vec<tokio_postgres::row::Row>, tokio_postgres::error::Error>
    {
        self.conn.query(statement, &[&i64::try_from(thread).unwrap()]).await
    }

    async fn upsert_hash2(&self, board: &str, no: u64, hash_type: &str, hashsum: Vec<u8>) {
        self.conn
            .execute(sql::upsert_hash(self.schema(), board, no, hash_type).as_str(), &[&hashsum])
            .await
            .expect("Err executing sql: upsert_hash2");
    }

    /// Mark a single post as deleted.
    async fn upsert_deleted(&self, no: u32, statement: &tokio_postgres::Statement) {
        self.conn
            .execute(statement, &[&i64::try_from(no).unwrap()])
            .await
            .expect("Err executing sql: upsert_deleted");
    }

    /// Mark posts from a thread where it's deleted.
    async fn upsert_deleteds(
        &self,
        thread: u32,
        item: &[u8],
        statement: &tokio_postgres::Statement
    ) -> Result<u64>
    {
        Ok(self
            .conn
            .execute(statement, &[
                &serde_json::from_slice::<serde_json::Value>(item)?,
                &i64::try_from(thread).unwrap()
            ])
            .await?)
        //.expect("Err executing sql: upsert_deleteds");
    }

    /// This method updates an existing post or inserts a new one.
    /// Posts are only updated where there's a field change.
    /// A majority of posts in a thread don't change, this minimizes I/O writes.
    /// (sha256, sha25t, and deleted are handled seperately as they are special
    /// cases) https://stackoverflow.com/a/36406023
    /// https://dba.stackexchange.com/a/39821
    ///
    /// 4chan inserts a backslash in their md5.
    /// https://stackoverflow.com/a/11449627
    async fn upsert_thread(
        &self,
        item: &[u8],
        statement: &tokio_postgres::Statement
    ) -> Result<u64>
    {
        Ok(self
            .conn
            .execute(statement, &[&serde_json::from_slice::<serde_json::Value>(item)?])
            .await?)
        // .expect("Err executing sql: upsert_thread");
    }

    async fn check_metadata_col(&self, board: &str, statement: &tokio_postgres::Statement) -> bool {
        self.conn
            .query(statement, &[&board])
            .await
            .ok()
            .filter(|re| !re.is_empty())
            .map(|re| re[0].try_get(0).ok())
            .flatten()
            .unwrap_or(false)
    }

    async fn get_threads_list(
        &self,
        item: &[u8],
        statement: &tokio_postgres::Statement
    ) -> Result<VecDeque<u32>>
    {
        let i = serde_json::from_slice::<serde_json::Value>(item)?;
        Ok(self
            .conn
            .query(statement, &[&i])
            .await
            .ok()
            .filter(|re| !re.is_empty())
            .map(|re| re[0].try_get(0).ok())
            .flatten()
            .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
            .flatten()
            .ok_or_else(|| anyhow!("Error in get_threads_list"))?)
    }

    /// This query is only run ONCE at every startup
    /// Running a JOIN to compare against the entire DB on every INSERT/UPDATE
    /// would not be that great. That is not done here.
    /// This gets all the threads from cache, compares it to the new json to get
    /// new + modified threads Then compares that result to the database
    /// where a thread is deleted or archived, and takes only the threads
    /// where's it's not deleted or archived
    async fn get_combined_threads(
        &self,
        board: &str,
        new_threads: &[u8],
        statement: &tokio_postgres::Statement
    ) -> Result<VecDeque<u32>>
    {
        let i = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        Ok(self
            .conn
            .query(statement, &[&board, &i])
            .await
            .ok()
            .filter(|re| !re.is_empty())
            .map(|re| re[0].try_get(0).ok())
            .flatten()
            .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
            .flatten()
            .ok_or_else(|| anyhow!("Error in get_combined_threads"))?)
    }

    /// Combine new and prev threads.json into one. This retains the prev
    /// threads (which the new json doesn't contain, meaning they're either
    /// pruned or archived).  That's especially useful for boards without
    /// archives. Use the WHERE clause to select only modified threads. Now
    /// we basically have a list of deleted and modified threads.
    /// Return back this list to be processed.
    /// Use the new threads.json as the base now.
    async fn get_deleted_and_modified_threads(
        &self,
        board: &str,
        new_threads: &[u8],
        statement: &tokio_postgres::Statement
    ) -> Result<VecDeque<u32>>
    {
        let i = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        Ok(self
            .conn
            .query(statement, &[&board, &i])
            .await
            .ok()
            .filter(|re| !re.is_empty())
            .map(|re| re[0].try_get(0).ok())
            .flatten()
            .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
            .flatten()
            .ok_or_else(|| anyhow!("Error in get_combined_threads"))?)
    }

    #[allow(unused_mut)]
    async fn compute(
        &self,
        typ: YotsubaType,
        board_settings: &BoardSettings,
        // media_statement: &tokio_postgres::Statement,
        mut tx: Option<UnboundedSender<u32>>,
        mut rx: Option<UnboundedReceiver<u32>>
    )
    {
        match typ {
            YotsubaType::Archive | YotsubaType::Threads => {
                if let Some(t) = tx {
                    // loop {
                    let bs = board_settings.clone();
                    if self.fetch_board(typ, bs, &t).await.is_some() {};
                    // }
                }
            }
            YotsubaType::Media => {
                if let Some(mut r) = rx {
                    loop {
                        while let Some(thread) = r.recv().await {
                            // let bs = board_settings.clone();
                            // let info = MediaInfo { board: bs, thread: result };
                            self.fetch_media(board_settings, thread).await;
                        }
                        if self.finished.load(Ordering::Relaxed) {
                            break;
                        }
                        sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        }
    }

    async fn fetch_media(&self, info: &BoardSettings, thread: u32) {
        // FETCH MEDIA
        if !(info.download_media || info.download_thumbnails) {
            return;
        }
        let ms = self
            .conn
            .prepare_typed(sql::media_posts(self.schema(), &info.board).as_str(), &[
                tokio_postgres::types::Type::INT8
            ])
            .await
            .unwrap();
        match self.get_media_posts(thread, &ms).await {
            Ok(media_list) => {
                let mut fut = FuturesUnordered::new();
                // let client = &self.client;
                let mut has_media = false;
                for row in media_list.iter() {
                    has_media = true;
                    // let no: i64 = row.get("no");

                    // Preliminary checks before downloading
                    let sha256: Option<Vec<u8>> = row.get("sha256");
                    let sha256t: Option<Vec<u8>> = row.get("sha256t");
                    let mut dl_media = false;
                    if info.download_media {
                        match sha256 {
                            Some(h) => {
                                // Improper sha, re-dl
                                if h.len() < (65 / 2) {
                                    dl_media = true;
                                }
                            }
                            None => {
                                // No thumbs, proceed to dl
                                dl_media = true;
                            }
                        }
                        if dl_media {
                            fut.push(self.dl_media_post2(row, info, thread, false));
                        }
                    }
                    let mut dl_thumb = false;
                    if info.download_thumbnails {
                        match sha256t {
                            Some(h) => {
                                // Improper sha, re-dl
                                if h.len() < (65 / 2) {
                                    dl_thumb = true;
                                }
                            }
                            None => {
                                // No thumbs, proceed to dl
                                dl_thumb = true;
                            }
                        }
                        if dl_thumb {
                            fut.push(self.dl_media_post2(row, info, thread, true));
                        }
                    }
                }
                if has_media {
                    let s = &self;
                    while let Some(hh) = fut.next().await {
                        if let Some((no, hashsum, thumb)) = hh {
                            if let Some(hsum) = hashsum {
                                // Media info
                                // if !thumb {
                                //     println!(
                                //         "{}/{} Upserting sha256: ({})",
                                //         &info.board,
                                //         no,
                                //         if thumb { "thumb" } else { "media" }
                                //     );
                                // }

                                s.upsert_hash2(
                                    &info.board,
                                    no,
                                    if thumb { "sha256t" } else { "sha256" },
                                    hsum
                                )
                                .await;
                            } else {
                                error!("Error unwrapping hashsum");
                            }
                        } else {
                            error!("Error running hashsum function");
                        }
                        // // Listen to CTRL-C
                        // if self.finished.load(Ordering::Relaxed) {
                        //     return;
                        // }
                    }
                }
            }
            Err(e) => error!("/{}/{}\tError getting missing media -> {}", info.board, thread, e)
        }
    }

    // Downloads the list of archive / live threads
    async fn get_generic_thread(
        &self,
        thread_type: YotsubaType,
        bs: &BoardSettings,
        last_modified: &mut String,
        fetched_threads: &mut Option<Vec<u8>>,
        local_threads_list: &mut VecDeque<u32>,
        init: &mut bool,
        update_metadata: &mut bool,
        has_archives: &mut bool,
        statements: &HashMap<String, tokio_postgres::Statement>
    )
    {
        if thread_type == YotsubaType::Archive && !*has_archives {
            return;
        };
        let current_board = &bs.board;
        for retry_attempt in 0..(bs.retry_attempts + 1) {
            match self
                .client
                .get(
                    &format!(
                        "{domain}/{board}/{thread_type}.json",
                        domain = self.config.settings.api_url.as_ref().unwrap(),
                        board = current_board,
                        thread_type = thread_type
                    ),
                    Some(last_modified)
                )
                .await
            {
                Ok((last_modified_new, status, body)) => {
                    if last_modified_new.is_empty() {
                        error!(
                            "/{}/ [{}] <{}> An error has occurred getting the last_modified date",
                            current_board, thread_type, status
                        );
                    } else if *last_modified != last_modified_new {
                        last_modified.clear();
                        last_modified.push_str(&last_modified_new);
                    }
                    match status {
                        StatusCode::OK => {
                            if body.is_empty() {
                                error!(
                                    "({})\t/{}/\t<{}> Fetched threads was found to be empty!",
                                    thread_type, current_board, status
                                );
                            } else {
                                info!(
                                    "({})\t/{}/\tReceived new threads", // on {}",
                                    thread_type,
                                    current_board // ,Local::now().to_rfc2822()
                                );
                                *fetched_threads = Some(body.to_owned());

                                // Check if there's an entry in the metadata
                                if self
                                    .check_metadata_col(
                                        &current_board,
                                        &statements
                                            .get(&format!(
                                                "{}_check_metadata_col_{}",
                                                thread_type, current_board
                                            ))
                                            .unwrap()
                                    )
                                    .await
                                {
                                    let ena_resume = env::var("ENA_RESUME")
                                        .ok()
                                        .map(|a| a.parse::<bool>().ok())
                                        .flatten()
                                        .unwrap_or(false);

                                    // if there's cache
                                    // if this is a first startup
                                    // and ena_resume is false or thread type is archive
                                    // this will trigger getting archives from last left off
                                    // regardless of ena_resume. ena_resume only affects threads, so
                                    // a refetch won't be triggered.
                                    // if *init && (!ena_resume || (ena_resume && thread_type ==
                                    // YotsubaType::Archive))
                                    if *init && (thread_type == YotsubaType::Archive || !ena_resume)
                                    // clippy
                                    {
                                        // going here means the program was restarted
                                        // use combination of ALL threads from cache + new threads,
                                        // getting a total of 150+ threads
                                        // (excluding archived, deleted, and duplicate threads)
                                        if let Ok(mut list) = self
                                            .get_combined_threads(
                                                &current_board,
                                                &body,
                                                &statements
                                                    .get(&format!(
                                                        "{}_combined_threads_{}",
                                                        thread_type, current_board
                                                    ))
                                                    .unwrap()
                                            )
                                            .await
                                        {
                                            local_threads_list.append(&mut list);
                                        } else {
                                            info!(
                                                "({})\t/{}/\tSeems like there was no modified threads at startup..",
                                                thread_type, current_board
                                            );
                                        }

                                        // update base at the end
                                        *update_metadata = true;
                                        *init = false;
                                    } else {
                                        // here is when we have cache and the program in continously
                                        // running
                                        // ONLY get the new/modified/deleted threads
                                        // Compare time modified and get the new threads
                                        let id = &format!(
                                            "{}_deleted_and_modified_threads_{}",
                                            thread_type, current_board
                                        );
                                        match &statements.get(id) {
                                            Some(statement_recieved) => {
                                                if let Ok(mut list) = self
                                                    .get_deleted_and_modified_threads(
                                                        &current_board,
                                                        &body,
                                                        statement_recieved
                                                    )
                                                    .await
                                                {
                                                    local_threads_list.append(&mut list);
                                                } else {
                                                    info!(
                                                        "({})\t/{}/\tSeems like there was no modified threads..",
                                                        thread_type, current_board
                                                    )
                                                }
                                            }
                                            None => error!("Statement: {} was not found!", id)
                                        }

                                        // update base at the end
                                        *update_metadata = true;
                                        *init = false;
                                    }
                                } else {
                                    // No cache found
                                    // Use fetched_threads
                                    if let Err(e) = self
                                        .upsert_metadata(
                                            &current_board,
                                            &body,
                                            &statements
                                                .get(&format!(
                                                    "{}_upsert_metadata_{}",
                                                    thread_type, current_board
                                                ))
                                                .unwrap()
                                        )
                                        .await
                                    {
                                        error!("Error running upsert_metadata function! {}", e)
                                    }
                                    *update_metadata = false;
                                    *init = false;

                                    match if thread_type == YotsubaType::Threads {
                                        self.get_threads_list(
                                            &body,
                                            &statements
                                                .get(&format!(
                                                    "{}_threads_list_{}",
                                                    thread_type, current_board
                                                ))
                                                .unwrap()
                                        )
                                        .await
                                    } else {
                                        //serde_json::from_value::<VecDeque<u32>>(
                                        // Converting to anyhow::Error
                                        if let Ok(t) =
                                            serde_json::from_slice::<VecDeque<u32>>(&body)
                                        {
                                            Ok(t)
                                        } else {
                                            Err(anyhow!(
                                                "Error converting body to VecDeque for get_threads_list"
                                            ))
                                        }
                                        // .unwrap()
                                        //)
                                        // .ok()
                                    } {
                                        Ok(mut list) => local_threads_list.append(&mut list),
                                        Err(e) => warn!(
                                            "({})\t/{}/\tSeems like there was no modified threads in the beginning?.. {}",
                                            thread_type, current_board, e
                                        )
                                    }
                                }
                            }
                        }
                        StatusCode::NOT_MODIFIED => {
                            info!("({})\t/{}/\t<{}>", thread_type, current_board, status)
                        }
                        StatusCode::NOT_FOUND => {
                            error!(
                                "({})\t/{}/\t<{}> No {} found! {}",
                                thread_type,
                                current_board,
                                status,
                                thread_type,
                                if retry_attempt == 0 {
                                    "".into()
                                } else {
                                    format!("Retry attempt: #{}", retry_attempt)
                                }
                            );
                            sleep(Duration::from_secs(1)).await;
                            if thread_type == YotsubaType::Archive {
                                *has_archives = false;
                            }
                            continue;
                        }
                        _ => error!(
                            "({})\t/{}/\t<{}> An error has occurred!",
                            thread_type, current_board, status
                        )
                    };
                }
                Err(e) => error!(
                    "({})\t/{}/\tError fetching the {}.json.\t{}",
                    thread_type, current_board, thread_type, e
                )
            }
            if thread_type == YotsubaType::Archive {
                *has_archives = true;
            }
            break;
        }
    }

    // Manages a single board
    async fn fetch_board(
        &self,
        thread_type: YotsubaType,
        bs: BoardSettings,
        _t: &UnboundedSender<u32> // not used because the 3rd thread for media dl is not used
    ) -> Option<()>
    {
        let current_board = &bs.board;
        let mut threads_last_modified = String::new();
        let mut local_threads_list: VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut init = true;
        let mut has_archives = true;
        let statements = self.create_statements(current_board, thread_type).await;

        // This function is only called by fetch_board so it'll never be media.

        let rd = bs.refresh_delay.into();
        let dur = Duration::from_millis(250);
        let ratel = Duration::from_millis(bs.throttle_millisec.into());

        loop {
            let now = tokio::time::Instant::now();

            // Download threads.json / archive.json
            let mut fetched_threads: Option<Vec<u8>> = None;
            self.get_generic_thread(
                thread_type,
                &bs,
                &mut threads_last_modified,
                &mut fetched_threads,
                &mut local_threads_list,
                &mut init,
                &mut update_metadata,
                &mut has_archives,
                &statements
            )
            .await;

            // Display length of new fetched threads
            let threads_len = local_threads_list.len();
            if threads_len > 0 {
                info!("({})\t/{}/\tTotal new threads: {}", thread_type, current_board, threads_len);
            }

            // Download each thread
            let mut position = 1_u32;
            while let Some(thread) = local_threads_list.pop_front() {
                let now_thread = tokio::time::Instant::now();
                self.assign_to_thread(&bs, thread_type, thread, position, threads_len, &statements)
                    .await;
                position += 1;

                // Download thumbnails
                // if bs.download_thumbnails {
                // self.fetch_media(&bs, thread, false).await;
                // }

                // Send to download full media
                // if bs.download_media {
                // t.send(thread).unwrap();
                // tokio::spawn(async move {
                self.fetch_media(&bs, thread).await;
                // });
                // }

                if self.finished.load(Ordering::Relaxed) {
                    return Some(());
                }
                // ratelimit
                tokio::time::delay_until(now_thread + ratel).await;
            }
            // Update the cache at the end so that if the program was stopped while
            // processing threads, when it restarts it'll use the same
            // list of threads it was processing before + new ones.
            if threads_len > 0 && update_metadata {
                if let Some(ft) = &fetched_threads {
                    if let Err(e) = self
                        .upsert_metadata(
                            &current_board,
                            &ft,
                            statements
                                .get(&format!("{}_upsert_metadata_{}", thread_type, &bs.board))
                                .unwrap()
                        )
                        .await
                    {
                        error!("Error executing upsert_metadata function! {}", e);
                    }
                    update_metadata = false;
                }
            }
            //  Board refresh delay ratelimit
            while now.elapsed().as_secs() < rd {
                if self.finished.load(Ordering::Relaxed) {
                    return Some(());
                }
                sleep(dur).await;
            }
            if self.finished.load(Ordering::Relaxed) {
                break;
            }
        }
        Some(())
    }

    // Download a single thread and its media
    async fn assign_to_thread(
        &self,
        board_settings: &BoardSettings,
        thread_type: YotsubaType,
        thread: u32,
        position: u32,
        length: usize,
        statements: &HashMap<String, tokio_postgres::Statement>
    )
    {
        let thread_type = &format!("{}", thread_type);

        let board = &board_settings.board;
        // let one_millis = Duration::from_millis(1);

        for _ in 0..(board_settings.retry_attempts + 1) {
            match self
                .client
                .get(
                    &format!(
                        "{domain}/{bo}/thread/{th}.json",
                        domain = self.config.settings.api_url.as_ref().unwrap(),
                        bo = board,
                        th = thread
                    ),
                    None
                )
                .await
            {
                Ok((_, status, body)) => match status {
                    StatusCode::OK => {
                        if body.is_empty() {
                            error!(
                                "({})\t/{}/{}\t<{}> Body was found to be empty!",
                                thread_type, board, thread, status
                            );
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            if let Err(e) = self
                                .upsert_thread(
                                    &body,
                                    statements
                                        .get(&[thread_type, "_upsert_thread_", board].concat())
                                        .unwrap()
                                )
                                .await
                            {
                                error!("Error executing upsert_thread function! {}", e);
                            }
                            match self
                                .upsert_deleteds(
                                    thread,
                                    &body,
                                    statements
                                        .get(&[thread_type, "_upsert_deleteds_", board].concat())
                                        .unwrap()
                                )
                                .await
                            {
                                Ok(_) => info!(
                                    "({})\t/{}/{}\t[{}/{}]",
                                    thread_type, board, thread, position, length
                                ),
                                Err(e) => error!("Error running upsert_deleteds function! {}", e)
                            }
                            break;
                        }
                    }
                    StatusCode::NOT_FOUND => {
                        self.upsert_deleted(
                            thread,
                            statements
                                .get(&[thread_type, "_upsert_deleted_", board].concat())
                                .unwrap()
                        )
                        .await;
                        warn!(
                            "({})\t/{}/{}\t[{}/{}]\t<DELETED>",
                            thread_type, board, thread, position, length
                        );
                        break;
                    }
                    _e => {}
                },
                Err(e) => {
                    error!("({})\t/{}/{}\tError fetching thread {}", thread_type, board, thread, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    // Downloads any missing media from a thread
    async fn dl_media_post2(
        &self,
        row: &tokio_postgres::row::Row,
        bs: &BoardSettings,
        thread: u32,
        thumb: bool
    ) -> Option<(u64, Option<Vec<u8>>, bool)>
    {
        let path = self.get_path();
        let no: i64 = row.get("no");
        // let _sha256: Option<Vec<u8>> = row.get("sha256");
        // let _sha256t: Option<Vec<u8>> = row.get("sha256t");
        let ext: String = row.get("ext");
        // let _ext2: String = ext.clone();
        let tim: i64 = row.get("tim");

        let mut hashsum: Option<Vec<u8>> = None;
        let domain = &self.config.settings.media_url.as_ref().unwrap();
        let board = &bs.board;

        let url = format!(
            "{}/{}/{}{}{}",
            domain,
            board,
            tim,
            if thumb { "s" } else { "" },
            if thumb { ".jpg" } else { &ext }
        );
        for _ in 0..(bs.retry_attempts + 1) {
            match self.client.get(&url, None).await {
                Ok((_, status, body)) => match status {
                    StatusCode::OK => {
                        if body.is_empty() {
                            error!(
                                "/{}/{}\t<{}> Body was found to be empty!",
                                board, thread, status
                            );
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            // let body1 = body.clone();
                            // let hash_q = self.conn
                            // .query("select sha, sha::text as sha_text from (select sha256($1) as
                            // sha)x;", &[&body]) .await;

                            // create a Sha256 object
                            let mut hasher = Sha256::new();

                            // write input message
                            hasher.input(&body);

                            // read hash digest and consume hasher
                            let hash_bytes = hasher.result();

                            // let hash_bytes: Vec<u8> =
                            // hi.try_get("sha").expect("Error getting hash u8");
                            // let hash_text: String =
                            // hi.try_get("sha_text").expect("Error getting hash txt");
                            let temp_path = format!("{}/tmp/{}_{}{}", path, no, tim, ext);
                            hashsum = Some(hash_bytes.as_slice().to_vec());

                            // if (bs.keep_media && !thumb)
                            //     || (bs.keep_thumbnails && thumb)
                            //     || ((bs.keep_media && !thumb) && (bs.keep_thumbnails && thumb))
                            if (bs.keep_thumbnails || !thumb) && (thumb || bs.keep_media)
                            // clippy
                            {
                                if let Ok(mut dest) = std::fs::File::create(&temp_path) {
                                    if std::io::copy(&mut body.as_slice(), &mut dest).is_ok() {
                                        // Media info
                                        // println!(
                                        //     "({}) /{}/{}#{} -> {}{}{}",
                                        //     if thumb { "thumb" } else { "media" },
                                        //     board,
                                        //     thread,
                                        //     no,
                                        //     tim,
                                        //     if thumb { "s" } else { "" },
                                        //     if thumb { ".jpg" } else { &ext }
                                        // );
                                        // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
                                        // /1/0a
                                        // Move and rename

                                        let hash_str = &format!("{:x}", hash_bytes); // &hash_text[2..];
                                        let basename = Path::new(&hash_str)
                                            .file_stem()
                                            .expect("err get basename")
                                            .to_str()
                                            .expect("err get basename end");
                                        let second =
                                            &basename[&basename.len() - 3..&basename.len() - 1];
                                        let first = &basename[&basename.len() - 1..];
                                        let final_dir_path =
                                            format!("{}/media/{}/{}", path, first, second);
                                        let final_path =
                                            format!("{}/{}{}", final_dir_path, hash_str, ext);

                                        let path_final = Path::new(&final_path);

                                        if path_final.exists() {
                                            warn!("Already exists: {}", final_path);
                                            if let Err(e) = std::fs::remove_file(&temp_path) {
                                                error!("Remove temp: {}", e);
                                            }
                                        } else {
                                            if let Err(e) = std::fs::create_dir_all(&final_dir_path)
                                            {
                                                error!("Create final dir: {}", e);
                                            }
                                            if let Err(e) = std::fs::rename(&temp_path, &final_path)
                                            {
                                                error!("Rename temp to final: {}", e);
                                            }
                                        }
                                    } else {
                                        error!("Error copying file to a temporary path");
                                    }
                                } else {
                                    error!("Error creating a temporary file path");
                                }
                            }
                            break;
                        }
                    }
                    StatusCode::NOT_FOUND => {
                        error!("/{}/{}\t<{}> {}", board, no, status, &url);
                        break;
                    }
                    _e => {
                        error!("/{}/{}\t<{}> {}", board, no, status, &url);
                        sleep(Duration::from_secs(1)).await;
                    }
                },
                Err(e) => {
                    error!("/{}/{}\tError fetching thread {}", board, thread, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Some((u64::try_from(no).unwrap(), hashsum, thumb))
    }
}
