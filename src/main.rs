#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]
#![recursion_limit = "2048"]

// use anyhow::{anyhow, Result};
// use futures::prelude::*;
// use futures::io::{self, AsyncReadExt, AsyncWriteExt, Cursor};
// use async_ctrlc::CtrlC;
// use futures::io::AsyncWriteExt;
// use async_std::sync::RwLock;
// use futures::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use async_compat::CompatExt;
use ctrlc;
use fomat_macros::{epintln, fomat, pintln};
use futures::{future::Either, stream::FuturesUnordered};

// use futures::stream::FuturesOrdered;
#[allow(unused_imports)]
use ::log::*;
use ansiform::ansi;
use async_process::Command;
use futures::stream::{self, StreamExt};
use futures_lite::*;
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use reqwest::{self, StatusCode};
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{create_dir_all, File},
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    time::{Duration, Instant},
};
pub mod config;
pub mod log;
pub mod net;
pub mod yotsuba;
// use crate::log::try_init_timed_custom_env;
use config::*;
use net::*;
use yotsuba::update_post_with_extra;
mod refresh;

#[path = "sql/sql.rs"]
pub mod sql;

static CTRLC: Lazy<AtomicU8> = Lazy::new(|| AtomicU8::new(0));
static INIT: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));

fn get_ctrlc() -> bool {
    CTRLC.load(Ordering::SeqCst) >= 1
}
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ThreadType {
    Threads,
    Archive,
}

impl ThreadType {
    fn is_threads(&self) -> bool {
        *self == ThreadType::Threads
    }

    fn is_archive(&self) -> bool {
        *self == ThreadType::Archive
    }

    fn as_str<'a>(&self) -> &'a str {
        self.into()
    }
}

impl From<&ThreadType> for &str {
    fn from(t: &ThreadType) -> Self {
        match t {
            ThreadType::Threads => "threads",
            ThreadType::Archive => "archive",
        }
    }
}

impl std::fmt::Display for ThreadType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fomat_macros::wite!(f, (self.as_str()))
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MediaType {
    Full,
    Thumbnail,
}

impl MediaType {
    fn human_str<'a>(&self) -> &'a str {
        match self {
            MediaType::Full => "media",
            MediaType::Thumbnail => "thumb",
        }
    }
    fn ext<'a>(&self) -> &'a str {
        match self {
            MediaType::Full => "",
            MediaType::Thumbnail => "s.jpg",
        }
    }
}

pub type MediaDetails = (
    u64,
    u64,
    Option<String>,
    Option<Vec<u8>>,
    u64,
    String,
    String,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
);

/// From `hex-slice` crate
pub struct HexSlice<'a, T: 'a>(&'a [T]);
use core::fmt;

impl<'a, T: fmt::LowerHex> fmt::LowerHex for HexSlice<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val = self.0;
        for b in val {
            fmt::LowerHex::fmt(b, f)?
        }
        Ok(())
    }
}

async fn sleep(dur: Duration) {
    smol::Timer::after(dur).await;
}

fn exists<P: AsRef<std::path::Path>>(path: P) -> bool {
    std::fs::metadata(path).is_ok()
}

async fn db_retry() {
    sleep(Duration::from_millis(500)).await
}

fn main() -> Result<()> {
    // Backtraces are only useful in debug builds
    // if cfg!(debug_assertions) {
    //     if std::env::var("RUST_BACKTRACE").is_err() {
    //         std::env::set_var("RUST_BACKTRACE", "full");
    //     }
    // }
    // if std::env::var("ENA_LOG").is_err() {
    //     std::env::set_var("ENA_LOG", format!("{}=info", env!("CARGO_PKG_NAME")));
    // }

    log::init(::log::LevelFilter::Info).unwrap();
    let num_threads = num_cpus::get().max(1);

    if std::env::var("SMOL_THREADS").is_err() {
        std::env::set_var("SMOL_THREADS", num_threads.to_string());
    }

    // let (sender, receiver) = oneshot::channel::<()>();
    smol::block_on(async {
        ctrlc::set_handler(move || {
            if CTRLC.load(Ordering::SeqCst) == 0 {
                warn!("Received SIGTERM/SIGINT signal. Ena will try to exit cleanly (by waiting for all current posts/images to finish downloading).");
            }
            CTRLC.fetch_add(1, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
        async_main().compat().await
    })
}

async fn async_main() -> Result<()> {
    let mut opt = match config::get_opt() {
        Err(e) => {
            epintln!((e));
            return Ok(());
        }
        Ok(res) => res,
    };

    if opt.debug {
        pintln!((opt.to_string().unwrap()));
        return Ok(());
    }

    let origin = "http://boards.4chan.org";
    if !opt.asagi_mode {
        // temp set env variable
        std::env::set_var("PGPASSWORD", &opt.database.password);
        std::env::set_var("PGOPTIONS", "--client-min-messages=warning");

        // Check if database exists just for the sake of displaying if it exists or not

        let mut child = Command::new("psql")
            .stdin(async_process::Stdio::piped())
            .stdout(async_process::Stdio::piped())
            .args(&[
                "-h",
                &opt.database.host,
                "-p",
                &fomat!((opt.database.port)),
                "-U",
                &opt.database.username,
                "-t",
            ])
            .spawn()?;
        {
            let stdin = child.stdin.as_mut().expect("Failed to open stdin");
            stdin
                .write_all(
                    fomat!(
                    "SELECT true WHERE EXISTS (SELECT FROM pg_database WHERE datname = '" (&opt.database.name) "')"
                    )
                    .as_bytes(),
                )
                .await
                .expect("Failed to write to stdin");
        }
        let output = child.output().await?;
        let out = String::from_utf8_lossy(&output.stdout);
        let out = out.trim();

        if out.is_empty() {
            let up_sql = {
                let mut up = include_str!("sql/up.sql").to_string();
                if opt.database.schema == "public" {
                    up = up.replace(r#""%%SCHEMA%%", "#, "");
                }

                // This is could be better.. but w/e
                // Just don't have empty strings on your timescaledb settings..
                if let Some(ts) = &opt.timescaledb {
                    if ts.column.is_empty() || ts.every.is_empty() {
                        epintln!("Skipping timescaledb.. One of the columns are empty.");
                        up = up.replace("CREATE EXTENSION", "-- CREATE EXTENSION");
                        up = up.replace("SELECT create_hypertable", "-- SELECT create_hypertable");
                    } else {
                        up = up.replace("'time'", &fomat!("'"(ts.column)"'" ));

                        if !ts.every.is_empty() && !ts.every.to_lowercase().contains("interval") {
                            epintln!(
                            "Error!"
                            "\nThe current schema cannot use an integer for the `every` column for TimescaleDB due to our use of triggers."
                            "\nSee tracking issue: https://github.com/timescale/timescaledb/issues/1084"
                            "\n"
                            r#"Consider using something like "INTERVAL '2 weeks'""#
                            );
                            return Ok(());
                        }

                        up = up.replace("INTERVAL '2 weeks'", &ts.every);
                    }
                } else {
                    up = up
                        .replace("CREATE EXTENSION", "-- CREATE EXTENSION")
                        .replace("SELECT create_hypertable", "-- SELECT create_hypertable");
                }

                up = up
                    .replace("%%SCHEMA%%", &opt.database.schema)
                    .replace("%%DB_NAME%%", &opt.database.name);
                up
            };

            info!("Initializing database..");

            // Create database if not exists
            let mut child = Command::new("psql")
                .stdin(async_process::Stdio::piped())
                .stdout(async_process::Stdio::piped())
                .args(&[
                    "-h",
                    &opt.database.host,
                    "-p",
                    &fomat!((opt.database.port)),
                    "-U",
                    &opt.database.username,
                ])
                .spawn()?;
            {
                let stdin = child.stdin.as_mut().expect("Failed to open stdin");
                stdin
                    .write_all(
                        fomat!(
                "SELECT 'CREATE DATABASE " (&opt.database.name) "' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '"
                (&opt.database.name)
                r#"')\gexec"#)
                        .as_bytes(),
                    )
                    .await
                    .expect("Failed to write to stdin");
            }
            let output = child.output().await?;
            let out = String::from_utf8_lossy(&output.stdout);
            let out = out.trim();

            // When the ouput is something, that means a new database was created
            if !out.is_empty() {
                // Run the sql migration to init tables/triggers/etc
                let mut child = Command::new("psql")
                    .stdin(async_process::Stdio::piped())
                    .stdout(async_process::Stdio::piped())
                    .args(&[
                        "-q",
                        "-h",
                        &opt.database.host,
                        "-p",
                        &fomat!((opt.database.port)),
                        "-U",
                        &opt.database.username,
                        "-d",
                        &opt.database.name,
                    ])
                    .spawn()?;
                {
                    let stdin = child.stdin.as_mut().expect("Failed to open stdin");
                    stdin
                        .write_all(up_sql.as_bytes())
                        .await
                        .expect("Failed to write to stdin");
                }
                let output = child.output().await?;
            }
        }

        if get_ctrlc() {
            return Ok(());
        }

        // Finally connect to the database
        let (client, connection) =
            tokio_postgres::connect(opt.database.url.as_ref().unwrap(), tokio_postgres::NoTls)
                .await
                .unwrap();
        smol::spawn(async move {
            if let Err(e) = connection.await {
                epintln!("connection error: "(e));
            }
        })
        .detach();

        let fchan = FourChan::new(create_client(origin, &opt).await?, client, opt).await;
        fchan.run().await
    } else {
        use itertools::Itertools;
        // use mysql_async::prelude::*;
        use sql::DropExecutor;
        use strum::IntoEnumIterator;

        // temp set env variable
        std::env::set_var("MYSQL_PWD", &opt.database.password);

        // Create database if not exists
        let mut child = Command::new("mysql")
            .stdout(async_process::Stdio::piped())
            .args(&[
                "-h",
                &opt.database.host,
                "-P",
                &fomat!((opt.database.port)),
                "-u",
                &opt.database.username,
                "-e",
                // This is made to be wrapped in quotes, so we dont't need to wrap in quotes manually.
                // CREATE DATABASE IF NOT EXISTS `asagi` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                &fomat!(r#"CREATE DATABASE IF NOT EXISTS `"# (&opt.database.name) "` CHARACTER SET = " (&opt.database.charset) " COLLATE = " (&opt.database.collate) r#";"#),
            ])
            .spawn()?;

        let _ = child.status().await?;
        let query_count = sql::Query::iter().count() - 1;
        let board_count = opt
            .boards
            .iter()
            .map(|board| board.name.as_str())
            .chain(
                opt.threads
                    .iter()
                    .map(|t| t.trim_start_matches('/').split('/').nth(0).unwrap()),
            )
            .unique()
            .count();
        let stmt_count = board_count * query_count;

        let pool = mysql_async::Pool::new(
            &fomat!( (opt.database.url.as_ref().unwrap()) "?stmt_cache_size=" (stmt_count) "&pool_min=1&pool_max=3"),
        );
        let fchan = FourChan::new(create_client(origin, &opt).await?, pool, opt).await;
        fchan.run().await?;
        fchan.db_client.disconnect_pool().await?;

        Ok(())
    }
}

struct FourChan<D>
where
    D: sql::QueryExecutor + sql::DropExecutor,
{
    client: reqwest::Client,
    db_client: D,
    opt: Opt,
}

impl<D> FourChan<D>
where
    D: sql::QueryExecutor + sql::DropExecutor,
{
    pub async fn new(client: reqwest::Client, db_client: D, opt: Opt) -> Self {
        Self {
            client,
            db_client,
            opt,
        }
    }

    /// Archive 4chan based on --boards | --threads options
    async fn run(&self) -> Result<()> {
        // Init the `boards` table for MySQL
        if self.opt.asagi_mode {
            let mut board = Board::default();
            board.name = "boards".into();
            self.db_client
                .board_table_exists(&board, &self.opt, &self.opt.database.name)
                .await;
        }

        // If postgres, this will init all statements
        // If mysql, this will init only board statements since we need to know the specific board name due
        // to its schema
        self.db_client.init_statements(1, "boards").await.unwrap();

        self.download_boards_index().await.unwrap();

        // This `board_settings` is already patched to include settings from file + CLI
        // let mut base = self.opt.board_settings.clone();

        // Keep a cache of boards not found in self.opt.boards
        let mut boards_map: HashMap<String, Board> = HashMap::new();
        // let mut mm = &mut boards_map;

        let found = self.opt.boards.iter().find(|&b| b.name == "");
        let find_board = |search_str: &str| {
            self.opt
                .boards
                .iter()
                .find(|&b| b.name.as_str() == search_str)
        };

        // Grab seperate boards to dl
        // Grab seperate threads to dl
        // Combine them and run
        for s_thread in self.opt.threads.iter() {
            let split: Vec<&str> = s_thread.split('/').filter(|s| !s.is_empty()).collect();
            let b_cli = split[0];
            if boards_map.get_mut(b_cli).is_none() {
                let mut base = self.opt.board_settings.clone();
                base.name = b_cli.into();
                boards_map.insert(b_cli.into(), base);
            }
        }
        let mut fut = self
            .opt
            .threads
            .iter()
            .map(|s_thread| {
                // No need to check if this is a valid thread. It was already checked in the parsing of the CLI
                // stage. That's why it's in self.opt.threads
                let split: Vec<&str> = s_thread.split('/').filter(|s| !s.is_empty()).collect();
                let b_cli = split[0];
                let no = split[1].parse::<u64>().unwrap();
                let search = self.opt.boards.iter().find(|&b| b.name.as_str() == b_cli);

                let res = if let Some(found) = search {
                    self.download_board_and_thread(found.clone(), Some(no))
                } else {
                    self.download_board_and_thread(boards_map.get(b_cli).unwrap().clone(), Some(no))
                };
                res
            })
            .chain(
                self.opt
                    .boards
                    .iter()
                    .cloned()
                    .map(|board| self.download_board_and_thread(board, None)),
            )
            .collect::<FuturesUnordered<_>>();

        if self.opt.asagi_mode {
            config::display_asagi();
        // Make them see the asciiart
        // sleep(Duration::from_millis(500)).await;
        } else {
            config::display();
        }
        info!("Press CTRL+C to exit");

        while let Some(res) = fut.next().await {
            res.unwrap();
        }

        Ok(())
    }

    async fn download_boards_index(&self) -> Result<()> {
        let last_modified = self
            .db_client
            .boards_index_get_last_modified()
            .await
            .ok()
            .flatten();
        for retry in 0..=3u8 {
            let resp = self
                .client
                .gett(
                    self.opt.api_url.join("boards.json")?,
                    last_modified.as_ref(),
                )
                .await;
            match resp {
                Ok((status, lm, body)) => {
                    // Will do nothing on StatusCode::NOT_MODIFIED
                    if status == StatusCode::OK {
                        self.db_client
                            .boards_index_upsert(
                                &serde_json::from_slice::<serde_json::Value>(&body)?,
                                &lm,
                            )
                            .await
                            .unwrap();
                    }
                    break;
                }
                Err(e) => {
                    error!(
                        "({endpoint})\t\t[download_boards_index] [{err}]",
                        endpoint = "boards",
                        err = e,
                    );
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn download_board_and_thread(&self, board: Board, thread: Option<u64>) -> Result<()> {
        let mut _board = board;
        if let Some(t) = thread {
            if t == 0 {
                return Ok(());
            }
        } else {
            // Go here if this function was called for a board
            if !_board.with_threads && !_board.with_archives {
                return Ok(());
            }
        }

        if _board.skip_board_check {
            for retry in 0..=_board.retry_attempts {
                match self
                    .client
                    .head(&fomat!((self.opt.api_url)"/"(&_board.name)"/threads.json"))
                    .send()
                    .await
                {
                    Err(e) => {
                        if retry == _board.retry_attempts {
                            epintln!("Error requesting `/" (&_board.name) "/threads.json` [" (e) "]");
                            return Ok(());
                        }
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        if status == StatusCode::OK {
                            break;
                        } else {
                            if retry == _board.retry_attempts {
                                epintln!("Invalid board `"(&_board.name)"`");
                                return Ok(());
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(500)).await
            }
        } else {
            // Check if valid
            let valid_board: bool = self.db_client.board_is_valid(&_board.name).await.unwrap();
            if !valid_board {
                epintln!("Invalid board `"(&_board.name)"`");
                return Ok(());
            }
        }

        // Upsert Board
        self.db_client.board_upsert(&_board.name).await.unwrap();

        // Create the boards if it doesn't exist
        if self.opt.asagi_mode {
            if get_ctrlc() {
                return Ok(());
            }
            // The group of statments for just `Boards` was done in the beginning (in the `run() method`), so
            // this can be called This will create all the board tables, triggers, etc if the board
            // doesn't exist
            self.db_client
                .board_table_exists(&_board, &self.opt, &self.opt.database.name)
                .await;
            if get_ctrlc() {
                return Ok(());
            }
        }

        // Get board id
        let board_id = self.db_client.board_get(&_board.name).await;
        if let Ok(id) = board_id {
            _board.id = id.unwrap();
        } else {
            let id = self.db_client.board_get(&_board.name).await.unwrap();
            _board.id = id.unwrap();
        }

        if self.opt.asagi_mode {
            // Init the actual statements for this specific board. Ignores when already exists.
            // For postgres, all the statements were initialized in the `run` method
            self.db_client
                .init_statements(_board.id, &_board.name)
                .await
                .unwrap();
        }

        let mut rate = refresh::refresh_rate(
            if thread.is_some() {
                _board.interval_threads.into()
            } else {
                _board.interval_boards.into()
            },
            5 * 1000,
            10,
        );

        let hz = Duration::from_millis(250);
        let interval = Duration::from_millis(_board.interval_threads.into());

        // Startup is to determine whether to get combined or modified threads
        let mut startup = true;
        let mut res = None;

        loop {
            let now = Instant::now();
            let mut thread_type = ThreadType::Threads;

            // Go here if this function was called for a thread
            if let Some(_thread) = thread {
                // with_threads or with_archives don't apply here, since going here implies you want the
                // thread/archive
                let (_thread_type, deleted) = self
                    .download_thread(&_board, _thread, ThreadType::Threads, startup)
                    .await
                    .unwrap();
                thread_type = _thread_type;
                if !_board.watch_threads || get_ctrlc() || _thread_type.is_archive() || deleted {
                    break;
                }
            } else {
                // Go here if this function was called for a board

                if _board.with_threads && _board.with_archives {
                    let (threads, archive) = futures::join!(
                        self.download_board(&_board, ThreadType::Threads, startup),
                        self.download_board(&_board, ThreadType::Archive, startup)
                    );
                    res = threads.ok();
                    // Ignore since once done it'll hardly be updated, so use the threads' StatusCode
                    archive.unwrap();
                } else {
                    if _board.with_threads {
                        thread_type = ThreadType::Threads;
                    } else {
                        thread_type = ThreadType::Archive;
                    }
                    res = self
                        .download_board(&_board, thread_type, startup)
                        .await
                        .ok();
                }

                // After getting the board once, see if we're archiving it or not
                if !_board.watch_boards || get_ctrlc() {
                    break;
                }
            }

            // FIXME: elpased() silent panics!
            let interval = {
                if _board.interval_dynamic {
                    if let Some(st) = res {
                        if st == StatusCode::OK {
                            rate.reset();
                        }
                        Duration::from_millis(rate.next().unwrap())
                    } else {
                        Duration::from_millis(rate.next().unwrap())
                    }
                } else {
                    Duration::from_millis(if thread.is_some() {
                        _board.interval_threads.into()
                    } else {
                        _board.interval_boards.into()
                    })
                }
            };
            info!(
                "({endpoint})\t/{board}/\t\t{wait}",
                endpoint = thread_type,
                board = &_board.name,
                wait = format!(
                    ansi!("{;green}"),
                    format!("Waiting {} ms", interval.as_millis())
                )
            );
            while now.elapsed() < interval {
                if get_ctrlc() {
                    break;
                }
                sleep(hz).await;
            }
            if get_ctrlc() {
                break;
            }
            startup = false;
        }

        Ok(())
    }

    async fn download_board(
        &self,
        board: &Board,
        thread_type: ThreadType,
        startup: bool,
    ) -> Result<StatusCode> {
        let _sem = if thread_type.is_threads() {
            SEMAPHORE_BOARDS.acquire(1).await
        } else {
            SEMAPHORE_BOARDS_ARCHIVE.acquire(1).await
        };
        if get_ctrlc() {
            return Ok(StatusCode::OK);
        }
        // let fun_name = "download_board";
        let last_modified = self
            .db_client
            .board_get_last_modified(thread_type, board)
            .await;
        let mut url = self
            .opt
            .api_url
            .join(fomat!((&board.name)"/").as_str())?
            .join(&fomat!((thread_type)".json"))?;
        for retry in 0..=board.retry_attempts {
            if get_ctrlc() {
                break;
            }
            if retry != 0 {
                sleep(Duration::from_secs(1)).await;
            }
            let resp = self.client.gett(url.as_str(), last_modified.as_ref()).await;
            match resp {
                Err(e) => {
                    error!(
                        "({endpoint})\t/{board}/\t\t{err}",
                        endpoint = thread_type,
                        board = &board.name,
                        err = e,
                    );
                    if retry == board.retry_attempts {
                        return Ok(StatusCode::SERVICE_UNAVAILABLE);
                    }
                }
                Ok((status, lm, body)) => {
                    match status {
                        StatusCode::OK => {
                            let body_json =
                                serde_json::from_slice::<serde_json::Value>(&body).unwrap();
                            // "download_board: ({thread_type}) /{board}/{tab}{new_lm} | {prev_lm} | {retry_status}"
                            /*pintln!((fun_name) ":  (" (thread_type) ") /" (&board.name) "/"
                            if board.name.len() <= 2 { "\t\t" } else {"\t "}
                            (&lm)
                            " | "
                            if let Some(_lm) =  &last_modified { (_lm) } else { "None" }
                            " | "
                            if retry > 0 { " Retry #"(retry)" [RESOLVED]" } else { "" }
                            );*/
                            loop {
                                let res = if startup {
                                    self.db_client
                                        .threads_get_combined(thread_type, board.id, &body_json)
                                        .await
                                } else {
                                    if thread_type.is_threads() {
                                        self.db_client
                                            .threads_get_modified(board.id, &body_json)
                                            .await
                                    } else {
                                        self.db_client
                                            .threads_get_combined(
                                                ThreadType::Archive,
                                                board.id,
                                                &body_json,
                                            )
                                            .await
                                    }
                                };

                                match res {
                                    Err(e) => {
                                        if get_ctrlc() {
                                            break;
                                        }
                                        error!(
                                            "({endpoint})\t/{board}/{tab}{err}",
                                            endpoint = thread_type,
                                            board = &board.name,
                                            tab = if board.name.len() <= 2 { "\t\t" } else { "\t" },
                                            err = e
                                        );
                                        db_retry().await;
                                    }
                                    Ok(either) => {
                                        match either {
                                            Either::Right(rows) => {
                                                if let Some(mut rows) = rows {
                                                    let total = rows.len();
                                                    if total > 0 {
                                                        if thread_type.is_threads() {
                                                            info!(
                                                                "({endpoint})\t/{board}/\t\t{modified} threads: {length}",
                                                                endpoint = thread_type,
                                                                modified = if startup {
                                                                    "Received"
                                                                } else {
                                                                    if last_modified.is_none() {
                                                                        "Total new"
                                                                    } else {
                                                                        "Total new/modified"
                                                                    }
                                                                },
                                                                board = &board.name,
                                                                length = total
                                                            );
                                                        } else {
                                                            info!(
                                                                "({endpoint})\t/{board}/\t\t{modified} threads: {length}",
                                                                endpoint = format!(ansi!("{;magenta}"), thread_type),
                                                                modified = if startup {
                                                                    "Received"
                                                                } else {
                                                                    if last_modified.is_none() {
                                                                        "Total new"
                                                                    } else {
                                                                        "Total new/modified"
                                                                    }
                                                                },
                                                                board = &board.name,
                                                                length = total
                                                            );
                                                        }
                                                        let r = rows
                                                            .into_iter()
                                                            .map(|no| {
                                                                self.download_thread(
                                                                    board,
                                                                    no,
                                                                    thread_type,
                                                                    startup,
                                                                )
                                                            })
                                                            .collect::<Vec<_>>();
                                                        let mut stream_of_futures = stream::iter(r);

                                                        // buffered ordered or unordered makes no difference since it's run concurrently
                                                        let mut fut = stream_of_futures
                                                            .buffer_unordered(
                                                                self.opt.limit as usize,
                                                            );
                                                        while let Some(res) = fut.next().await {
                                                            res.unwrap();
                                                        }
                                                    } else {
                                                        if thread_type.is_threads() {
                                                            info!("({endpoint})\t/{board}/\t\t[Not Modified]", endpoint = thread_type, board = &board.name,);
                                                        } else {
                                                            info!("({endpoint})\t/{board}/\t\t[Not Modified]", endpoint = format!(ansi!("{;magenta}"), thread_type), board = &board.name,);
                                                        }
                                                    }
                                                }
                                            }
                                            Either::Left(mut rows) => {
                                                futures::pin_mut!(rows);

                                                // Collecting here is OK since eventually doing a `rows.map` is basically the same thing
                                                // This is done so I get the length
                                                let list = rows.collect::<Vec<_>>().await;
                                                let total = list.len();
                                                if total > 0 {
                                                    if thread_type.is_threads() {
                                                        info!(
                                                            "({endpoint})\t/{board}/\t\t{modified} threads: {length}",
                                                            endpoint = thread_type,
                                                            modified = if startup {
                                                                "Received"
                                                            } else {
                                                                if last_modified.is_none() {
                                                                    "Total new"
                                                                } else {
                                                                    "Total new/modified"
                                                                }
                                                            },
                                                            board = &board.name,
                                                            length = total
                                                        );
                                                    } else {
                                                        info!(
                                                            "({endpoint})\t/{board}/\t\t{modified} threads: {length}",
                                                            endpoint = format!(ansi!("{;magenta}"), thread_type),
                                                            modified = if startup {
                                                                "Received"
                                                            } else {
                                                                if last_modified.is_none() {
                                                                    "Total new"
                                                                } else {
                                                                    "Total new/modified"
                                                                }
                                                            },
                                                            board = &board.name,
                                                            length = total
                                                        );
                                                    }
                                                    let mut fut = stream::iter(list)
                                                        .map(|row| {
                                                            let no: i64 = row.unwrap().get(0);
                                                            self.download_thread(
                                                                board,
                                                                no as u64,
                                                                thread_type,
                                                                startup,
                                                            )
                                                        })
                                                        .buffer_unordered(self.opt.limit as usize);
                                                    let mut total = 0usize;
                                                    while let Some(res) = fut.next().await {
                                                        res.unwrap();
                                                    }
                                                } else {
                                                    if thread_type.is_threads() {
                                                        warn!(
                                                            "({endpoint})\t/{board}/\t\t{status}",
                                                            endpoint = thread_type,
                                                            board = &board.name,
                                                            status = status,
                                                        );
                                                    // info!("({endpoint})\t/{board}/\t\t[Not Modified]", endpoint = thread_type, board = &board.name,);
                                                    } else {
                                                        warn!(
                                                            "({endpoint})\t/{board}/\t\t{status}",
                                                            endpoint = format!(
                                                                ansi!("{;magenta}"),
                                                                thread_type
                                                            ),
                                                            board = &board.name,
                                                            status = status,
                                                        );
                                                        // info!("({endpoint})\t/{board}/\t\t[Not
                                                        // Modified]", endpoint =
                                                        // format!(ansi!("{;magenta}"),
                                                        // thread_type), board =
                                                        // &board.name,);
                                                    }
                                                }
                                            }
                                        }
                                        break;
                                    }
                                }
                            }

                            if get_ctrlc() {
                                break;
                            }

                            // Update threads/archive cache at the end
                            loop {
                                let res = {
                                    if thread_type.is_threads() {
                                        self.db_client
                                            .board_upsert_threads(
                                                board.id,
                                                &board.name,
                                                &body_json,
                                                &lm,
                                            )
                                            .await
                                    } else {
                                        self.db_client
                                            .board_upsert_archive(
                                                board.id,
                                                &board.name,
                                                &body_json,
                                                &lm,
                                            )
                                            .await
                                    }
                                };
                                match res {
                                    Err(e) => {
                                        if get_ctrlc() {
                                            return Ok(status);
                                        }
                                        error!(
                                            "({endpoint})\t/{board}/\t\t{err}",
                                            endpoint = thread_type,
                                            board = &board.name,
                                            err = e
                                        );

                                        db_retry().await;
                                    }
                                    Ok(_) => break,
                                }
                            }
                            break; // exit the retry loop
                        }
                        StatusCode::NOT_MODIFIED => {
                            info!(
                                "({endpoint})\t/{board}/\t\t{status}",
                                endpoint = thread_type,
                                board = &board.name,
                                status = format!(ansi!("{;green}"), status),
                            );
                            return Ok(status);
                        }
                        _ => {
                            error!(
                                "({endpoint})\t/{board}/\t\t{status}",
                                endpoint = thread_type,
                                board = &board.name,
                                status = status,
                            );
                            if retry == board.retry_attempts {
                                return Ok(status);
                            }
                        }
                    }
                }
            }
        }
        Ok(StatusCode::OK)
    }

    async fn download_thread(
        &self,
        board: &Board,
        thread: u64,
        thread_type: ThreadType,
        startup: bool,
    ) -> Result<(ThreadType, bool)> {
        let mut thread_type = thread_type;
        let mut deleted = false;

        let sem = if thread_type.is_threads() {
            SEMAPHORE_THREADS.acquire(1).await
        } else {
            SEMAPHORE_THREADS_ARCHIVE.acquire(1).await
        };
        if get_ctrlc() {
            return Ok((thread_type, deleted));
        }
        let mut with_tail = if thread_type.is_threads() {
            board.with_tail
        } else {
            false
        };
        let hz = Duration::from_millis(250);
        let mut interval = Duration::from_millis(board.interval_threads.into());

        // One giant loop so we can re-execute this function (if we find out tail-json is nonexistent)
        // without resorting to recursion which generates unintended behaviours with the semaphore.
        // We break at the end so it's OK.
        'outer: loop {
            if get_ctrlc() {
                return Ok((thread_type, deleted));
            }
            let last_modified = self
                .db_client
                .thread_get_last_modified(board.id, thread)
                .await;
            let mut url = self
                .opt
                .api_url
                .join(fomat!((&board.name)"/").as_str())?
                .join("thread/")?
                .join(&format!(
                    "{thread}{tail}.json",
                    thread = thread,
                    tail = if with_tail { "-tail" } else { "" }
                ))?;
            for retry in 0..=board.retry_attempts {
                let now = Instant::now();

                // TODO: The 4chan server doesn't always update `Last-Modified` immediately in conformance with the
                // modified threads we get from our diff This is ok I guess since we eventually get
                // the thread after waiting the for ratelimit amount (interval_board)
                let resp = self.client.gett(url.as_str(), last_modified.as_ref()).await;
                match resp {
                    Err(e) => {
                        error!(
                            "({endpoint})\t/{board}/{thread}{tail}\t[download_thread] [{err}]",
                            endpoint = thread_type,
                            board = &board.name,
                            thread = thread,
                            tail = if with_tail { "-tail " } else { " " },
                            err = e
                        );
                    }
                    Ok((status, lm, body)) => {
                        if body.len() > 0 {
                            let j = serde_json::from_slice::<serde_json::Value>(body.as_slice());
                            if let Ok(thread_json) = j {
                                let op = thread_json.get("posts").and_then(|v| v.get(0));
                                let archived = op
                                    .and_then(|v| v.get("archived"))
                                    .and_then(|v| v.as_u64())
                                    .map_or_else(|| false, |v| v == 1);
                                let archived_on =
                                    op.and_then(|v| v.get("archived")).and_then(|v| v.as_u64());
                                if archived || archived_on.is_some() {
                                    thread_type = ThreadType::Archive;

                                    // Tail json doesn't include archived_on. Plus we filter the first post upon `thread_upsert`
                                    // The easiest way to fix this is to fetch the thread without tail and upsert it.
                                    if with_tail {
                                        with_tail = false;
                                        continue 'outer;
                                    }
                                }
                            }
                        }
                        match status {
                            StatusCode::OK => {
                                // Going here (StatusCode::OK) means thread was modified
                                // DELAY
                                while now.elapsed() < interval {
                                    if get_ctrlc() {
                                        break;
                                    }
                                    sleep(hz).await;
                                }
                                if get_ctrlc() {
                                    break;
                                }

                                // ignore if err, it means it's empty or incomplete
                                match serde_json::from_slice::<serde_json::Value>(&body) {
                                    Err(e) => {
                                        error!(
                                            "({endpoint})\t/{board}/{thread}{tail}\t[download_thread] [{err}]",
                                            endpoint = thread_type,
                                            board = &board.name,
                                            thread = thread,
                                            tail = if with_tail { "-tail " } else { " " },
                                            err = e
                                        );
                                    }
                                    Ok(mut thread_json) => {
                                        if !with_tail {
                                            if !self.opt.asagi_mode
                                                || (self.opt.asagi_mode && board.with_extra_columns)
                                            {
                                                update_post_with_extra(&mut thread_json);
                                            }
                                        }
                                        let op_post = thread_json
                                            .get("posts")
                                            .unwrap()
                                            .as_array()
                                            .unwrap()
                                            .iter()
                                            .nth(0)
                                            .unwrap();

                                        let no: u64 = op_post["no"].as_u64().unwrap();
                                        let resto: u64 = op_post["resto"].as_u64().unwrap_or(0);

                                        if with_tail {
                                            // Check if we can use the tail.json
                                            // Check if we have the tail_id in the db
                                            let tail_id = op_post["tail_id"].as_u64().unwrap();
                                            let query = loop {
                                                match self
                                                    .db_client
                                                    .post_get_single(board.id, thread, tail_id)
                                                    .await
                                                {
                                                    Ok(b) => break b,
                                                    Err(e) => {
                                                        if get_ctrlc() {
                                                            return Ok((thread_type, deleted));
                                                        }
                                                        error!(
                                                            "({endpoint})\t/{board}/{thread}{tail}\t[post_get_single] [{err}]",
                                                            endpoint = thread_type,
                                                            board = &board.name,
                                                            thread = thread,
                                                            tail = if with_tail { "-tail " } else { " " },
                                                            err = e
                                                        );
                                                        db_retry().await;
                                                    }
                                                }
                                            };
                                            // If no Row returned, download thread normally
                                            if !query {
                                                with_tail = false;
                                                continue 'outer;
                                            }

                                            // Don't pop OP post (which has no time). It will be filtered upon inside `thread_upsert`.
                                            if !self.opt.asagi_mode
                                                || (self.opt.asagi_mode && board.with_extra_columns)
                                            {
                                                update_post_with_extra(&mut thread_json);
                                            }
                                        }

                                        if get_ctrlc() {
                                            return Ok((thread_type, deleted));
                                        }

                                        // Upsert Thread
                                        // This should never return 0
                                        let len = self
                                            .db_client
                                            .thread_upsert(board, &thread_json)
                                            .await
                                            .unwrap();

                                        if get_ctrlc() {
                                            return Ok((thread_type, deleted));
                                        }

                                        // Display
                                        // "download_thread: ({thread_type}) /{board}/{thread}{tail}{retry_status} {new_lm} | {prev_lm} |
                                        // {len}"
                                        /*pintln!("download_thread: (" (thread_type) ") /" (&board.name) "/" (thread)
                                            if with_tail { "-tail" } else { "" }
                                            if retry > 0 { " Retry #"(retry)" [RESOLVED]" }
                                            " "
                                            (&lm)
                                            " | "
                                            if let Some(_lm) =  &last_modified { (_lm) } else { "None" }
                                            " | "
                                            if (&last_modified).is_some() {
                                                "UPSERTED" if len == 0 { "" } else { ": "(len) }
                                            } else {
                                                "NEW" if len == 0 { "" } else { ": "(len) }
                                            }
                                            if len == 0 && !self.opt.asagi_mode { " | WARNING EMPTY SET" }
                                        );*/
                                        if len > 0 {
                                            if thread_type.is_threads() {
                                                info!(
                                                    "({endpoint})\t/{board}/{thread}\t\t{len} {tail}",
                                                    endpoint = thread_type,
                                                    board = &board.name,
                                                    thread = thread,
                                                    len = len,
                                                    tail = if with_tail { "[tail]" } else { "" }
                                                );
                                            } else {
                                                info!(
                                                    "({endpoint})\t/{board}/{thread}\t\t{len} {tail}",
                                                    endpoint = format!(ansi!("{;magenta}"), thread_type),
                                                    board = &board.name,
                                                    thread = thread,
                                                    len = len,
                                                    tail = if with_tail { "[tail]" } else { "" }
                                                );
                                            }
                                        }

                                        // Update thread's deleteds
                                        loop {
                                            let res = self
                                                .db_client
                                                .thread_update_deleteds(board, thread, &thread_json)
                                                .await;
                                            match res {
                                                Err(e) => {
                                                    if get_ctrlc() {
                                                        break;
                                                    }
                                                    error!(
                                                        "({endpoint})\t/{board}/{thread}\t{err}",
                                                        endpoint = thread_type,
                                                        board = &board.name,
                                                        thread = thread,
                                                        err = e,
                                                    );
                                                    db_retry().await;
                                                }
                                                Ok(either) => match either {
                                                    Either::Right(rows) => {
                                                        if let Some(mut rows) = rows {
                                                            for no in rows {
                                                                warn!(
                                                                    "({endpoint})\t/{board}/{thread}#{no}\t{deleted}",
                                                                    endpoint = thread_type,
                                                                    board = &board.name,
                                                                    thread = thread,
                                                                    no = no,
                                                                    deleted = format!(ansi!("{;yellow}"), "DELETED"),
                                                                );
                                                            }
                                                        }
                                                        break;
                                                    }
                                                    Either::Left(rows) => {
                                                        futures::pin_mut!(rows);
                                                        while let Some(Ok(row)) = rows.next().await
                                                        {
                                                            let no: Option<i64> = row.get("no");
                                                            let resto: Option<i64> =
                                                                row.get("resto");
                                                            if let Some(no) = no {
                                                                if let Some(resto) = resto {
                                                                    warn!(
                                                                        "({endpoint})\t/{board}/{thread}#{no}\t{deleted}",
                                                                        endpoint = thread_type,
                                                                        board = &board.name,
                                                                        thread = if resto == 0 { no } else { resto },
                                                                        no = no,
                                                                        deleted = format!(ansi!("{;yellow}"), "DELETED"),
                                                                    );
                                                                }
                                                            }
                                                        }

                                                        break;
                                                    }
                                                },
                                            }
                                        }

                                        // Get Media
                                        if board.with_full_media || board.with_thumbnails {
                                            if self.opt.asagi_mode {
                                                // Get list of media. Filter out `filedeleted`. Filter out non-media.
                                                let media_list: Vec<(
                                                    u64,
                                                    u64,
                                                    &str,
                                                    u64,
                                                    &str,
                                                    &str,
                                                )> = (&thread_json["posts"])
                                                    .as_array()
                                                    .unwrap()
                                                    .iter()
                                                    .filter(|&post_json| {
                                                        !post_json
                                                            .get("filedeleted")
                                                            .map(|j| j.as_u64())
                                                            .flatten()
                                                            .map_or(false, |filedeleted| {
                                                                filedeleted == 1
                                                            })
                                                            && post_json.get("md5").is_some()
                                                    })
                                                    .map(|v| {
                                                        (
                                                            v.get("resto")
                                                                .unwrap()
                                                                .as_u64()
                                                                .unwrap_or_default(),
                                                            v.get("no")
                                                                .unwrap()
                                                                .as_u64()
                                                                .unwrap_or_default(),
                                                            v.get("md5")
                                                                .unwrap()
                                                                .as_str()
                                                                .unwrap_or_default(),
                                                            v.get("tim")
                                                                .unwrap()
                                                                .as_u64()
                                                                .unwrap_or_default(),
                                                            v.get("ext")
                                                                .unwrap()
                                                                .as_str()
                                                                .unwrap_or_default(),
                                                            v.get("filename")
                                                                .unwrap()
                                                                .as_str()
                                                                .unwrap_or_default(),
                                                        )
                                                    })
                                                    .collect();

                                                let media_list_len = media_list.len();

                                                // Asagi
                                                // This query is much better than a JOIN
                                                // let sq = fomat!(
                                                //     "SELECT * FROM " (&board.name) "_images WHERE media_hash IN ("
                                                //     for (i, (md5, filename)) in media_list.iter().enumerate() {
                                                //         "\n"(format_sql_query::QuotedData(&md5.replace("\\", "")))
                                                //         if i < media_list_len-1 { "," } else { "" }
                                                //     }
                                                //     ");"
                                                // );

                                                if media_list.len() > 0 {
                                                    if board.with_thumbnails && board.name != "f" {
                                                        let r = media_list
                                                            .iter()
                                                            .map(|&details| {
                                                                let (
                                                                    resto,
                                                                    no,
                                                                    md5,
                                                                    tim,
                                                                    ext,
                                                                    filename,
                                                                ) = details;
                                                                self.download_media(
                                                                    board,
                                                                    (
                                                                        resto,
                                                                        no,
                                                                        Some(md5.into()),
                                                                        None,
                                                                        tim,
                                                                        ext.into(),
                                                                        filename.into(),
                                                                        None,
                                                                        None,
                                                                    ),
                                                                    MediaType::Thumbnail,
                                                                )
                                                            })
                                                            .collect::<Vec<_>>();

                                                        let mut stream_of_futures = stream::iter(r);

                                                        let mut fut = stream_of_futures
                                                            .buffer_unordered(
                                                                self.opt.limit_media as usize,
                                                            );
                                                        while let Some(res) = fut.next().await {
                                                            res.unwrap();
                                                        }
                                                    }
                                                    if get_ctrlc() {
                                                        break;
                                                    }
                                                    if board.with_full_media {
                                                        let r = media_list
                                                            .iter()
                                                            .map(|&details| {
                                                                let (
                                                                    resto,
                                                                    no,
                                                                    md5,
                                                                    tim,
                                                                    ext,
                                                                    filename,
                                                                ) = details;
                                                                self.download_media(
                                                                    board,
                                                                    (
                                                                        resto,
                                                                        no,
                                                                        Some(md5.into()),
                                                                        None,
                                                                        tim,
                                                                        ext.into(),
                                                                        filename.into(),
                                                                        None,
                                                                        None,
                                                                    ),
                                                                    MediaType::Full,
                                                                )
                                                            })
                                                            .collect::<Vec<_>>();

                                                        let mut stream_of_futures = stream::iter(r);

                                                        let mut fut = stream_of_futures
                                                            .buffer_unordered(
                                                                self.opt.limit_media as usize,
                                                            );
                                                        while let Some(res) = fut.next().await {
                                                            res.unwrap();
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Postgres Side

                                                // If 0 it usually means the thread hardly has any replies, if any.
                                                let next_no = {
                                                    // Boards like /f/ have media in the OP almost always, so that's where start should be
                                                    if board.name == "f" {
                                                        0
                                                    } else {
                                                        // First reply no, if not exists default to 0
                                                        thread_json["posts"]
                                                            .get(1)
                                                            .and_then(|j| j.get("no"))
                                                            .map(serde_json::Value::as_u64)
                                                            .flatten()
                                                            .unwrap_or_default()
                                                    }
                                                };

                                                loop {
                                                    // We basically want to get the same amount of posts from received from the json
                                                    // i.e The posts from the live thread minus the bumped off posts (if any) that's recorded in the db
                                                    // (if any) But queried from the database since
                                                    // it has sha256 & sha256t
                                                    let res = self
                                                        .db_client
                                                        .thread_get_media(board, thread, next_no)
                                                        .await;

                                                    match res {
                                                        Err(e) => {
                                                            if get_ctrlc() {
                                                                break;
                                                            }
                                                            error!(
                                                                "({endpoint})\t/{board}/{thread}#{no}\t[thread_get_media] [{err}]",
                                                                endpoint = thread_type,
                                                                board = &board.name,
                                                                thread = if resto == 0 { no } else { resto },
                                                                no = no,
                                                                err = e,
                                                            );
                                                            db_retry().await;
                                                        }
                                                        Ok(either) => {
                                                            if let Either::Left(rows) = either {
                                                                futures::pin_mut!(rows);
                                                                let mm =
                                                        // let mm : Vec<Result<(Vec<u8>, u64, String, String, Option<Vec<u8>, Option<Vec<u8>>>),_>> =
                                                        rows.map(|res|
                                                        res.map(|row| {
                                                            let resto = row.get::<&str, i64>("resto") as u64;
                                                            let no = row.get::<&str, i64>("no") as u64;
                                                            let md5 = row.get::<&str, Option<Vec<u8>>>("md5");
                                                            let tim = row.get::<&str, Option<i64>>("tim").map(|v| v as u64).unwrap_or_default();
                                                            let ext = row.get::<&str, Option<String>>("ext").unwrap_or_default();
                                                            let filename = row.get::<&str, Option<String>>("filename").unwrap_or_default();
                                                            let sha256 = row.get::<&str, Option<Vec<u8>>>("sha256");
                                                            let sha256t = row.get::<&str, Option<Vec<u8>>>("sha256t");
                                                            (resto, no, None, md5, tim, ext, filename, sha256, sha256t)
                                                        }
                                                        ))
                                                        .map(|v|v.unwrap())
                                                        .collect::<Vec<_>>().await;
                                                                // FIXME: Should not unwrap while streaming from a database!!
                                                                {
                                                                    // Downlaod Thumbnails
                                                                    if board.with_thumbnails
                                                                        && board.name != "f"
                                                                    {
                                                                        let r = mm.clone().into_iter().map(|details| self.download_media(board, details, MediaType::Thumbnail)).collect::<Vec<_>>();

                                                                        let mut stream_of_futures =
                                                                            stream::iter(r);

                                                                        let mut fut =
                                                                            stream_of_futures
                                                                                .buffer_unordered(
                                                                                    self.opt
                                                                                        .limit_media
                                                                                        as usize,
                                                                                );
                                                                        while let Some(res) =
                                                                            fut.next().await
                                                                        {
                                                                            res.unwrap();
                                                                        }
                                                                    }
                                                                }

                                                                if get_ctrlc() {
                                                                    return Ok((
                                                                        thread_type,
                                                                        deleted,
                                                                    ));
                                                                }

                                                                // Downlaod full media
                                                                if board.with_full_media {
                                                                    let r = mm
                                                                        .into_iter()
                                                                        .map(|details| {
                                                                            self.download_media(
                                                                                board,
                                                                                details,
                                                                                MediaType::Full,
                                                                            )
                                                                        })
                                                                        .collect::<Vec<_>>();

                                                                    let mut stream_of_futures =
                                                                        stream::iter(r);

                                                                    let mut fut = stream_of_futures
                                                                        .buffer_unordered(
                                                                            self.opt.limit_media
                                                                                as usize,
                                                                        );
                                                                    while let Some(res) =
                                                                        fut.next().await
                                                                    {
                                                                        res.unwrap();
                                                                    }
                                                                }
                                                                break; // exit the db loop
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if get_ctrlc() {
                                            break;
                                        }

                                        // Update thread's last_modified
                                        loop {
                                            let res = self
                                                .db_client
                                                .thread_update_last_modified(&lm, board.id, thread)
                                                .await;
                                            match res {
                                                Err(e) => {
                                                    if get_ctrlc() {
                                                        return Ok((thread_type, deleted));
                                                    }
                                                    error!(
                                                        "({endpoint})\t/{board}/{thread}\t[thread_update_last_modified] [{err}]",
                                                        endpoint = thread_type,
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        err = e,
                                                    );
                                                    db_retry().await;
                                                }
                                                Ok(_) => break,
                                            }
                                        }
                                        break; // exit the retry loop
                                    }
                                }
                            }
                            StatusCode::NOT_FOUND => {
                                if with_tail {
                                    with_tail = false;
                                    continue 'outer;
                                }
                                drop(sem);
                                deleted = true;
                                let tail = if with_tail { "-tail" } else { "" };
                                loop {
                                    let either =
                                        self.db_client.thread_update_deleted(board, thread).await;

                                    // Display the deleted thread
                                    match either {
                                        Err(e) => {
                                            if get_ctrlc() {
                                                break;
                                            }
                                            error!(
                                                "({endpoint})\t/{board}/{thread}{tail}\t[thread_update_deleted] [{err}]",
                                                endpoint = thread_type,
                                                board = &board.name,
                                                thread = thread,
                                                tail = if with_tail { "-tail " } else { " " },
                                                err = e
                                            );
                                            db_retry().await;
                                        }
                                        Ok(res) => {
                                            match res {
                                                Either::Right(rows) => {
                                                    if let Some(no) = rows {
                                                        warn!(
                                                            "({endpoint})\t/{board}/{thread}\t\t{deleted}{tail}",
                                                            endpoint = thread_type,
                                                            board = &board.name,
                                                            thread = no,
                                                            deleted = format!(ansi!("{;yellow}"), "DELETED"),
                                                            tail = if with_tail { " [tail]" } else { "" },
                                                        );

                                                        // Ignore the below comments. If it's deleted, it won't matter what last-modified it is.
                                                        //
                                                        // Due to triggers, when a board is updated/deleted/inserted, the `time_last` is updated
                                                        // So I have to reset it to the correct value based on the HTTP response `Last-Modified`
                                                        //
                                                        //
                                                        // Just to be safe?...
                                                        if !lm.is_empty() {
                                                            loop {
                                                                let res = self
                                                                    .db_client
                                                                    .thread_update_last_modified(
                                                                        &lm, board.id, thread,
                                                                    )
                                                                    .await;
                                                                match res {
                                                                    Err(e) => {
                                                                        if get_ctrlc() {
                                                                            break;
                                                                        }
                                                                        error!("thread_update_last_modified: {}", e);
                                                                        db_retry().await;
                                                                    }
                                                                    Ok(_) => break,
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                Either::Left(rows) => {
                                                    futures::pin_mut!(rows);
                                                    while let Some(row) = rows.next().await {
                                                        match row {
                                                            Ok(row) => {
                                                                let no: Option<i64> = row.get("no");
                                                                if let Some(no) = no {
                                                                    warn!(
                                                                        "({endpoint})\t/{board}/{thread}\t\t{deleted}{tail}",
                                                                        endpoint = thread_type,
                                                                        board = &board.name,
                                                                        thread = no,
                                                                        deleted = format!(ansi!("{;yellow}"), "DELETED"),
                                                                        tail = if with_tail { " [tail]" } else { "" },
                                                                    );
                                                                } else {
                                                                    error!(
                                                                        "({endpoint})\t/{board}/{thread}{tail}\t[thread_update_deleted] [`no` is empty]",
                                                                        endpoint = thread_type,
                                                                        board = &board.name,
                                                                        thread = thread,
                                                                        tail = if with_tail { "-tail " } else { " " },
                                                                    );
                                                                }
                                                            }
                                                            Err(e) => {
                                                                error!(
                                                                    "({endpoint})\t/{board}/{thread}{tail}\t[thread_update_deleted] [{err}]",
                                                                    endpoint = thread_type,
                                                                    board = &board.name,
                                                                    thread = thread,
                                                                    tail = if with_tail { "-tail " } else { " " },
                                                                    err = e
                                                                );
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }

                                return Ok((thread_type, deleted));
                            }
                            _ => {
                                // Don't output
                                if !startup {
                                    warn!(
                                        "({endpoint})\t/{board}/{thread}\t\t{status}{tail}",
                                        endpoint = thread_type,
                                        board = &board.name,
                                        thread = thread,
                                        status = format!(ansi!("{;yellow}"), status),
                                        tail = if with_tail { " [tail]" } else { "" },
                                    );
                                }
                                if status == StatusCode::NOT_MODIFIED {
                                    drop(sem);
                                    return Ok((thread_type, deleted));
                                }
                            }
                        }
                    }
                }

                if get_ctrlc() {
                    return Ok((thread_type, deleted));
                }
                sleep(Duration::from_secs(1)).await;
                if get_ctrlc() {
                    return Ok((thread_type, deleted));
                }
            }
            break;
        }
        Ok((thread_type, deleted))
    }

    async fn download_media(
        &self,
        board: &Board,
        details: MediaDetails,
        media_type: MediaType,
    ) -> Result<()> {
        let sem = SEMAPHORE_MEDIA.acquire(1).await;
        if get_ctrlc() {
            return Ok(());
        }

        // TEST CONCLUSION: Since this makes downloading sequential, basically one by one, it's really slow
        // to get everything.. But it does gaurantee no duplicates in the beginning!!

        // Test individual get for pg
        // let mut _sem_test = None;
        // if !self.opt.asagi_mode {
        //     _sem_test = Some(SEMAPHORE_MEDIA_TEST.acquire(1).await);
        // }
        // if get_ctrlc() {
        //     return Ok(());
        // }

        // This is from 4chan post / postgres post
        let (resto, no, md5_base64, md5, tim, ext, filename, sha256, sha256t) = details;

        let mut dir = None;
        let mut path = None;
        let mut url = None;

        // Check if the file exists (already downloaded) before downloading the file
        if self.opt.asagi_mode {
            let either = loop {
                let res = self
                    .db_client
                    .post_get_media(board, md5_base64.as_ref().unwrap(), None)
                    .await;
                match res {
                    Err(e) => {
                        if get_ctrlc() {
                            return Ok(());
                        }
                        error!(
                            "({endpoint})\t/{board}/{thread}#{no}\t[post_get_media] [{err}]",
                            endpoint = "media",
                            board = &board.name,
                            thread = if resto == 0 { no } else { resto },
                            no = no,
                            err = e,
                        );
                        db_retry().await;
                    }
                    Ok(either) => break either,
                }
            };
            if let Either::Right(Some(row)) = either {
                let banned = row
                    .get::<Option<u8>, &str>("banned")
                    .flatten()
                    .map_or_else(|| false, |v| v == 1);
                if banned {
                    warn!(
                        "({endpoint})\t/{board}/{thread}#{no}\t{message}",
                        endpoint = "media",
                        board = &board.name,
                        thread = if resto == 0 { no } else { resto },
                        no = no,
                        message = format!(ansi!("{;yellow}"), "Skipping banned media"),
                    );
                    return Ok(());
                }

                let media = row.get::<Option<String>, &str>("media").flatten().unwrap();
                let preview_op = row.get::<Option<String>, &str>("preview_op").flatten();
                let preview_reply = row.get::<Option<String>, &str>("preview_reply").flatten();
                let preview = preview_op
                    .map_or_else(|| preview_reply, |v| Some(v))
                    .unwrap();

                let tim_filename = if media_type == MediaType::Full {
                    media
                } else {
                    preview
                };

                // Directory Structure:
                // 1540970147550
                // {media_dir}/{board}/{image|thumb}/1540/97
                let _dir = fomat!(
                    (self.opt.media_dir.display()) "/" (&board.name) "/"
                    if media_type == MediaType::Full { "image" } else { "thumb" } "/"
                    (tim_filename[..4]) "/" (tim_filename[4..6])
                );
                let _path = fomat!(
                    (_dir) "/" (&tim_filename)
                );
                if exists(&_path) {
                    return Ok(());
                }
                path = Some(_path);
                dir = Some(_dir);
                let url_string = fomat!(
                    (&self.opt.media_url) (&board.name)"/"
                    if board.name == "f" { (&filename)(ext) }  else { (tim_filename) }
                );
                url = Some(url::Url::parse(&url_string).unwrap());
            }
        } else {
            let _checksum = {
                if media_type == MediaType::Full {
                    sha256
                } else {
                    // Thumbnails aren't unique and can have duplicates
                    // So check the database if we already have it or not
                    loop {
                        match self
                            .db_client
                            .post_get_media(board, "", sha256t.as_ref().map(|v| v.as_slice()))
                            .await
                        {
                            Err(e) => {
                                if get_ctrlc() {
                                    return Ok(());
                                }
                                error!(
                                    "({endpoint})\t/{board}/{thread}#{no}\t[post_get_media] [{err}]",
                                    endpoint = "media",
                                    board = &board.name,
                                    thread = if resto == 0 { no } else { resto },
                                    no = no,
                                    err = e,
                                );
                                db_retry().await;
                            }
                            Ok(Either::Right(_)) => unreachable!(),
                            Ok(Either::Left(s)) => {
                                break s
                                    .and_then(|row| row.get::<&str, Option<Vec<u8>>>("sha256t"));
                            }
                        }
                    }
                }
            };

            // Directory Structure:
            // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
            // {media_dir}/{full|thumbnails}/1/0a
            if let Some(checksum) = &_checksum {
                let hash = format!("{:02x}", HexSlice(checksum.as_slice()));
                let len = hash.len();
                // pintln!("download_media: (threads) /"(&board.name)"/" if resto == 0 { (no) } else { (resto)
                // }"#"(no)" | " [md5] " | " (&hash) " : " (len));
                if len == 64 {
                    let _path = fomat!
                    (
                        (self.opt.media_dir.display()) "/"
                        if media_type == MediaType::Full { "full" } else { "thumbnails" } "/"
                        (&hash[len - 1..]) "/" (&hash[len -3..len-1]) "/"
                        (&hash)
                        if media_type == MediaType::Full { (&ext) } else { ".jpg" }
                    );
                    if exists(&_path) {
                        // Upsert the thumbnail at the given md5 since full medias
                        // can have duplicate thumbnails, if this particular md5 (full-media) doesn't have it
                        // then upsert the hash, since we have it on the filesystem.
                        if media_type == MediaType::Thumbnail {
                            for retry in 0..=3u8 {
                                let res = self
                                    .db_client
                                    .post_upsert_media(
                                        md5.as_ref().unwrap().as_slice(),
                                        None,
                                        _checksum.as_ref().map(|v| v.as_slice()),
                                    )
                                    .await;
                                match res {
                                    Err(e) => {
                                        error!(
                                            "({endpoint})\t/{board}/{thread}#{no}\t[post_upsert_media] [{err}]",
                                            endpoint = "media",
                                            board = &board.name,
                                            thread = if resto == 0 { no } else { resto },
                                            no = no,
                                            err = e,
                                        );
                                    }
                                    Ok(_) => {
                                        break;
                                    }
                                }
                                sleep(Duration::from_millis(500)).await;
                            }
                        }
                        return Ok(());
                    } else {
                        error!(
                            "({endpoint})\t/{board}/{thread}#{no}\tExists in db but not in filesystem!: `{path:?}`",
                            endpoint = "media",
                            board = &board.name,
                            thread = if resto == 0 { no } else { resto },
                            no = no,
                            path = path,
                        );
                    }
                } else {
                    error!(
                        "({endpoint})\t/{board}/{thread}#{no}\tError! Hash found to be {len} chars long when it's supposed to be 64",
                        endpoint = "media",
                        board = &board.name,
                        thread = if resto == 0 { no } else { resto },
                        no = no,
                        len = len,
                    );
                }
                // path = Some(_path); // used by asagi
            }
            let url_string = fomat!(
                (&self.opt.media_url) (&board.name)"/"
                if board.name == "f" {
                    (&filename)(ext)
                }  else {
                    (tim) if media_type == MediaType::Full { (ext) } else { "s.jpg" }
                }
            );
            url = Some(url::Url::parse(&url_string).unwrap());
        }

        // Download the file
        if let Some(_url) = url {
            let mut retry = -1;
            loop {
                retry = retry + 1;
                if get_ctrlc() {
                    break;
                }
                let start_time = Instant::now();
                match self.client.get(_url.as_str()).send().await {
                    Err(e) => error!(
                        "({endpoint})\t/{board}/{thread}#{no}\t[{err}]",
                        endpoint = "media",
                        board = &board.name,
                        thread = if resto == 0 { no } else { resto },
                        no = no,
                        err = e,
                    ),
                    Ok(resp) => {
                        let status = resp.status();
                        match status {
                            StatusCode::NOT_FOUND => {
                                // epintln!("download_media: /"(&board.name)"/" if resto == 0 { (no) } else { (resto)
                                // }"#"(no)" " (StatusCode::NOT_FOUND));
                                break;
                            }
                            StatusCode::OK => {
                                // Going here means that we don't have the file

                                if get_ctrlc() {
                                    break;
                                }
                                if self.opt.asagi_mode {
                                    // Make dirs
                                    if let Some(_dir) = &dir {
                                        let res = smol::Unblock::new(create_dir_all(_dir.as_str()))
                                            .into_inner()
                                            .await;
                                        if let Err(e) = res {
                                            error!(
                                                "({endpoint})\t/{board}/{thread}#{no}\t[{err}]",
                                                endpoint = "media",
                                                board = &board.name,
                                                thread = if resto == 0 { no } else { resto },
                                                no = no,
                                                err = e,
                                            );
                                        }
                                    }
                                    if path.as_ref().is_none() {
                                        error!(
                                            "({endpoint})\t/{board}/{thread}#{no}\tFile path is empty! This isn't supposed to happen!",
                                            endpoint = "media",
                                            board = &board.name,
                                            thread = if resto == 0 { no } else { resto },
                                            no = no,
                                        );
                                        continue;
                                    }
                                    let file_path = path.as_ref().unwrap();
                                    let res = {
                                        match download_and_hash_media(
                                            resp,
                                            &file_path,
                                            media_type,
                                            self.opt.asagi_mode,
                                        )
                                        .await
                                        {
                                            Err(e) => {
                                                // Going here probably means invalid file. Continue the while loop to redownload it
                                                if get_ctrlc() {
                                                    return Ok(());
                                                }

                                                if retry == 0 {
                                                    error!(
                                                            "({endpoint})\t/{board}/{thread}#{no}\t[download_and_hash_media] [{status}] [{err}]",
                                                            endpoint = "media",
                                                            board = &board.name,
                                                            thread = if resto == 0 { no } else { resto },
                                                            no = no,
                                                            status = status,
                                                            err = e,
                                                        );
                                                } else {
                                                    error!(
                                                            "({endpoint})\t/{board}/{thread}#{no}\t[download_and_hash_media] [{status}] [Retry #{retry}] [{err}]",
                                                            endpoint = "media",
                                                            board = &board.name,
                                                            thread = if resto == 0 { no } else { resto },
                                                            no = no,
                                                            status = status,
                                                            retry = retry,
                                                            err = e,
                                                        );
                                                }
                                                db_retry().await;
                                                continue;
                                            }
                                            Ok(opt) => opt,
                                        }
                                    };
                                    if res.is_none() || get_ctrlc() {
                                        return Ok(());
                                    }
                                    if retry > 0 {
                                        info!(
                                                "({endpoint})\t/{board}/{thread}#{no}\t[download_and_hash_media] {resolved}",
                                                endpoint = "media",
                                                board = &board.name,
                                                thread = if resto == 0 { no } else { resto },
                                                no = no,
                                                resolved = format!(ansi!("{;green}"), "RESOLVED")
                                            );
                                    }
                                    let (file_size, hasher, _) = res.unwrap();
                                    // let hasher = res.unwrap().0;
                                    // Only check md5 for full media
                                    if media_type == MediaType::Full {
                                        let result = hasher.finalize();
                                        let md5_bytes =
                                            base64::decode(md5_base64.as_ref().unwrap().as_bytes())
                                                .unwrap();
                                        if md5_bytes.as_slice() != result.as_slice() {
                                            if retry > 0 {
                                                error!(
                                                        "({endpoint})\t/{board}/{thread}#{no}\tHashes don't match! [Retry #{retry}]",
                                                        endpoint = "media",
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        no = no,
                                                        retry = retry,
                                                    );
                                            } else {
                                                error!(
                                                        "({endpoint})\t/{board}/{thread}#{no}\tHashes don't match!",
                                                        endpoint = "media",
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        no = no,
                                                    );
                                            }
                                            continue;
                                        }
                                    }

                                    // Display media info
                                    let elapsed = start_time.elapsed().as_millis();
                                    info!(
                                            "({endpoint})\t/{board}/{thread}#{no}\tDownloaded {media_type}/{filename}  {file_size} bytes. Took {elapsed} ms.",
                                            endpoint = "media",
                                            board = &board.name,
                                            thread = if resto == 0 { no } else { resto },
                                            no = no,
                                            media_type = media_type.human_str(),
                                            filename = fomat!((tim) if media_type == MediaType::Full { (ext) } else { "s.jpg" } ),
                                            file_size = file_size,
                                            elapsed = elapsed,
                                        );

                                    break;
                                } else {
                                    let now = std::time::SystemTime::now()
                                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                                        .map(|v| v.as_nanos())
                                        .unwrap_or(tim.into());

                                    // Unique tmp filename so it won't clash when saving
                                    let tmp_path = fomat!((&self.opt.media_dir.display()) "/tmp/"
                                    (&board.name)"-"
                                    (resto) "-"
                                    (no) "-"
                                    {"{:02x}", HexSlice(md5.as_ref().unwrap().as_slice()) }  "-"
                                    (now)
                                    if media_type == MediaType::Full { (ext) } else { "s.jpg" }
                                    );

                                    // Download File and calculate hashes
                                    let res = {
                                        match download_and_hash_media(
                                            resp,
                                            &tmp_path,
                                            media_type,
                                            self.opt.asagi_mode,
                                        )
                                        .await
                                        {
                                            Err(e) => {
                                                // Going here probably means invalid file. Continue the while loop to redownload it
                                                if get_ctrlc() {
                                                    return Ok(());
                                                }
                                                if retry == 0 {
                                                    error!(
                                                        "({endpoint})\t/{board}/{thread}#{no}\t[download_and_hash_media] [{status}] [{err}]",
                                                        endpoint = "media",
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        no = no,
                                                        status = status,
                                                        err = e,
                                                    );
                                                } else {
                                                    error!(
                                                        "({endpoint})\t/{board}/{thread}#{no}\t[download_and_hash_media] [{status}] [Retry #{retry}] [{err}]",
                                                        endpoint = "media",
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        no = no,
                                                        status = status,
                                                        retry = retry,
                                                        err = e,
                                                    );
                                                }
                                                db_retry().await;
                                                continue;
                                            }
                                            Ok(opt) => opt,
                                        }
                                    };
                                    if res.is_none() || get_ctrlc() {
                                        return Ok(());
                                    }
                                    if retry > 0 {
                                        info!(
                                            "({endpoint})\t/{board}/{thread}#{no}\t[download_and_hash_media] {resolved}",
                                            endpoint = "media",
                                            board = &board.name,
                                            thread = if resto == 0 { no } else { resto },
                                            no = no,
                                            resolved = format!(ansi!("{;green}"), "RESOLVED")
                                        );
                                    }
                                    let (file_size, hasher, hasher_sha256) = res.unwrap();
                                    let hasher_sha256 = hasher_sha256.unwrap();
                                    let result_sha256 = hasher_sha256.finalize();
                                    let hash = format!("{:02x}", result_sha256);
                                    let len = hash.len();

                                    // Only check md5 for full media
                                    if media_type == MediaType::Full {
                                        let result = hasher.finalize();
                                        if md5.as_ref().unwrap().as_slice() != result.as_slice() {
                                            error!(
                                                "{}",
                                                fomat!("download_media: Hashes don't match! /" (&board.name) "/"
                                                if resto == 0 { (no) } else { (resto) }
                                                "#" (no)
                                                {" `{:02x}`",HexSlice(md5.as_ref().unwrap().as_slice()) } {" != `{:02x}`", &result}
                                                if retry > 0 { " [Retry #" (retry) "]" } else { "" }
                                                )
                                            );
                                            let _ = std::fs::remove_file(&tmp_path);
                                            continue;
                                        }
                                    }

                                    // Display media info
                                    let elapsed = start_time.elapsed().as_millis();
                                    info!(
                                        "({endpoint})\t/{board}/{thread}#{no}\tDownloaded {media_type}/{filename}  {file_size} bytes. Took {elapsed} ms.",
                                        endpoint = "media",
                                        board = &board.name,
                                        thread = if resto == 0 { no } else { resto },
                                        no = no,
                                        media_type = media_type.human_str(),
                                        filename = fomat!((tim) if media_type == MediaType::Full { (ext) } else { "s.jpg" } ),
                                        file_size = file_size,
                                        elapsed = elapsed,
                                    );

                                    // Move the tmp file to it's final path
                                    if len == 64 {
                                        let _dir = fomat!
                                        (
                                            (self.opt.media_dir.display()) "/"
                                            if media_type == MediaType::Full { "full" } else { "thumbnails" } "/"
                                            (&hash[len - 1..]) "/" (&hash[len -3..len-1])
                                        );
                                        let _path = fomat!
                                        (
                                            (_dir) "/"
                                            (&hash)
                                            if media_type == MediaType::Full { (&ext) } else { ".jpg" }
                                        );

                                        if exists(&_path) {
                                            // FIXME Becuase this `download_media` method is run concurrently
                                            // If in the beginning there's no hashes, this error will always be reported
                                            // because hashes are upserted at the end of this method, but they're all being ran
                                            // at the same time, so there won't be any hashes to work with in the beginning.
                                            // As the database get's populated with more hashes, this error will fade away.
                                            // Another solutions is to run this method sequentially but then we lose the async http get
                                            // for lots of media..
                                            // Another solution is to use md5 on the filesystem since we're checking with md5 anyways.
                                            //
                                            // Ignore error report for now.
                                            // epintln!("download_media: `"
                                            // {"{:02x}",HexSlice(md5.as_ref().unwrap().as_slice()) }
                                            // " | "
                                            // (&_path)
                                            // "` exists! Downloaded for nothing.. Perhaps you didn't upload your hashes to the database
                                            // beforehand?");

                                            // Try to upsert to database
                                            let mut success = true;
                                            for _ in 0..=3u8 {
                                                let res = self
                                                    .db_client
                                                    .post_upsert_media(
                                                        md5.as_ref().unwrap().as_slice(),
                                                        if media_type == MediaType::Full {
                                                            Some(result_sha256.as_slice())
                                                        } else {
                                                            None
                                                        },
                                                        if media_type == MediaType::Thumbnail {
                                                            Some(result_sha256.as_slice())
                                                        } else {
                                                            None
                                                        },
                                                    )
                                                    .await;
                                                if let Err(e) = res {
                                                    error!(
                                                        "({endpoint})\t/{board}/{thread}#{no}\t[post_upsert_media] [{err}]",
                                                        endpoint = "media",
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        no = no,
                                                        err = e,
                                                    );
                                                    success = false;
                                                } else {
                                                    success = true;
                                                    break;
                                                }
                                                if get_ctrlc() {
                                                    success = true;
                                                    break;
                                                }
                                                sleep(Duration::from_millis(500)).await;
                                            }

                                            // Delete temp file after since going in this block means the file exists
                                            let _ = std::fs::remove_file(&tmp_path);

                                            // Exit if exists
                                            if success {
                                                break;
                                            }
                                        }

                                        // Make dirs
                                        let dirs_result =
                                            smol::Unblock::new(create_dir_all(_dir.as_str()))
                                                .into_inner()
                                                .await;
                                        match dirs_result {
                                            Err(e) => error!(
                                                "({endpoint})\t/{board}/{thread}#{no}\tError creating dirs `{dir}` [{err}]",
                                                endpoint = "media",
                                                board = &board.name,
                                                thread = if resto == 0 { no } else { resto },
                                                no = no,
                                                dir = &_dir,
                                                err = e,
                                            ),
                                            Ok(_) => {
                                                // Move to final path
                                                let rename_result = smol::Unblock::new(std::fs::rename(&tmp_path, &_path)).into_inner().await;
                                                match rename_result {
                                                    Err(e) => error!(
                                                        "({endpoint})\t/{board}/{thread}#{no}\tError moving `{tmp}` to `{dest}` [{err}]",
                                                        endpoint = "media",
                                                        board = &board.name,
                                                        thread = if resto == 0 { no } else { resto },
                                                        no = no,
                                                        tmp = &tmp_path,
                                                        dest = &_path,
                                                        err = e,
                                                    ),
                                                    Ok(_) => {
                                                        // TODO clear this
                                                        /*
                                                        {
                                                            let mut file = OpenOptions::new().write(true).append(true).open("my-file.txt").unwrap();

                                                            if let Err(e) = fomat_macros::witeln!(file, "download_media: /"(&board.name)"/" if resto == 0 { (no) } else { (resto)
                                                        }"#"(no) { " {:02x} | ", HexSlice(md5.as_ref().unwrap().as_slice()) } (&_path)  )
                                                            {
                                                                eprintln!("Couldn't write to file: {}", e);
                                                            }
                                                        }
                                                        */

                                                        // Try to upsert to database
                                                        let mut success = true;
                                                        for _ in 0..=5u8 {
                                                            let res = self
                                                                .db_client
                                                                .post_upsert_media(
                                                                    md5.as_ref().unwrap().as_slice(),
                                                                    if media_type == MediaType::Full { Some(result_sha256.as_slice()) } else { None },
                                                                    if media_type == MediaType::Thumbnail { Some(result_sha256.as_slice()) } else { None },
                                                                )
                                                                .await;
                                                            if let Err(e) = res {
                                                                error!(
                                                                    "({endpoint})\t/{board}/{thread}#{no}\t[post_upsert_media] [{err}]",
                                                                    endpoint = "media",
                                                                    board = &board.name,
                                                                    thread = if resto == 0 { no } else { resto },
                                                                    no = no,
                                                                    err = e,
                                                                );
                                                                success = false;
                                                            } else {
                                                                success = true;
                                                                break;
                                                            }
                                                            if get_ctrlc() {
                                                                break;
                                                            }
                                                            sleep(Duration::from_millis(500)).await;
                                                        }
                                                        if success {
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        error!(
                                            "({endpoint})\t/{board}/{thread}#{no}\tError! Hash found to be {len} chars long when it's supposed to be 64",
                                            endpoint = "media",
                                            board = &board.name,
                                            thread = if resto == 0 { no } else { resto },
                                            no = no,
                                            len = len,
                                        );
                                    }
                                }
                            }
                            status => error!(
                                "({endpoint})\t/{board}/{thread}#{no}\t{status}",
                                endpoint = "media",
                                board = &board.name,
                                thread = if resto == 0 { no } else { resto },
                                no = no,
                                status = status,
                            ),
                        }
                    }
                }
            }
        } else {
            error!(
                "({endpoint})\t/{board}/{thread}#{no}\tNo URL was found! This shouldn't have happen!",
                endpoint = "media",
                board = &board.name,
                thread = if resto == 0 { no } else { resto },
                no = no,
            );
        }

        Ok(())
    }
}

async fn download_and_hash_media(
    resp: reqwest::Response,
    path: &str,
    media_type: MediaType,
    asagi_mode: bool,
) -> Result<Option<(usize, Md5, Option<sha2::Sha256>)>> {
    let mut file = smol::Unblock::new(File::create(path)?);
    let mut writer = io::BufWriter::new(&mut file);
    let mut resp = resp;
    let mut hasher = Md5::new();
    let mut hasher_sha256 = if !asagi_mode {
        Some(sha2::Sha256::new())
    } else {
        None
    };
    let mut len: usize = 0;
    while let Some(item) = resp.chunk().await? {
        if media_type == MediaType::Full && get_ctrlc() {
            writer.flush().await?;
            writer.close().await?;
            let _ = std::fs::remove_file(path);
            return Ok(None);
        }
        let _item = item.as_ref();
        if media_type == MediaType::Full {
            hasher.update(_item);
        }
        if let Some(mut _hasher_sha256) = hasher_sha256.as_mut() {
            _hasher_sha256.update(_item);
        };
        len = len + _item.len();
        writer.write_all(_item).await?;
    }
    writer.flush().await?;
    writer.close().await?;
    Ok(Some((len, hasher, hasher_sha256)))
}

// TODO clear this
// #[allow(unused_imports)]
// use std::{fs::OpenOptions, io::prelude::*};
