#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]
#![recursion_limit = "512"]

// use anyhow::{anyhow, Result};
// use futures::prelude::*;
// use futures::io::{self, AsyncReadExt, AsyncWriteExt, Cursor};
// use async_ctrlc::CtrlC;
// use futures::io::AsyncWriteExt;
// use async_std::sync::RwLock;
// use futures::io::{AsyncReadExt, AsyncWriteExt};
use async_rwlock::RwLock;
use color_eyre::eyre::{eyre, Result};
use fomat_macros::{epintln, fomat, pintln};
use futures::future;

use async_trait::async_trait;
use ctrlc;
use futures::{channel::oneshot, future::Either, stream::FuturesUnordered};
use futures_lite::*;
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use reqwest::{
    self,
    header::{IF_MODIFIED_SINCE, LAST_MODIFIED},
    IntoUrl, StatusCode,
};
use smol::{unblock, Task, Timer};
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{create_dir_all, File},
    io::Read,
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    thread,
    time::{Duration, Instant},
};

pub mod config;
pub mod yotsuba;
use config::*;
use yotsuba::update_post_with_extra;

#[path = "sql/sql.rs"]
pub mod sql;

// mysql://root:zxc@localhost:3306/ena2?charset=utf8mb4
// postgresql://postgres:zxc@localhost:5432/ena?client_encoding=utf8

#[async_trait]
pub trait HttpClient: Sync + Send {
    async fn gett<U: IntoUrl + Send + Debug + Clone>(&self, url: U, last_modified: &Option<String>) -> Result<(StatusCode, String, Vec<u8>)>;
}

#[async_trait]
pub trait Archiver {
    async fn run(&self) -> Result<()>;
    async fn download_board_and_thread(&self, boards: Option<Vec<Board>>, thread_entry: Option<(&Board, u64)>) -> Result<()>;
    async fn download_boards_index(&self) -> Result<()>;
    async fn download_board(&self, board_info: &Board, thread_type: ThreadType, startup: bool) -> Result<()>;
    async fn download_thread(&self, board_info: &Board, thread: u64, thread_type: ThreadType) -> Result<ThreadType>;
    async fn download_media(&self, board_info: &Board, details: MediaDetails, media_type: MediaType) -> Result<()>;
}
/*
#[derive(Deserialize, Debug)]
struct Post {
    no:    u64,
    resto: u64,
    tim:   u64,
    ext:   String,
}
impl Default for Post {
    fn default() -> Self {
        let t = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
        Self { no: t.as_secs(), resto: t.as_secs(), tim: t.as_secs(), ext: String::new() }
    }
}*/

async fn sleep(dur: Duration) {
    Timer::new(dur).await;
}

fn exists<P: AsRef<std::path::Path>>(path: P) -> bool {
    std::fs::metadata(path).is_ok()
}

/// Download to file
/*
async fn dl(client: &reqwest::Client, url: String, post: Post, config: &Opt) -> Result<()> {
    if config.debug {
        pintln!((url));
    }
    for _ in 0..=3 {
        let resp = client.get(&url).send().await.unwrap();
        match resp.status() {
            StatusCode::OK => {
                let _url = url::Url::parse(&url).unwrap();
                let last = _url.path_segments().map(Iterator::last).flatten().unwrap();

                let file_path = fomat!((config.media_dir.to_str().unwrap())"/"(last));

                let file = blocking!(File::create(&file_path))?;
                let mut file = writer(file);

                // let bytes = resp.bytes().await.unwrap();
                // io::copy_buf(bytes.as_ref(), &mut file).await.unwrap();
                // writer.close().await.unwrap();
                let mut stream = resp.bytes_stream();
                while let Some(item) = stream.next().await {
                    file.write_all(&item?).await.unwrap();
                }
                // file.write_all(bytes.as_ref()).await.unwrap();
                file.flush().await.unwrap();
            }
            StatusCode::NOT_FOUND => epintln!((StatusCode::NOT_FOUND)": "(&url)" "[post]),
            status => epintln!((status)": "(&url)),
        }
        break;
    }
    Ok(())
}
*/

/// Implementation of `HttpClient` for `reqwest`.
#[async_trait]
impl HttpClient for reqwest::Client {
    async fn gett<U: IntoUrl + Send + Debug + Clone>(&self, url: U, last_modified: &Option<String>) -> Result<(StatusCode, String, Vec<u8>)> {
        // let url: &str = url.into();
        // let _url = url.clone();
        let res = {
            if let Some(lm) = last_modified {
                if lm.is_empty() {
                    self.get(url)
                } else {
                    self.get(url).header(IF_MODIFIED_SINCE, lm)
                }
            } else {
                self.get(url)
            }
            .send()
            .await
        }?;

        // Last-Modified Should never be empty is status is OK
        // Which we always check after this method is called
        let lm = res.headers().get(LAST_MODIFIED).map(|r| r.to_str().ok()).flatten().unwrap_or("");

        Ok((res.status(), lm.into(), res.bytes().await.map(|b| b.to_vec()).unwrap_or(vec![])))
    }
}

/*
async fn get_json(client: &reqwest::Client, url: &str, last_modified: Option<&str>) -> Result<serde_json::Value> {
    for idx in 0..3u8 {
        let resp = if last_modified.is_some() { client.get(url).header(IF_MODIFIED_SINCE, last_modified.unwrap()) } else { client.get(url) }.send().await;
        match resp {
            // this will reach the end of idx if it keeps returning none, ie 404
            Ok(res) => match res.status() {
                StatusCode::OK =>
                    if let Ok(j) = res.json().await {
                        return Ok(j);
                    },
                StatusCode::NOT_FOUND => epintln!((StatusCode::NOT_FOUND)": "(&url)),
                StatusCode::NOT_MODIFIED => epintln!((StatusCode::NOT_FOUND)": "(&url)),
                status => epintln!((status)": "(&url)),
            },
            // hardly reaches the end here for errors
            // probably network error
            Err(err) => {
                if idx == 5 {
                    return Err(eyre!(err));
                }
                epintln!((err));
            }
        }
    }
    Err(eyre!("Something went wrong!")) // when none
}
*/

async fn md5_of_file(file_path: String) -> Result<Vec<u8>> {
    let result = unblock!({
        // Check disk
        let mut file = File::open(file_path);

        // Get the hash for file on disk
        let mut hasher = Md5::new();
        let mut contents = Vec::new();
        file.map(|mut f| f.read_to_end(&mut contents)).map(|f| {
            f.map(|i| {
                hasher.update(&contents);
                hasher.finalize()
            })
        })
    })??;
    Ok(result.to_vec())
}

/*
async fn process_thread(thread: serde_json::Value, client: &reqwest::Client, config: &Opt) -> Result<()> {
    let mut fut = FuturesUnordered::new();
    let no = thread.get("no").unwrap().as_i64().unwrap();
    let posts_json = get_json(client, &fomat!((&config.api_url)"/a/thread/"(no)".json")).await.unwrap();
    let posts = posts_json.get("posts").unwrap().as_array().unwrap();
    for post in posts {
        if let Some(md5) = post.get("md5") {
            let tim = post.get("tim").unwrap().as_i64().unwrap();
            // TODO: Media
            let ext = if config.with_media { post.get("ext").unwrap().as_str().unwrap() } else { "s.jpg" };
            // let ext = "s.jpg";
            let url = fomat!((&config.media_url)"/a/"(tim)(ext));
            let file_path = fomat!((&config.media_dir.to_str().unwrap())"/"(tim)(ext));
            let path_file = std::path::Path::new(&file_path);

            // let mut run:bool = false;

            if path_file.is_file() {
                // TODO: Can't verify thumbs, only full media
                if ext != "s.jpg" {
                    // md5 from json to binary
                    let md5 = md5.as_str().unwrap();
                    let md5_bytes = base64::decode(md5.as_bytes())?;
                    let result = md5_of_file((&file_path).clone()).await.unwrap();

                    // Compare
                    if &md5_bytes[..] != &result[..] {
                        // run = true;
                        let no = post.get("no").unwrap().as_i64().unwrap();
                        let resto = post.get("resto").unwrap().as_i64().unwrap();
                        let po = Post { no: no as u64, resto: resto as u64, tim: tim as u64, ext: ext.into() };
                        // TODO: Hashes don't match on disk! When downloading full media
                        // epintln!("Page #"(p)" Thread #"(t)" Hashes don't match on disk!: "[po]);
                        epintln!("Hashes don't match on disk!: "[po]);
                        erintln!({"{:x?}", &md5_bytes[..]});
                        erintln!({"{:x?}", &result[..]});
                        fut.push(dl(&client, url, po, &config));
                    }
                }
            } else {
                // run = true;
                let no = post.get("no").unwrap().as_i64().unwrap();
                let resto = post.get("resto").unwrap().as_i64().unwrap();
                let po = Post { no: no as u64, resto: resto as u64, tim: tim as u64, ext: ext.into() };
                fut.push(dl(&client, url, po, &config));
            }

            // stream/chunk download media buffered to 10 concurrenlty rather than like 300
            // concurrently/parallel  -> Only poll if >= limit
            let media_threads = config.media_threads.max(5); // Choose the max between user and 5
            if fut.len() >= (media_threads as usize) {
                while let Some(_) = fut.next().await {}
            }

            // break;
        }
    }

    // Finish off any images missing in the thread
    while let Some(_) = fut.next().await {}

    // TODO: CTRL-C has to be implemented here since where getting threads concurrently!
    // if let Ok(item) = receiver.try_recv() {
    //     if matches!(item, Some(true)) {
    //         return Ok(());
    //     }
    // }
    Ok(())
}
*/

static CTRLC: Lazy<AtomicU8> = Lazy::new(|| AtomicU8::new(0));
static INIT: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));

// Semaphores to limit concurrency
static SEMAPHORE: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, config::SEMAPHORE_AMOUNT.load(Ordering::SeqCst) as usize));
static MEDIA_SEMAPHORE: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| futures_intrusive::sync::Semaphore::new(true, config::MEDIA_SEMAPHORE_AMOUNT.load(Ordering::SeqCst) as usize));

struct FourChan<D> {
    client:    reqwest::Client,
    db_client: D,
    opt:       Opt,
}

// pub trait Site {}
// impl Site for FourChan {}

impl<D> FourChan<D>
where D: sql::QueryExecutor
{
    pub async fn new(client: reqwest::Client, db_client: D, opt: Opt) -> Self {
        Self { client, db_client, opt }
    }
}

async fn create_client(origin: &str, opt: &Opt) -> Result<reqwest::Client> {
    // let mut headers = reqwest::header::HeaderMap::new();
    // headers.insert("Connection", "keep-alive".parse().unwrap());
    // headers.insert("Origin", origin.parse().unwrap());
    // let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0";
    // reqwest::Client::builder().default_headers(headers).user_agent(ua).use_rustls_tls().no_proxy().
    // build()
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Connection", "keep-alive".parse()?);
    headers.insert("Origin", origin.parse()?);
    let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0";
    let mut proxies_list = vec![];
    // let pr = opt.proxies;
    if let Some(proxies) = &opt.proxies {
        for proxy in proxies {
            // Test each proxy with retries
            let proxy_url = &proxy.url;
            if proxy_url.is_empty() {
                continue;
            }
            let username = proxy.username.as_ref();
            let password = proxy.password.as_ref();
            let mut _proxy = reqwest::Proxy::https(proxy_url.as_str())?;
            if username.is_some() && password.is_some() {
                let user = username.unwrap();
                let pass = password.unwrap();
                if !user.is_empty() {
                    _proxy = _proxy.basic_auth(user.as_str(), pass.as_str());
                }
            }
            proxies_list.push(_proxy.clone());
            for retry in 0..=3u8 {
                let mut cb = reqwest::Client::builder().default_headers(headers.clone()).user_agent(ua).use_rustls_tls().gzip(true).brotli(true);
                let mut cb = cb.proxy(_proxy.clone()).build()?;
                pintln!("Testing proxy: "(proxy_url));
                // TODO should be a head request?
                let (status, last_modified, body) = cb.gett("https://a.4cdn.org/po/catalog.json", &None).await?;
                if status == StatusCode::OK {
                    break;
                } else {
                    if retry == 3 {
                        epintln!("Proxy: " (proxy_url) " ["(status)"]");
                        proxies_list.pop();
                    }
                }
            }
        }
        if proxies_list.len() > 0 {
            let mut cb = reqwest::Client::builder().default_headers(headers.clone()).user_agent(ua).use_rustls_tls().gzip(true).brotli(true);
            for proxy in proxies_list {
                cb = cb.proxy(proxy);
            }
            let client = cb.build().map_err(|e| eyre!(e));
            client
        } else {
            let client = reqwest::Client::builder().default_headers(headers.clone()).user_agent(ua).use_rustls_tls().gzip(true).brotli(true).no_proxy().build().map_err(|e| eyre!(e));
            client
        }
    } else {
        // same as above
        let client = reqwest::Client::builder().default_headers(headers.clone()).user_agent(ua).use_rustls_tls().gzip(true).brotli(true).no_proxy().build().map_err(|e| eyre!(e));
        client
    }
}

async fn test_main() -> Result<()> {
    let opt = config::get_opt()?;

    if opt.debug {
        pintln!((serde_json::to_string_pretty(&opt).unwrap()));
        return Ok(());
    }

    if !opt.asagi_mode {
        let (client, connection) = tokio_postgres::connect(opt.database.url.as_ref().unwrap(), tokio_postgres::NoTls).await.unwrap();
        Task::spawn(async move {
            if let Err(e) = connection.await {
                epintln!("connection error: "(e));
            }
        })
        .detach();
        let fchan = FourChan::new(create_client("http://boards.4chan.org", &opt).await?, client, opt).await;
        fchan.run().await
    } else {
        use mysql_async::prelude::*;
        let pool = mysql_async::Pool::new(opt.database.url.as_ref().unwrap());
        let mut conn = pool.get_conn().await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;").await?;
        let rw = RwLock::new(conn);
        let fchan = FourChan::new(create_client("http://boards.4chan.org", &opt).await?, rw, opt).await;
        fchan.run().await?;
        drop(fchan);
        pool.disconnect().await?;
        Ok(())
    }
}

fn get_ctrlc() -> bool {
    CTRLC.load(Ordering::SeqCst) >= 1
}

#[async_trait]
impl<D> Archiver for FourChan<D>
where D: sql::QueryExecutor + Sync + Send
{
    /// Archive 4chan based on --boards | --threads options
    async fn run(&self) -> Result<()> {
        // Init the `boards` table for MySQL
        if self.opt.asagi_mode {
            self.db_client.board_table_exists("boards", &self.opt).await;
        }

        // If postgres, this will init all statements
        // If mysql, this will init only board statements since we need to know the specific board name due
        // to its schema
        self.db_client.init_statements(1, "boards").await;

        self.download_boards_index().await.unwrap();

        // This `board_settings` is already patched to include settings from file + CLI
        // let mut base = self.opt.board_settings.clone();

        // Keep a cache of boards not found in self.opt.boards
        let mut boards_map: HashMap<String, Board> = HashMap::new();
        // let mut mm = &mut boards_map;

        let found = self.opt.boards.iter().find(|&b| b.board == "");
        let find_board = |search_str: &str| self.opt.boards.iter().find(|&b| b.board.as_str() == search_str);

        // Grab seperate boards to dl
        // Grab seperate threads to dl
        // Combine them and run
        // let mut current_board = ""; // First board will always have an entry due to opt parsing rules
        let temp = vec![self.opt.boards.clone()];
        // let mut tv = vec![];
        for s_thread in self.opt.threads.iter() {
            let split: Vec<&str> = s_thread.split('/').filter(|s| !s.is_empty()).collect();
            let b_cli = split[0];
            // let no = split[1].parse::<u64>().unwrap();
            if boards_map.get_mut(b_cli).is_none() {
                let mut base = self.opt.board_settings.clone();
                base.board = b_cli.into();
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
                // if is_board(s) {
                //     current_board = s.as_str();
                // }
                let b_cli = split[0];
                let no = split[1].parse::<u64>().unwrap();
                let search = self.opt.boards.iter().find(|&b| b.board.as_str() == b_cli);

                let res = if let Some(found) = search { Some((found, no)) } else { Some((boards_map.get(b_cli).unwrap(), no)) };
                self.download_board_and_thread(None, res)
            })
            .chain(temp.into_iter().map(|boards| self.download_board_and_thread(Some(boards), None)))
            .collect::<FuturesUnordered<_>>();
        while let Some(res) = fut.next().await {
            res.unwrap();
        }

        Ok(())
    }

    async fn download_boards_index(&self) -> Result<()> {
        let last_modified = self.db_client.boards_index_get_last_modified().await;
        // FIXME: retry variable
        for retry in 0..=3u8 {
            let resp = self.client.gett(self.opt.api_url.join("boards.json")?, &last_modified).await;
            match resp {
                Ok((status, lm, body)) => {
                    // Will do nothing on StatusCode::NOT_MODIFIED
                    if status == StatusCode::OK {
                        self.db_client.boards_index_upsert(&serde_json::from_slice::<serde_json::Value>(&body)?, &lm).await.unwrap();
                    }
                    break;
                }
                Err(e) => {
                    epintln!("download_boards_index: "(e));
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn download_board_and_thread(&self, boards: Option<Vec<Board>>, thread_entry: Option<(&Board, u64)>) -> Result<()> {
        if let Some(_boards) = boards {
            if _boards.len() == 0 {
                return Ok(());
            }
            let mut startup = true;
            let hz = Duration::from_millis(250);
            let mut interval = Duration::from_millis(30_000);
            // cycle
            loop {
                let now = Instant::now();
                for _board in _boards.iter() {
                    if startup {
                        let valid_board: bool = self.db_client.board_is_valid(_board.board.as_str()).await;
                        if !valid_board {
                            // FIXME Filter out invalid boards from the vec before iterating
                            return Err(eyre!("Invalid board `{}`", &_board.board));
                        }
                    }

                    // upsert_board
                    self.db_client.board_upsert(_board.board.as_str()).await.unwrap();

                    // get board
                    let board_id = self.db_client.board_get(_board.board.as_str()).await.unwrap();
                    // FIXME Get rid of clones... probably have to do some interior mutability or something
                    let mut _board = _board.clone();
                    _board.id = board_id;

                    // When thread is None it's from list of boards
                    if _board.with_threads && !_board.with_archives {
                        self.download_board(&_board, ThreadType::Threads, startup).await.unwrap();
                    } else if !_board.with_threads && _board.with_archives {
                        self.download_board(&_board, ThreadType::Archive, startup).await.unwrap();
                    } else if _board.with_threads && _board.with_archives {
                        let (threads, archive) = futures::join!(self.download_board(&_board, ThreadType::Threads, startup), self.download_board(&_board, ThreadType::Archive, startup));
                        threads.unwrap();
                        archive.unwrap();
                    }
                    if !_board.watch_boards || get_ctrlc() {
                        return Ok(());
                    }
                    interval = Duration::from_millis(_board.interval_boards.into());
                }
                if startup {
                    startup = false;
                }
                // FIXME: elpased() silent panic
                // Need to know if modified to be able to use ratelimt struct
                // Duration::from_millis(ratelimit.next().unwrap_or(30) * 1000)
                while now.elapsed() < interval {
                    if get_ctrlc() {
                        break;
                    }
                    sleep(hz).await;
                }
                if get_ctrlc() {
                    break;
                }
            }
        } else if let Some((board_info, thread)) = thread_entry {
            if thread == 0 {
                return Ok(());
            }

            // Check if valid
            let valid_board: bool = self.db_client.board_is_valid(board_info.board.as_str()).await;
            if !valid_board {
                return Err(eyre!("Invalid board `{}`", &board_info.board));
            }

            // upsert_board
            self.db_client.board_upsert(board_info.board.as_str()).await.unwrap();

            // get board
            let board_id = self.db_client.board_get(board_info.board.as_str()).await.unwrap();
            // FIXME Get rid of clones... probably have to do some interior mutability or something
            let mut _board = board_info.clone();
            _board.id = board_id;

            if self.opt.asagi_mode {
                // The group of statments for just `Boards` was done in the beginning, so this can be called
                // This will create all the board tables, triggers, etc if the board doesn't exist
                self.db_client.board_table_exists(&_board.board, &self.opt).await;

                // Init the actual statements for this specific board
                self.db_client.init_statements(_board.id, &_board.board).await;
            }

            let hz = Duration::from_millis(250);
            let interval = Duration::from_millis(_board.interval_threads.into());
            loop {
                let now = Instant::now();
                // with_threads or with_archives don't apply here, since going here implies you want the
                // thread/archive
                // FIXME on delted/archived, need to leave the loop
                self.download_thread(&_board, thread, ThreadType::Threads).await.unwrap();
                if !_board.watch_threads || get_ctrlc() {
                    break;
                }
                // FIXME: elpased() silent panic
                while now.elapsed() < interval {
                    if get_ctrlc() {
                        break;
                    }
                    sleep(hz).await;
                }
                if get_ctrlc() {
                    break;
                }
            }
        } else {
            unreachable!()
        }
        Ok(())
    }

    async fn download_board(&self, board_info: &Board, thread_type: ThreadType, startup: bool) -> Result<()> {
        let fun_name = "download_board";
        if startup && self.opt.asagi_mode {
            // The group of statments for just `Boards` was done in the beginning, so this can be called
            // This will create all the board tables, triggers, etc if the board doesn't exist
            self.db_client.board_table_exists(&board_info.board, &self.opt).await;

            // Init the actual statements for this specific board
            self.db_client.init_statements(board_info.id, &board_info.board).await;
        }
        // Board has to take all permits
        if get_ctrlc() {
            return Ok(());
        }
        if !startup {
            sleep(Duration::from_secs(1)).await;
        }
        let last_modified = self.db_client.board_get_last_modified(thread_type, board_info.id).await;
        let url = self.opt.api_url.join(fomat!((&board_info.board)"/").as_str())?.join(&fomat!((thread_type)".json"))?;
        if get_ctrlc() {
            return Ok(());
        }
        for retry in 0..=board_info.retry_attempts {
            if get_ctrlc() {
                break;
            }
            if retry != 0 {
                sleep(Duration::from_secs(1)).await;
            }
            let resp = self.client.gett(url.as_str(), &last_modified).await;
            match resp {
                Ok((status, lm, body)) => {
                    match status {
                        StatusCode::OK => {
                            let body_json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();

                            // "download_board: ({thread_type}) /{board}/{tab}{new_lm} | {prev_lm} | {retry_status}"
                            pintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                            if board_info.board.len() <= 2 { "\t\t" } else {"\t "}
                            (&lm)
                            " | "
                            if let Some(_lm) =  &last_modified { (_lm) } else { "None" }
                            " | "
                            if retry > 0 { " Retry #"(retry)" [RESOLVED]" } else { "" }
                            );

                            let either = if startup {
                                self.db_client.threads_get_combined(thread_type, board_info.id, &body_json).await
                            } else {
                                if thread_type.is_threads() {
                                    self.db_client.threads_get_modified(board_info.id, &body_json).await
                                } else {
                                    self.db_client.threads_get_combined(ThreadType::Archive, board_info.id, &body_json).await
                                }
                            };
                            // TODO mysql or postgres
                            match either {
                                Either::Right(rows) =>
                                    if let Some(mut rows) = rows {
                                        let mut fut = rows.map(|no| self.download_thread(board_info, no, thread_type)).collect::<FuturesUnordered<_>>().await;
                                        while let Some(res) = fut.next().await {
                                            res.unwrap();
                                        }
                                    },
                                Either::Left(rowstream) => match rowstream {
                                    Ok(rows) => {
                                        futures::pin_mut!(rows);

                                        let mut fut = rows
                                            .map(|row| {
                                                let no: i64 = row.unwrap().get(0);
                                                self.download_thread(board_info, no as u64, thread_type)
                                            })
                                            .collect::<FuturesUnordered<_>>()
                                            .await;
                                        // let mut fut = FuturesUnordered::new();
                                        // while let Some(row) = rows.next().await {
                                        //     match row {
                                        //         Ok(row) => {
                                        //             let t: i64 = row.get(0);
                                        //             fut.push(self.download_thread(board_info, t as u64, thread_type));
                                        //         }
                                        //         Err(e) => {
                                        //             epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                                        //             if board_info.board.len() <= 2 { "\t\t" } else {"\t"}
                                        //             (&lm)
                                        //             " | ["
                                        //             (e)"]"
                                        //             );
                                        //         }
                                        //     }
                                        // }
                                        while let Some(res) = fut.next().await {
                                            res.unwrap();
                                        }
                                    }
                                    Err(e) => {
                                        epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                                        if board_info.board.len() <= 2 { "\t\t" } else {"\t"}
                                        (&lm)
                                        " | ["
                                        (e)"]"
                                        );
                                    }
                                },
                            }

                            if get_ctrlc() {
                                break;
                            }

                            // Update threads/archive cache at the end
                            if thread_type.is_threads() {
                                self.db_client.board_upsert_threads(board_info.id, &board_info.board, &body_json, &lm).await.unwrap();
                            } else if thread_type.is_archive() {
                                self.db_client.board_upsert_archive(board_info.id, &board_info.board, &body_json, &lm).await.unwrap();
                            }
                            break; // exit the retry loop
                        }
                        StatusCode::NOT_FOUND => {
                            epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                            if board_info.board.len() <= 2 { "\t\t" } else {"\t"}
                            (&lm)
                            " | ["
                            (status)"]"
                            );
                            break;
                        }
                        StatusCode::NOT_MODIFIED => {
                            epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                            if board_info.board.len() <= 2 { "\t\t" } else {"\t"}
                            (&lm)
                            " | ["
                            (status)"]"
                            );
                            break;
                        }
                        _ => {
                            epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                            if board_info.board.len() <= 2 { "\t\t" } else {"\t"}
                            (&lm)
                            " | ["
                            (status)"]"
                            );
                        }
                    }
                }
                Err(e) => {
                    epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                    if board_info.board.len() <= 2 { "\t\t" } else {"\t "}
                    if let Some(_lm) =  &last_modified { (_lm) } else { "None" }
                    " | ["
                    (e)"]"
                    );
                }
            }
        }
        Ok(())
    }

    async fn download_thread(&self, board_info: &Board, thread: u64, thread_type: ThreadType) -> Result<ThreadType> {
        let sem = SEMAPHORE.acquire(1).await;
        let mut with_tail = board_info.with_tail;
        let hz = Duration::from_millis(250);
        let mut interval = Duration::from_millis(board_info.interval_threads.into());
        let mut thread_type = thread_type;

        // One giant loop so we can re-execute this function (if we find out tail-json is nonexistent)
        // without resorting to recursion which generates unintended behaviours with the semaphore.
        // We break at the end so it's OK.
        'outer: loop {
            if get_ctrlc() {
                return Ok(thread_type);
            }
            // TODO: OR media not complete (missing media)
            let last_modified = self.db_client.thread_get_last_modified(board_info.id, thread).await;
            let url =
                self.opt.api_url.join(fomat!((&board_info.board)"/").as_str())?.join("thread/")?.join(&format!("{thread}{tail}.json", thread = thread, tail = if with_tail { "-tail" } else { "" }))?;

            for retry in 0..=board_info.retry_attempts {
                let now = Instant::now();
                let resp = self.client.gett(url.as_str(), &last_modified).await;
                if retry != 0 {
                    sleep(Duration::from_secs(1)).await;
                }
                match resp {
                    Ok((status, lm, body)) => {
                        if body.len() > 0 {
                            let j = serde_json::from_slice::<serde_json::Value>(body.as_slice());
                            if let Ok(thread_json) = j {
                                let op = thread_json.get("posts").and_then(|v| v.get(0));
                                let archived = op.and_then(|v| v.get("archived")).and_then(|v| v.as_u64()).map_or_else(|| false, |v| v == 1);
                                let archived_on = op.and_then(|v| v.get("archived")).and_then(|v| v.as_u64());
                                if archived || archived_on.is_some() {
                                    thread_type = ThreadType::Archive;
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
                                    Ok(mut thread_json) => {
                                        if !with_tail {
                                            update_post_with_extra(&mut thread_json);
                                        }
                                        // let posts = thread_json.get("posts").unwrap().as_array().unwrap();
                                        let op_post = thread_json.get("posts").unwrap().as_array().unwrap().iter().nth(0).unwrap();

                                        let no: u64 = op_post["no"].as_u64().unwrap();
                                        let resto: u64 = op_post["resto"].as_u64().unwrap_or(0);

                                        if with_tail {
                                            // Check if we can use the tail.json
                                            // Check if we have the tail_id in the db
                                            let tail_id = op_post["tail_id"].as_u64().unwrap();
                                            let query = (&self)
                                            .db_client.post_get_single(board_info.id, thread, tail_id)
                                            //.query_one("SELECT * from posts where board=$1 and resto=$2 and no=$3 LIMIT 1;", &[&(board_info.id as i16), &(thread as i64), &(tail_id as i64)])
                                            .await;

                                            // If no Row returned, download thread normally
                                            if !query {
                                                with_tail = false;
                                                continue 'outer;
                                            }

                                            // Don't pop OP post (which has no time). It will be filtered upon inside `thread_upsert`.
                                            update_post_with_extra(&mut thread_json);
                                        }

                                        if get_ctrlc() {
                                            return Ok(thread_type);
                                        }

                                        // TODO: Download media only if we don't already have it

                                        // Upsert Thread
                                        let len = self.db_client.thread_upsert(board_info, &thread_json).await;

                                        // Display
                                        // "download_thread: ({thread_type}) /{board}/{thread}{tail}{retry_status} {new_lm} | {prev_lm} |
                                        // {len}"
                                        pintln!("download_thread: (" (thread_type) ") /" (&board_info.board) "/" (thread)
                                            if with_tail { "-tail" } else { "" }
                                            if retry > 0 { " Retry #"(retry)" [RESOLVED]" } else { "" }
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
                                        );

                                        // Update thread's deleteds
                                        let either = self.db_client.thread_update_deleteds(board_info, thread, &thread_json).await;
                                        match either {
                                            Either::Right(rows) => {
                                                if let Some(mut rows) = rows {
                                                    // TODO get row, not u64
                                                    while let Some(no) = rows.next().await {
                                                        pintln!("download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)"#"(no)"\t[DELETED]");
                                                    }
                                                }
                                            }
                                            Either::Left(rows) =>
                                                if let Ok(rows) = rows {
                                                    futures::pin_mut!(rows);
                                                    while let Some(Ok(row)) = rows.next().await {
                                                        let no: Option<i64> = row.get("no");
                                                        let resto: Option<i64> = row.get("resto");
                                                        if let Some(no) = no {
                                                            if let Some(resto) = resto {
                                                                pintln!("download_thread: ("(thread_type)") /"(&board_info.board)"/"
                                                                if resto == 0 { (no) } else { (resto) }
                                                                "#"(no)"\t[DELETED]");
                                                            }
                                                        }
                                                    }
                                                },
                                        }

                                        // Get Media
                                        if board_info.with_full_media || board_info.with_thumbnails {
                                            if self.opt.asagi_mode {
                                                // Get list of media. Filter out `filedeleted`. Filter out non-media.
                                                let media_list: Vec<(u64, u64, &str, u64, &str, &str)> = (&thread_json["posts"])
                                                    .as_array()
                                                    .unwrap()
                                                    .iter()
                                                    .filter(|&post_json| {
                                                        !post_json.get("filedeleted").map(|j| j.as_u64()).flatten().map_or(false, |filedeleted| filedeleted == 1) && post_json.get("md5").is_some()
                                                    })
                                                    .map(|v| {
                                                        (
                                                            v.get("resto").unwrap().as_u64().unwrap_or_default(),
                                                            v.get("no").unwrap().as_u64().unwrap_or_default(),
                                                            v.get("md5").unwrap().as_str().unwrap_or_default(),
                                                            v.get("tim").unwrap().as_u64().unwrap_or_default(),
                                                            v.get("ext").unwrap().as_str().unwrap_or_default(),
                                                            v.get("filename").unwrap().as_str().unwrap_or_default(),
                                                        )
                                                    })
                                                    .collect();

                                                let media_list_len = media_list.len();

                                                // Asagi
                                                // This query is much better than a JOIN
                                                // let sq = fomat!(
                                                //     "SELECT * FROM " (&board_info.board) "_images WHERE media_hash IN ("
                                                //     for (i, (md5, filename)) in media_list.iter().enumerate() {
                                                //         "\n"(format_sql_query::QuotedData(&md5.replace("\\", "")))
                                                //         if i < media_list_len-1 { "," } else { "" }
                                                //     }
                                                //     ");"
                                                // );

                                                if media_list.len() > 0 {
                                                    if board_info.with_thumbnails && board_info.board != "f" {
                                                        let mut fut = media_list
                                                            .iter()
                                                            .map(|&details| {
                                                                let (resto, no, md5, tim, ext, filename) = details;
                                                                self.download_media(board_info, (resto, no, Some(md5.into()), None, tim, ext.into(), filename.into(), None, None), MediaType::Thumbnail)
                                                            })
                                                            .collect::<FuturesUnordered<_>>();
                                                        while let Some(res) = fut.next().await {
                                                            res.unwrap();
                                                        }
                                                    }
                                                    if get_ctrlc() {
                                                        break;
                                                    }
                                                    if board_info.with_full_media {
                                                        let mut fut = media_list
                                                            .iter()
                                                            .map(|&details| {
                                                                let (resto, no, md5, tim, ext, filename) = details;
                                                                self.download_media(board_info, (resto, no, Some(md5.into()), None, tim, ext.into(), filename.into(), None, None), MediaType::Full)
                                                            })
                                                            .collect::<FuturesUnordered<_>>();
                                                        while let Some(res) = fut.next().await {
                                                            res.unwrap();
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Postgres Side
                                                let start = if let Some(post_json) = thread_json["posts"].get(1) {
                                                    post_json["no"].as_u64().unwrap_or_default()
                                                } else {
                                                    thread_json["posts"][0]["no"].as_u64().unwrap_or_default()
                                                };
                                                loop {
                                                    let either = self.db_client.thread_get_media(board_info, thread, start).await;
                                                    if let Either::Left(res) = either {
                                                        match res {
                                                            Err(e) => epintln!("download_media: "(e)),
                                                            Ok(rows) => {
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
                                                                    if board_info.with_thumbnails && board_info.board != "f" {
                                                                        let mut fut_thumbs = mm
                                                                            .clone()
                                                                            .into_iter()
                                                                            .map(| v|
                                                            //  v.map(| details| {
                                                            //     self.download_media(board_info, details,  true)
                                                            //  })
                                                            self.download_media(board_info, v,  MediaType::Thumbnail))
                                                                            .collect::<FuturesUnordered<_>>();
                                                                        while let Some(res) = fut_thumbs.next().await {
                                                                            res.unwrap()
                                                                        }
                                                                    }
                                                                }

                                                                if get_ctrlc() {
                                                                    return Ok(thread_type);
                                                                }

                                                                // Downlaod full media
                                                                if board_info.with_full_media {
                                                                    let mut fut = mm
                                                                        .into_iter()
                                                                        .map(| v|
                                                            // v.map(| details| {
                                                            //    self.download_media(board_info, details,  true)
                                                            // })
                                                           self.download_media(board_info, v,  MediaType::Full))
                                                                        .collect::<FuturesUnordered<_>>();
                                                                    while let Some(res) = fut.next().await {
                                                                        res.unwrap()
                                                                    }
                                                                }
                                                                break; // exit the db loop
                                                            }
                                                        }
                                                    }
                                                    sleep(Duration::from_millis(500)).await;
                                                }
                                            }
                                        }

                                        // Update thread's last_modified
                                        self.db_client.thread_update_last_modified(&lm, board_info.id, thread).await.unwrap();
                                        break; // exit the retry loop
                                    }
                                    Err(e) => {
                                        epintln!("download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)
                                        if with_tail { "-tail " } else { " " }
                                        "["(e)"] " [&last_modified]);
                                    }
                                }
                            }
                            StatusCode::NOT_FOUND => {
                                if with_tail {
                                    with_tail = false;
                                    continue 'outer;
                                }
                                let either = self.db_client.thread_update_deleted(board_info.id, thread).await;
                                let tail = if with_tail { "-tail" } else { "" };
                                match either {
                                    Either::Right(rows) =>
                                        if let Some(mut rows) = rows {
                                            while let Some(no) = rows.next().await {
                                                pintln!(
                                                    "download_thread: ("(thread_type)") /"(&board_info.board)"/"(no)(tail)"\t["(status)"] [DELETED]"
                                                );
                                            }
                                        },

                                    Either::Left(rows) => match rows {
                                        Ok(rows) => {
                                            futures::pin_mut!(rows);
                                            while let Some(row) = rows.next().await {
                                                match row {
                                                    Ok(row) => {
                                                        let no: Option<i64> = row.get("no");
                                                        if let Some(no) = no {
                                                            pintln!(
                                                                "download_thread: ("(thread_type)") /"(&board_info.board)"/"(no)(tail)"\t["(status)"] [DELETED]"
                                                            );
                                                        } else {
                                                            epintln!(
                                                                "download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)(tail)"\t["(status)"] [`no` is empty]"
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        epintln!(
                                                            "download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)(tail)"\t["(status)"] [thread_update_deleted]"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            epintln!(
                                                "download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)(tail)"\t["(status)"] [thread_update_deleted]" "["(e)"]"
                                            );
                                        }
                                    },
                                }

                                break;
                            }
                            StatusCode::NOT_MODIFIED => {
                                // Don't output
                                break;
                            }
                            _ => {
                                epintln!(
                                    "download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)
                                    if with_tail { "-tail" } else { "" }
                                    "\t["(status)"]"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        epintln!(
                            "download_thread: ("(thread_type)") /"(&board_info.board)"/"(thread)
                            if with_tail { "-tail" } else { "" }
                            "\t["(e)"]"
                        );
                    }
                }
            }
            break;
        }
        Ok(thread_type)
    }

    async fn download_media(&self, board_info: &Board, details: MediaDetails, media_type: MediaType) -> Result<()> {
        if get_ctrlc() {
            return Ok(());
        }

        // This is from 4chan post / postgres post
        let (resto, no, md5_base64, md5, tim, ext, filename, sha256, sha256t) = details;

        let mut dir = None;
        let mut path = None;
        let mut url = None;

        // Check if exists
        if self.opt.asagi_mode {
            if let Either::Right(Some(row)) = self.db_client.post_get_media(board_info, md5_base64.as_ref().unwrap(), None).await {
                let banned = row.get::<Option<u8>, &str>("banned").flatten().map_or_else(|| false, |v| v == 1);
                if banned {
                    pintln!("download_media: Skipping banned media: /" (&board_info.board)"/"
                    if resto == 0 { (no) } else { (resto) }
                    "#" (no)
                    )
                }

                let media = row.get::<Option<String>, &str>("media").flatten().unwrap();
                let preview_op = row.get::<Option<String>, &str>("preview_op").flatten();
                let preview_reply = row.get::<Option<String>, &str>("preview_reply").flatten();
                let preview = preview_op.map_or_else(|| preview_reply, |v| Some(v)).unwrap();

                let tim_filename = if media_type == MediaType::Full { media } else { preview };

                // Directory Structure:
                // 1540970147550
                // {media_dir}/{board}/{image|thumb}/1540/97
                let _dir = fomat!(
                    (self.opt.media_dir.display()) "/" (&board_info.board) "/"
                    if media_type == MediaType::Full { "image" } else { "thumb" } "/"
                    (tim_filename[..4]) "/" (tim_filename[4..6])
                );
                let _path = fomat!(
                    (_dir) "/" (&tim_filename)
                );

                let actual_filename = if board_info.board == "f" { fomat!((percent_encoding::utf8_percent_encode(&filename, percent_encoding::NON_ALPHANUMERIC))(ext)) } else { tim_filename };
                if exists(&_path) {
                    return Ok(());
                }
                path = Some(_path);
                dir = Some(_dir);
                let _url = format!("{api_domain}/{board}/{filename}", api_domain = &self.opt.media_url, board = &board_info.board, filename = &actual_filename);
                url = Some(_url);
            } else {
                epintln!("download_media: Error getting media! This shouldn't have happened!");
            }
        } else {
            let _checksum = if media_type == MediaType::Full {
                sha256
            } else {
                // Thumbnails aren't unique and can have duplicates
                // So check the database if we already have it or not
                if let Either::Left(s) = self.db_client.post_get_media(board_info, "", sha256t.as_ref().map(|v| v.as_slice())).await {
                    s.ok().flatten().map(|row| row.get::<&str, Option<Vec<u8>>>("sha256t")).flatten()
                } else {
                    // This else case will probably never run
                    // Since the Either will always run
                    sha256t
                }
            };

            // Directory Structure:
            // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
            // {media_dir}/{full|thumbnails}/1/0a
            if let Some(checksum) = &_checksum {
                let hash = format!("{:02x}", HexSlice(checksum.as_slice()));
                let len = hash.len();
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
                        // can have duplicate thumbnails, if this particular md5 doesn't have it
                        // then upsert the hash, since we have it on the filesystem.
                        if media_type == MediaType::Thumbnail {
                            for retry in 0..=3u8 {
                                let res = self.db_client.post_upsert_media(md5.as_ref().unwrap().as_slice(), None, _checksum.as_ref().map(|v| v.as_slice())).await;
                                match res {
                                    Err(e) => {
                                        epintln!("download_media: post_upsert_media (thumb): [" (e) "]");
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
                        epintln!(
                        "download_media: Exists in db but not in filesystem!: `" (&_path) "`"
                        );
                    }
                } else {
                    epintln!("download_media: Error! Hash found to be " (len) " chars long when it's supposed to be 64" );
                }
                // path = Some(_path);
            }

            let tim_filename = fomat!((tim) if media_type == MediaType::Full { (ext) } else { "s.jpg" } );

            let actual_filename = {
                if board_info.board == "f" {
                    fomat!((percent_encoding::utf8_percent_encode(&filename, percent_encoding::NON_ALPHANUMERIC))(ext))
                } else {
                    tim_filename
                }
            };

            let _url = format!("{api_domain}/{board}/{filename}", api_domain = &self.opt.media_url, board = &board_info.board, filename = &actual_filename);
            url = Some(_url);
        }

        // Download the file
        if let Some(_url) = url {
            if get_ctrlc() {
                return Ok(());
            }
            let sem = MEDIA_SEMAPHORE.acquire(1).await;
            if get_ctrlc() {
                return Ok(());
            }
            for retry in 0..=board_info.retry_attempts {
                if retry != 0 {
                    epintln!("download_media: /"(board_info.board)"/"(resto)"/"(no) " [Retry #" (retry)"]");
                    sleep(Duration::from_millis(500)).await;
                }
                match self.client.get(_url.as_str()).send().await {
                    Err(e) => epintln!("download_media: " "/"(&board_info.board)"/"
                        if resto == 0 { (no) } else { (resto) }
                        "#"
                        (no)
                        "["(e)"]"),
                    Ok(resp) => {
                        match resp.status() {
                            StatusCode::NOT_FOUND => {
                                break;
                            }
                            StatusCode::OK => {
                                // pintln!("download_media: /" (&board_info.board)"/"
                                //     if resto == 0 { (no) } else { (resto) }
                                //     "#" (no)
                                //     );
                                // Going here means that we don't have the file
                                if get_ctrlc() {
                                    break;
                                }
                                if self.opt.asagi_mode {
                                    // Make dirs
                                    if let Some(_dir) = &dir {
                                        let res = smol::Unblock::new(create_dir_all(_dir.as_str())).into_inner().await;
                                        if let Err(e) = res {
                                            epintln!("download_media: [" (e) "]");
                                        }
                                    }
                                    if let Some(file_path) = &path {
                                        let mut file = smol::Unblock::new(File::create(&file_path).unwrap());
                                        let mut writer = io::BufWriter::new(&mut file);
                                        let mut stream = resp.bytes_stream();
                                        let mut hasher = Md5::new();
                                        while let Some(item) = stream.next().await {
                                            if get_ctrlc() {
                                                writer.flush().await.unwrap();
                                                writer.close().await.unwrap();
                                                let _ = std::fs::remove_file(&file_path);
                                                return Ok(());
                                            }
                                            let _item = &item.unwrap();
                                            if media_type == MediaType::Full {
                                                hasher.update(_item);
                                            }
                                            writer.write_all(_item).await.unwrap();
                                        }
                                        writer.flush().await.unwrap();
                                        writer.close().await.unwrap();
                                        // Only check md5 for full media
                                        if media_type == MediaType::Full {
                                            let result = hasher.finalize();
                                            let md5_bytes = base64::decode(md5_base64.as_ref().unwrap().as_bytes()).unwrap();
                                            if md5_bytes.as_slice() != result.as_slice() {
                                                epintln!("download_media: Hashes don't match! /" (&board_info.board) "/"
                                                if resto == 0 { (no) } else { (resto) }
                                                "#" (no)
                                                if retry > 0 { " [Retry #" (retry) "]" } else { "" }
                                                );
                                                continue;
                                            }
                                        }
                                        break;
                                    } else {
                                        epintln!("download_media: file path is empty! This isn't supposed to happen!");
                                    }
                                } else {
                                    let now = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).map(|v| v.as_nanos()).unwrap_or(tim.into());

                                    // Unique tmp filename so it won't clash when saving
                                    let tmp_path = fomat!((&self.opt.media_dir.display()) "/tmp/"
                                    (&board_info.board)"-"
                                    (resto) "-"
                                    (no) "-"
                                    {"{:02x}", HexSlice(md5.as_ref().unwrap().as_slice()) }  "-"
                                    (now)
                                    if media_type == MediaType::Full { (ext) } else { "s.jpg" }
                                    );

                                    // Download File and calculate hashes
                                    let mut file = smol::Unblock::new(File::create(&tmp_path).unwrap());
                                    let mut writer = io::BufWriter::new(&mut file);
                                    let mut stream = resp.bytes_stream();
                                    let mut hasher = Md5::new();
                                    let mut hasher_sha256 = sha2::Sha256::new();
                                    while let Some(item) = stream.next().await {
                                        if get_ctrlc() {
                                            writer.flush().await.unwrap();
                                            writer.close().await.unwrap();
                                            let _ = std::fs::remove_file(&tmp_path);
                                            return Ok(());
                                        }
                                        let _item = &item.unwrap();
                                        if media_type == MediaType::Full {
                                            hasher.update(_item);
                                        }
                                        hasher_sha256.update(_item);
                                        writer.write_all(_item).await.unwrap();
                                    }
                                    writer.flush().await.unwrap();
                                    writer.close().await.unwrap();
                                    let result_sha256 = hasher_sha256.finalize();
                                    let hash = format!("{:02x}", result_sha256);
                                    let len = hash.len();

                                    // Only check md5 for full media
                                    if media_type == MediaType::Full {
                                        let result = hasher.finalize();
                                        if md5.as_ref().unwrap().as_slice() != result.as_slice() {
                                            epintln!("download_media: Hashes don't match! /" (&board_info.board) "/"
                                            if resto == 0 { (no) } else { (resto) }
                                            "#" (no)
                                            {" `{:02x}`",HexSlice(md5.as_ref().unwrap().as_slice()) } {" != `{:02x}`", &result}
                                            if retry > 0 { " [Retry #" (retry) "]" } else { "" }
                                            );
                                            let _ = std::fs::remove_file(&tmp_path);
                                            continue;
                                        }
                                    }

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
                                            epintln!("download_media: `" 
                                            (&_path)
                                            "` exists! Downloaded for nothing.. Perhaps you didn't upload your hashes to the database beforehand?");

                                            // Try to upsert to database
                                            let mut success = true;
                                            for _ in 0..=3u8 {
                                                let res = self
                                                    .db_client
                                                    .post_upsert_media(
                                                        md5.as_ref().unwrap().as_slice(),
                                                        if media_type == MediaType::Full { Some(result_sha256.as_slice()) } else { None },
                                                        if media_type == MediaType::Thumbnail { Some(result_sha256.as_slice()) } else { None },
                                                    )
                                                    .await;
                                                if let Err(e) = res {
                                                    epintln!(
                                                        "download_media: post_upsert_media: " "/"(&board_info.board)"/"
                                                        if resto == 0 { (no) } else { (resto) }
                                                        "#"
                                                        (no)
                                                        " ["(e)"]");
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
                                            let _ = std::fs::remove_file(&tmp_path);
                                            if success {
                                                break;
                                            }
                                            // return Ok(());
                                        }
                                        // Make dirs
                                        let dirs_result = smol::Unblock::new(create_dir_all(_dir.as_str())).into_inner().await;
                                        match dirs_result {
                                            Err(e) => epintln!("download_media: Error creating dirs `" (&_dir) "` [" (e) "]"),
                                            Ok(_) => {
                                                // Move to final path
                                                let rename_result = smol::Unblock::new(std::fs::rename(&tmp_path, &_path)).into_inner().await;
                                                match rename_result {
                                                    Err(e) => epintln!("download_media: Error moving `" (&tmp_path) "` to `" (&_path) "` [" (e) "]"),
                                                    Ok(_) => {
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
                                                                epintln!("download_media: " "/"(&board_info.board)"/"
                                                                            if resto == 0 { (no) } else { (resto) }
                                                                            "#"
                                                                            (no)
                                                                            "["(e)"]");
                                                                success = false;
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
                                        epintln!("download_media: Error! Hash found to be " (len) " chars long when it's supposed to be 64" );
                                    }
                                }
                            }
                            status => epintln!("download_media: "(status)),
                        }
                    }
                }
            }
        } else {
            epintln!("download_media: No URL was found! This shouldn't happen!")
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ThreadType {
    Threads,
    Archive,
}

impl ThreadType {
    fn is_threads(&self) -> bool {
        matches!(self, ThreadType::Threads)
    }

    fn is_archive(&self) -> bool {
        matches!(self, ThreadType::Archive)
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

#[derive(PartialEq)]
pub enum MediaType {
    Full,
    Thumbnail,
}

impl MediaType {
    fn ext<'a>(&self) -> &'a str {
        match self {
            MediaType::Full => "",
            MediaType::Thumbnail => "s.jpg",
        }
    }
}

pub type MediaDetails = (u64, u64, Option<String>, Option<Vec<u8>>, u64, String, String, Option<Vec<u8>>, Option<Vec<u8>>);
// pub type MediaDetailsAsagi<'a> = (&'a str, u64, &'a str, &'a str);

// From `hex-slice` crate
pub struct HexSlice<'a, T: 'a>(&'a [T]);
use core::fmt;
// use core::fmt::Write;
impl<'a, T: fmt::LowerHex> fmt::LowerHex for HexSlice<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val = self.0;
        for b in val {
            fmt::LowerHex::fmt(b, f)?
        }
        Ok(())
        // fmt_inner_hex(self.0, f, fmt::LowerHex::fmt)
    }
}

fn fmt_inner_hex<T, F: Fn(&T, &mut fmt::Formatter) -> fmt::Result>(slice: &[T], f: &mut fmt::Formatter, fmt_fn: F) -> fmt::Result {
    for val in slice.iter() {
        fmt_fn(val, f)?;
    }
    Ok(())
}
/*
async fn async_main(mut receiver: oneshot::Receiver<()>, config: Opt) -> Result<()> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Connection", "keep-alive".parse().unwrap());
    headers.insert("Origin", "http://boards.4chan.org".parse().unwrap());
    let ua = &config.user_agent;
    let client = reqwest::Client::builder().default_headers(headers).user_agent(ua).use_rustls_tls().build()?;

    // dl(&client, "https://i.4cdn.org/a/1593207178861s.jpg".into()).await.unwrap();
    // let posts_json = get_json("https://a.4cdn.org/a/thread/204968711.json").await.unwrap();
    // return Ok(());

    let mut fut = FuturesUnordered::new();
    let threads_json = get_json(&client, &fomat!((&config.api_url) "/a/threads.json")).await.unwrap();
    let pages = threads_json.as_array().unwrap();
    let media_threads = config.media_threads.max(5); // Choose the max between user and 5
    pintln!("Media async threads: "(media_threads));
    let mut p: u8 = 0;
    for page in pages {
        p += 1;
        pintln!("Page #"(p));
        let threads = page.get("threads").unwrap().as_array().unwrap();
        let mut t: u8 = 0;
        for thread in threads {
            t += 1;
            pintln!("Thread #"(t));
            fut.push(process_thread(thread.to_owned(), &client, &config));
            if matches!(&receiver.try_recv(), Ok(Some(()))) {
                return Ok(());
            }
            // break;
        }
        // Finish off any images missing in the thread
        while let Some(_) = fut.next().await {
            if matches!(&receiver.try_recv(), Ok(Some(()))) {
                return Ok(());
            }
        }

        if matches!(&receiver.try_recv(), Ok(Some(()))) {
            return Ok(());
        }
        // break;
    }

    Ok(())
}
*/

fn main() -> Result<()> {
    if std::env::var("RUST_BACKTRACE").is_err() {
        std::env::set_var("RUST_BACKTRACE", "full");
    }
    color_eyre::install()?;
    let num_threads = num_cpus::get().max(1);

    // Run the thread-local and work-stealing executor on a thread pool.
    for _ in 0..num_threads {
        // A pending future is one that simply yields forever.
        thread::spawn(|| smol::run(future::pending::<()>()));
    }

    let (sender, receiver) = oneshot::channel::<()>();
    smol::block_on(async {
        pintln!("Press CTRL+C to exit");
        ctrlc::set_handler(move || {
            CTRLC.fetch_add(1, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
        test_main().await.unwrap();
        pintln!("Done!");
        Ok(())
    })
}
