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

use color_eyre::eyre::{eyre, Result};
use fomat_macros::{epintln, fomat, pintln};
use futures::future;

use async_trait::async_trait;
use ctrlc;
use futures::{
    channel::oneshot,
    future::Either,
    stream::{FuturesUnordered, StreamExt},
};
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
    fs::File,
    io::Read,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering},
    thread,
    time::{Duration, Instant},
};
use structopt::StructOpt;

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
    async fn download_board(&self, board_info: &Board, thread_type: &str, startup: bool) -> Result<()>;
    async fn download_thread(&self, board_info: &Board, thread: u64, thread_type: &str) -> Result<()>;
    async fn download_media(&self, board_info: &Board, post: &yotsuba::Post) -> Result<()>;
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
                        eprintln!("{:x?}", &md5_bytes[..]);
                        eprintln!("{:x?}", &result[..]);
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
static SEMAPHORE_AMOUNT: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(0));
static MEDIA_SEMAPHORE_AMOUNT: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(0));

static SEMAPHORE: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| 
    futures_intrusive::sync::Semaphore::new(true, SEMAPHORE_AMOUNT.load(Ordering::SeqCst) as usize ));
static MEDIA_SEMAPHORE: Lazy<futures_intrusive::sync::Semaphore> = Lazy::new(|| 
futures_intrusive::sync::Semaphore::new(true, MEDIA_SEMAPHORE_AMOUNT.load(Ordering::SeqCst) as usize ));

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

#[rustfmt::skip]
fn get_opt() -> Result<Opt> {
    let mut opt = Opt::from_args();
    let default = Board::default(); // This has to call Self::default(), not &default_opt.board_settings (to prevent incorrect values)
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
    let new_boards: Vec<Board> = opt.boards.iter().filter(|b| !opt.boards_excluded.iter().any(|be| b.board == be.board) ).map(|b|b.clone())
                        .collect();
    opt.boards = new_boards;
    // https://stackoverflow.com/a/55150936
    let mut opt = {
        if let Some(config_file) = &opt.config.to_str() {
            if config_file.is_empty() {
                opt
            } else {
                let content = if config_file == &"-" {
                    let mut content = String::new();
                    std::io::stdin().lock().read_to_string(&mut content)?;
                    // TODO: Handle above error
                    content
                } else {
                    std::fs::read_to_string(config_file)?
                    // TODO: Handle above error
                };
                let mut o = serde_yaml::from_str::<Opt>(&content);
                match o {
                    Ok(ref mut q) => {
                        let default_opt = Opt::default();
                        let default = Board::default(); // This has to call Self::default(), not &default_opt.board_settings (to prevent incorrect values)
                        let default_database = DatabaseOpt::default();
                        if opt.board_settings.retry_attempts    != default.retry_attempts   { q.board_settings.retry_attempts = opt.board_settings.retry_attempts; }
                        if opt.board_settings.interval_boards   != default.interval_boards  { q.board_settings.interval_boards = opt.board_settings.interval_boards; }
                        if opt.board_settings.interval_threads  != default.interval_threads { q.board_settings.interval_threads = opt.board_settings.interval_threads; }
                        if opt.board_settings.with_threads      != default.with_threads     { q.board_settings.with_threads = opt.board_settings.with_threads; }
                        if opt.board_settings.with_archives     != default.with_archives    { q.board_settings.with_archives = opt.board_settings.with_archives; }
                        if opt.board_settings.with_tail         != default.with_tail        { q.board_settings.with_tail = opt.board_settings.with_tail; }
                        if opt.board_settings.with_full_media   != default.with_full_media  { q.board_settings.with_full_media = opt.board_settings.with_full_media; }
                        if opt.board_settings.with_thumbnails   != default.with_thumbnails  { q.board_settings.with_thumbnails = opt.board_settings.with_thumbnails; }
                        if opt.board_settings.watch_boards      != default.watch_boards     { q.board_settings.watch_boards = opt.board_settings.watch_boards; }
                        if opt.board_settings.watch_threads     != default.watch_threads    { q.board_settings.watch_threads = opt.board_settings.watch_threads; }
                        
                        
                        let boards_excluded_combined: Vec<Board>  = q.boards_excluded.iter().chain(opt.boards_excluded.iter()).map(|b| b.clone()).collect();
                        let threads_combined: Vec<String> = q.threads.iter().chain(opt.threads.iter()).map(|s| s.clone()).collect();
                        let boards_combined: Vec<Board> = q.boards.iter().chain(opt.boards.iter()).map(|b| b.clone())
                        .filter(|b| !boards_excluded_combined.iter().any(|be| b.board == be.board) )
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
                            if b.with_archives      == default.with_archives    { b.with_archives =  q.board_settings.with_archives; }
                            if b.with_tail          == default.with_tail        { b.with_tail = q.board_settings.with_tail; }
                            if b.with_full_media    == default.with_full_media  { b.with_full_media = q.board_settings.with_full_media; }
                            if b.with_thumbnails    == default.with_thumbnails  { b.with_thumbnails = q.board_settings.with_thumbnails; }
                            if b.watch_boards       == default.watch_boards     { b.watch_boards = q.board_settings.watch_boards; }
                            if b.watch_threads      == default.watch_threads    { b.watch_threads = q.board_settings.watch_threads; }
                        }

                        // Finally patch the yaml's board_settings with CLI opts
                        if q.strict                 == default_opt.strict               { q.strict = opt.strict;                                    }
                        if q.asagi_mode             == default_opt.asagi_mode           { q.asagi_mode = opt.asagi_mode;                            }
                        if q.quickstart             == default_opt.quickstart           { q.quickstart = opt.quickstart;                            }
                        if q.start_with_archives    == default_opt.start_with_archives  { q.start_with_archives = opt.start_with_archives;          }
                        if q.config                 == default_opt.config               { q.config = opt.config;                                    }
                        if q.site                   == default_opt.site                 { q.site =  opt.site;                                       }
                        if q.limit                  == default_opt.limit                { q.limit = opt.limit;                                      }
                        if q.media_dir              == default_opt.media_dir            { q.media_dir = opt.media_dir;                              }
                        if q.media_storage          == default_opt.media_storage        { q.media_storage = opt.media_storage;                      }
                        if q.media_threads          == default_opt.media_threads        { q.media_threads = opt.media_threads;                      }
                        if q.user_agent             == default_opt.user_agent           { q.user_agent = opt.user_agent;                            }
                        if q.api_url                == default_opt.api_url              { q.api_url = opt.api_url;                                  }
                        if q.media_url              == default_opt.media_url            { q.media_url = opt.media_url;                              }
                        
                        // Database
                        // TODO use database url?
                        if q.database.url           == default_database.url             { q.database.url        = opt.database.url.clone();         }
                        if q.database.engine        == default_database.engine          { q.database.engine     = opt.database.engine.clone();      }
                        if q.database.name          == default_database.name            { q.database.name       = opt.database.name.clone();        }
                        if q.database.schema        == default_database.schema          { q.database.schema     = opt.database.schema.clone();      }
                        if q.database.port          == default_database.port            { q.database.port       = opt.database.port;                }
                        if q.database.username      == default_database.username        { q.database.username   = opt.database.username.clone();    }
                        if q.database.password      == default_database.password        { q.database.password   = opt.database.password.clone();    }
                        if q.database.charset       == default_database.charset         { q.database.charset    = opt.database.charset.clone();     }
                        if q.database.collate       == default_database.collate         { q.database.collate    = opt.database.collate;             }
                        // Or you can do it this way: 
                        // if opt.database.url           != default_database.url             { q.database.url        = opt.database.url.clone();         }
                        // if opt.database.engine        != default_database.engine          { q.database.engine     = opt.database.engine.clone();      }
                        // if opt.database.name          != default_database.name            { q.database.name       = opt.database.name.clone();        }
                        // if opt.database.schema        != default_database.schema          { q.database.schema     = opt.database.schema.clone();      }
                        // if opt.database.port          != default_database.port            { q.database.port       = opt.database.port;                }
                        // if opt.database.username      != default_database.username        { q.database.username   = opt.database.username.clone();    }
                        // if opt.database.password      != default_database.password        { q.database.password   = opt.database.password.clone();    }
                        // if opt.database.charset       != default_database.charset         { q.database.charset    = opt.database.charset.clone();     }
                        // if opt.database.collate       != default_database.collate         { q.database.collate    = opt.database.collate;             }
                        

                        if q.board_settings.retry_attempts      == default.retry_attempts   { q.board_settings.retry_attempts = opt.board_settings.retry_attempts; }
                        if q.board_settings.interval_boards     == default.interval_boards  { q.board_settings.interval_boards  = opt.board_settings.interval_boards; };
                        if q.board_settings.interval_threads    == default.interval_threads { q.board_settings.interval_threads = opt.board_settings.interval_threads; }
                        if q.board_settings.with_threads        == default.with_threads     { q.board_settings.with_threads = opt.board_settings.with_threads; }
                        if q.board_settings.with_archives       == default.with_archives    { q.board_settings.with_archives = opt.board_settings.with_archives; }
                        if q.board_settings.with_tail           == default.with_tail        { q.board_settings.with_tail = opt.board_settings.with_tail; }
                        if q.board_settings.with_full_media     == default.with_full_media  { q.board_settings.with_full_media = opt.board_settings.with_full_media; }
                        if q.board_settings.with_thumbnails     == default.with_thumbnails  { q.board_settings.with_thumbnails  = opt.board_settings.with_thumbnails; }
                        if q.board_settings.watch_boards        == default.watch_boards     { q.board_settings.watch_boards = opt.board_settings.watch_boards; }
                        if q.board_settings.watch_threads       == default.watch_threads    { q.board_settings.watch_threads = opt.board_settings.watch_threads; } 

                        q.clone()
                    }
                    Err(e) => {
                        epintln!((e));
                        opt
                    }
                }
            }
        } else {
            opt
        }
    };
    // TODO db_url is up-to-date, also update the individual fields (extract from db_url) 
    if opt.database.url.is_none() {
        let db_url = format!(
            "{engine}://{user}:{password}@{host}:{port}/{database}",
            // ?charset={charset}
            engine = if &opt.database.engine.as_str().to_lowercase() != "postgresql" { "mysql" } else { &opt.database.engine },
            user = &opt.database.username,
            password = &opt.database.password,
            host = &opt.database.host,
            port = &opt.database.port,
            database = &opt.database.name,
            // charset = &opt.database.charset
        );
        opt.database.url = Some(db_url);
    }
    
    if opt.asagi_mode && !opt.database.url.as_ref().unwrap().contains("mysql") {
        return Err(eyre!("Asagi mode must be used with a MySQL database. Did you mean to disable --asagi ?"));
    }
    
    if !opt.asagi_mode && !opt.database.url.as_ref().unwrap().contains("postgresql") {
        return Err(eyre!("Ena must be used with a PostgreSQL database. Did you mean to enable --asagi ?"));
    }
    SEMAPHORE_AMOUNT.fetch_add(if opt.strict { 1 } else { opt.limit }, Ordering::SeqCst);
    MEDIA_SEMAPHORE_AMOUNT.fetch_add(opt.media_threads, Ordering::SeqCst);
    
    // &opt.threads.dedup();

    // Afterwards dedup boards
    use itertools::Itertools;
    opt.boards = (&opt.boards).into_iter().unique_by(|board| board.board.as_str()).map(|b| b.clone()).collect();

    if !&opt.media_dir.is_dir() {
        std::fs::create_dir_all(&opt.media_dir)?;
    }

    Ok(opt)
}

async fn test_main() -> Result<()> {
    let opt = get_opt()?;
    
    println!("{:#?}",opt);
    return Ok(());

    if !opt.asagi_mode {
        let (client, connection) = tokio_postgres::connect(opt.database.url.as_ref().unwrap(), tokio_postgres::NoTls).await.unwrap();
        Task::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
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
        let rw = async_std::sync::RwLock::new(conn);
        let fchan = FourChan::new(create_client("http://boards.4chan.org", &opt).await?, rw, opt).await;
        fchan.run().await?;
        drop(fchan);
        pool.disconnect().await?;
        Ok(())
    }
}

fn get_ctrlc() -> u8 {
    CTRLC.load(Ordering::SeqCst)
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
                    eprintln!("download_boards_index: {}", e);
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn download_board_and_thread(&self, boards: Option<Vec<Board>>, thread_entry: Option<(&Board, u64)>) -> Result<()> {
        if let Some(_boards) = boards {
            let mut startup = true;
            // cycle
            loop {
                let hz = Duration::from_millis(250);
                let mut interval = Duration::from_millis(30_000); // this is temporary, it's updated at the end of the for loop
                // let mut ratelimit_orig = sql::refresh_rate(30, 5, 10);
                // let mut ratelimit = ratelimit_orig.clone();
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
                        self.download_board(&_board, "threads", startup).await.unwrap();
                    } else if !_board.with_threads && _board.with_archives {
                        self.download_board(&_board, "archive", startup).await.unwrap();
                    } else if _board.with_threads && _board.with_archives {
                        let (threads, archive) = futures::join!(self.download_board(&_board, "threads", startup), self.download_board(&_board, "archive", startup));
                        threads.unwrap();
                        archive.unwrap();
                    }
                    if !_board.watch_boards || get_ctrlc() >= 1 {
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
                    if get_ctrlc() >= 1 {
                        break;
                    }
                    sleep(hz).await;
                }
                if get_ctrlc() >= 1 {
                    break;
                }
            }
        } else if let Some((board_info, thread)) = thread_entry {
            // TODO loop here if watch-threads

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

            loop {
                let now = Instant::now();
                let hz = Duration::from_millis(250);
                let interval = Duration::from_millis(_board.interval_boards.into());
                // with_threads or with_archives don't apply here, since going here implies you want the
                // thread/archive
                // FIXME on delted/archived, need to leave the loop
                self.download_thread(&_board, thread, "threads").await.unwrap();
                if !_board.watch_threads || get_ctrlc() >= 1 {
                    return Ok(());
                }
                // FIXME: elpased() silent panic
                // Need to know if modified to be able to use ratelimt struct
                // Duration::from_millis(ratelimit.next().unwrap_or(30) * 1000)
                while now.elapsed() < interval {
                    if get_ctrlc() >= 1 {
                        break;
                    }
                    sleep(hz).await;
                }
                if get_ctrlc() >= 1 {
                    break;
                }
            }
        } else {
            unreachable!()
        }
        Ok(())
    }

    async fn download_board(&self, board_info: &Board, thread_type: &str, startup: bool) -> Result<()> {
        let fun_name = "download_board";
        if startup && self.opt.asagi_mode {
            // The group of statments for just `Boards` was done in the beginning, so this can be called
            // This will create all the board tables, triggers, etc if the board doesn't exist
            self.db_client.board_table_exists(&board_info.board, &self.opt).await;

            // Init the actual statements for this specific board
            self.db_client.init_statements(board_info.id, &board_info.board).await;
        }
        // Board has to take all permits
        let sem = SEMAPHORE.acquire(1).await;
        if get_ctrlc() >= 1 {
            return Ok(());
        }
        if !startup {
            sleep(Duration::from_secs(1)).await;
        }
        let last_modified = self.db_client.board_get_last_modified(thread_type, board_info.id).await;
        let url = self.opt.api_url.join(fomat!((&board_info.board)"/").as_str())?.join(&fomat!((thread_type)".json"))?;
        if get_ctrlc() >= 1 {
            return Ok(());
        }
        for retry in 0..=board_info.retry_attempts {
            if get_ctrlc() >= 1 {
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
                                if thread_type == "threads" {
                                    self.db_client.threads_get_modified(board_info.id, &body_json).await
                                } else {
                                    self.db_client.threads_get_combined("archive", board_info.id, &body_json).await
                                }
                            };
                            // TODO mysql or postgres
                            match either {
                                Either::Right(rows) =>
                                    if let Some(mut rows) = rows {
                                        let mut fut = FuturesUnordered::new();
                                        while let Some(no) = rows.next().await {
                                            fut.push(self.download_thread(board_info, no, thread_type));
                                        }
                                        drop(sem);
                                        while let Some(res) = fut.next().await {
                                            res.unwrap();
                                        }
                                    },
                                Either::Left(rowstream) => {
                                    match rowstream {
                                        Ok(rows) => {
                                            futures::pin_mut!(rows);
                                            let mut fut = FuturesUnordered::new();
                                            while let Some(row) = rows.next().await {
                                                match row {
                                                    Ok(row) => {
                                                        let t: i64 = row.get(0);
                                                        fut.push(self.download_thread(board_info, t as u64, thread_type));
                                                    }
                                                    Err(e) => {
                                                        epintln!((fun_name) ":  (" (thread_type) ") /" (&board_info.board) "/"
                                                        if board_info.board.len() <= 2 { "\t\t" } else {"\t"}
                                                        (&lm)
                                                        " | ["
                                                        (e)"]"
                                                        );
                                                    }
                                                }
                                            }
                                            // Drop permit to allow single thread
                                            // Reacquire permit at the end
                                            drop(sem);
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
                                    }
                                }
                            }
                            
                            if get_ctrlc() >= 1 {
                                break;
                            }
                            
                            // Update threads/archive cache at the end
                            if thread_type == "threads" {
                                self.db_client.board_upsert_threads(board_info.id, &board_info.board, &body_json, &lm).await.unwrap();
                            } else if thread_type == "archive" {
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

    async fn download_thread(&self, board_info: &Board, thread: u64, thread_type: &str) -> Result<()> {
        let sem = SEMAPHORE.acquire(1).await;
        if get_ctrlc() >= 1 {
            return Ok(());
        }
        // TODO: OR media not complete (missing media)
        let last_modified = self.db_client.thread_get_last_modified(board_info.id, thread).await;
        let url = self.opt.api_url.join(fomat!((&board_info.board)"/").as_str())?.join("thread/")?.join(&format!(
            "{thread}{tail}.json",
            thread = thread,
            tail = if board_info.with_tail { "-tail" } else { "" }
        ))?;
        for retry in 0..=board_info.retry_attempts {
            let resp = self.client.gett(url.as_str(), &last_modified).await;
            if retry != 0 {
                sleep(Duration::from_secs(1)).await;
            }
            match resp {
                Ok((status, lm, body)) => {
                    match status {
                        StatusCode::OK => {
                            // Going here (StatusCode::OK) means thread was modified
                            // DELAY
                            sleep(Duration::from_millis(board_info.interval_threads.into())).await;

                            // ignore if err, it means it's empty or incomplete
                            match serde_json::from_slice::<serde_json::Value>(&body) {
                                Ok(mut thread_json) => {
                                    
                                    if !board_info.with_tail {
                                        update_post_with_extra(&mut thread_json);
                                    }
                                    // let posts = thread_json.get("posts").unwrap().as_array().unwrap();
                                    let op_post = thread_json.get("posts").unwrap().as_array().unwrap().iter().nth(0).unwrap();

                                    let no: u64 = op_post["no"].as_u64().unwrap();
                                    let resto: u64 = op_post["resto"].as_u64().unwrap_or(0);

                                    if board_info.with_tail {
                                        // Check if we can use the tail.json
                                        // Check if we have the tail_id in the db
                                        let tail_id = op_post["tail_id"].as_u64().unwrap();
                                        let query = (&self)
                                            .db_client.post_get_single(board_info.id, thread, tail_id)
                                            //.query_one("SELECT * from posts where board=$1 and resto=$2 and no=$3 LIMIT 1;", &[&(board_info.id as i16), &(thread as i64), &(tail_id as i64)])
                                            .await;

                                        // If no Row returned, download thread normally
                                        if !query {
                                            let mut _board_info = board_info.clone();
                                            _board_info.with_tail = false;
                                            drop(sem);
                                            return self.download_thread(&_board_info, thread, thread_type).await;
                                        }

                                        // Pop tail_id, tail_size
                                        // Insert extra
                                        // thread_json["posts"].as_array_mut().unwrap().remove(0);
                                        // Don't pop OP post (which has no time). It will be filtered upon inside `thread_upsert`.
                                        update_post_with_extra(&mut thread_json);
                                    }

                                    if get_ctrlc() >= 1 {
                                        return Ok(());
                                    }

                                    // TODO: Download media only if we don't already have it

                                    // Upsert Thread
                                    let len = self.db_client.thread_upsert(board_info, &thread_json).await;

                                    // Display
                                    // "download_thread: ({thread_type}) /{board}/{thread}{tail}{retry_status} {new_lm} | {prev_lm} |
                                    // {len}"
                                    pintln!("download_thread: (" (thread_type) ") /" (&board_info.board) "/" (thread)
                                        if board_info.with_tail { "-tail" } else { "" }
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
                                                    println!("download_thread: ({}) /{}/{}#{}\t[DELETED]", thread_type, &board_info.board, thread, no)
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
                                                            println!("download_thread: ({}) /{}/{}#{}\t[DELETED]", thread_type, &board_info.board, if resto == 0 { no } else { resto }, no)
                                                        }
                                                    }
                                                }
                                            },
                                    }
                                    
                                    if board_info.with_full_media || board_info.with_thumbnails {
                                    // Get Thread
                                    let either = self.db_client.thread_get(board_info, thread).await;
                                    match either {
                                        Either::Right(res) => {
                                            match res {
                                                Err(e) => epintln!("download_threaD: /"(&board_info.board)"/"(thread)"\t" (e)),
                                                Ok(rows) => {
                                                    let post = yotsuba::Post::default();
                                                    let mut fut = rows.into_iter().map(|row| {
                                                        self.download_media(board_info, &post)
                                                    }).collect::<FuturesUnordered<_>>();
                                                    while let Some(res) = fut.next().await {
                                                        res.unwrap(); 
                                                    }
                                                },
                                            }
                                        },
                                        Either::Left(res) => {
                                            match res {
                                                Err(e) => epintln!("download_threaD: /"(&board_info.board)"/"(thread)"\t" (e)),
                                                Ok(rows) => {
                                                    futures::pin_mut!(rows);
                                                    let post = yotsuba::Post::default();
                                                    let mut fut = FuturesUnordered::new();
                                                    while let Some(Ok(row)) = rows.next().await {
                                                        fut.push(self.download_media(board_info, &post));    
                                                    }
                                                    
                                                    while let Some(res) = fut.next().await {
                                                        res.unwrap(); 
                                                    }
                                                },
                                            }
                                        },
                                    }
                                    // let mut fut = posts.iter().filter(|post|
                                    // post.get("md5").map(|md5|
                                    // md5.as_str()).flatten().is_some()).map(|post|
                                    // self.download_media(board_info,
                                    // post)).collect::<FuturesUnordered<_>>(); 
                                    // while let Some(res)
                                    // = fut.next().await {
                                    // res.unwrap(); }

                                    if get_ctrlc() >= 1 {
                                        break;
                                    }
                                    }
                                    
                                    // Update thread's last_modified
                                    self.db_client.thread_update_last_modified(&lm, board_info.id, thread).await.unwrap();
                                    break; // exit the retry loop

                                }
                                Err(e) => {
                                    eprintln!("download_thread: ({}) /{}/{}{} {} {:?}", thread_type, board_info.board, thread, if board_info.with_tail { "-tail" } else { "" }, e, &last_modified);
                                }
                            }
                        }
                        StatusCode::NOT_FOUND => {
                            if board_info.with_tail {
                                let mut _board_info = board_info.clone();
                                _board_info.with_tail = false;
                                drop(sem);
                                return self.download_thread(&_board_info, thread, thread_type).await;
                            }
                            let either = self.db_client.thread_update_deleted(board_info.id, thread).await;
                            let tail = if board_info.with_tail { "-tail" } else { "" };
                            match either {
                                Either::Right(rows) =>
                                    if let Some(mut rows) = rows {
                                        while let Some(no) = rows.next().await {
                                            println!(
                                                "download_thread: ({thread_type}) /{board}/{thread}{tail}\t[{status}] [DELETED]",
                                                thread_type = thread_type,
                                                board = &board_info.board,
                                                thread = no,
                                                tail = tail,
                                                status = status
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
                                                        println!(
                                                            "download_thread: ({thread_type}) /{board}/{thread}{tail}\t[{status}] [DELETED]",
                                                            thread_type = thread_type,
                                                            board = &board_info.board,
                                                            thread = no,
                                                            tail = tail,
                                                            status = status
                                                        );
                                                    } else {
                                                        eprintln!(
                                                            "download_thread: ({thread_type}) /{board}/{thread}{tail}\t[{status}] [`no` is empty]",
                                                            thread_type = thread_type,
                                                            board = &board_info.board,
                                                            thread = thread,
                                                            tail = tail,
                                                            status = status
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    eprintln!(
                                                        "download_thread: ({thread_type}) /{board}/{thread}{tail}\t[{status}] [thread_update_deleted][{err}]",
                                                        thread_type = thread_type,
                                                        board = &board_info.board,
                                                        thread = thread,
                                                        tail = tail,
                                                        status = &status,
                                                        err = e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "download_thread: ({thread_type}) /{board}/{thread}{tail}\t[{status}] [thread_update_deleted][{err}]",
                                            thread_type = thread_type,
                                            board = &board_info.board,
                                            thread = thread,
                                            tail = tail,
                                            status = &status,
                                            err = e
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
                            eprintln!("download_thread: /{}/{}{} [{}]", board_info.board, thread, if board_info.with_tail { "-tail" } else { "" }, status);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("download_thread: /{}/{}{}\n{}", board_info.board, thread, if board_info.with_tail { "-tail" } else { "" }, e);
                }
            }
        }
        Ok(())
    }

    async fn download_media(&self, board_info: &Board, post: &yotsuba::Post) -> Result<()> {
        // println!("/{}/{} | {} [media]", board_info.board, post["resto"].as_u64().unwrap(),
        // post["no"].as_u64().unwrap()); DL thumbs or not
        // Validate file & hash
        // Update post's checksum (sha256, etc..)
        // utf8_percent_encode(&filename, percent_encoding::NON_ALPHANUMERIC);
        Ok(())
    }
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
        println!("Press CTRL+C to exit");
        ctrlc::set_handler(move || {
            CTRLC.fetch_add(1, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
        test_main().await.unwrap();
        pintln!("Done!");
        Ok(())
    })
}
