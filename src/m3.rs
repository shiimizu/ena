
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_must_use)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]
#![deny(unsafe_code)]
#![allow(unused_assignments)]
#![allow(non_snake_case)]
#![recursion_limit="1024"]

#![feature(type_ascription)]
#![feature(exclusive_range_pattern)]
#![feature(slice_partition_dedup)]

extern crate reqwest;
extern crate pretty_env_logger;
extern crate rand;
#[macro_use] extern crate log;

#[macro_use] extern crate if_chain;

extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;

use async_std::task;
use async_std::prelude::*;
use async_std::stream;
use futures::stream::StreamExt as FutureStreamExt;
use futures::stream::FuturesUnordered;
use std::time::{Duration, Instant};
use postgres::{Connection, TlsMode};
use reqwest::header::{HeaderMap, HeaderValue, LAST_MODIFIED, USER_AGENT, IF_MODIFIED_SINCE};
use rand::distributions::{Distribution, Uniform};
use async_std::sync::{Arc, Mutex};
use std::collections::{BTreeMap, VecDeque};
use chrono::{Local,DateTime, TimeZone, Utc};
use sha2::{Sha256, Sha512, Digest};
use serde_json::json;


fn main() {
    let start_time = Instant::now();
    println!("{}", yotsuba_time());
    pretty_env_logger::init();
    start_background_thread();
    println!("Program finished in {} ms", start_time.elapsed().as_millis());
}

fn start_background_thread() {
    let mut handles = vec![];

    // Watch config changes on another thread so it won't interfere with the async scheduler on the other thread
    handles.push(std::thread::spawn(move || {
        task::block_on(async {

            // e6711da77b5da201ebe55fdbab54e04fe5f0fcf26cb8a0d0ffbbff5c6412bac7
            /*let now = Instant::now();
            if let Some(hash) = dl_media_and_hash("http://i.4pcdn.org/o/1575780851141.jpg", "test.jpg") {
                println!("{}", hash);
            }
            println!("elasped {:?}ms", now.elapsed().as_millis());*/

            //calc_sha256("1575780851141.jpg");

            let file = async_std::fs::File::open("ena_config.json").await.unwrap();
            //let af = Arc::clone(&file);
            let mut last_modified = std::time::SystemTime::now();
            loop {
                let metadata = &file.metadata().await.unwrap();
                if let Ok(time) = metadata.modified() {
                    if time != last_modified {
                        last_modified = time;
                        if let Ok(since) = time.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                            //Utc.timestamp(since.as_secs() as i64, 0).to_rfc2822().replace("+0000", "GMT")
                            println!("Config modified on {}", Local::now().to_rfc2822());
                        }
                    } else {
                        //println!("Unchanged");
                    }
                }
                task::sleep(Duration::from_secs(1)).await;
            }
        });
       
    }));
    // Do all work in a background thread to keep the main ui thread running smoothly
    // This essentially keeps CPU and power usage extremely low 
    handles.push(std::thread::spawn(move || {

        // By using async tasks we can process an enormous amount of tasks concurrently
        // without the overhead of multiple native threads. 
        task::block_on(start_async());

    }));
    for handle in handles {
        handle.join().unwrap();
    }
}

async fn start_async() {
    let mut archiver = YotsubaArchiver::new("postgresql://postgres:zxc@localhost:5432/archive", "ena_config.json").await;
    
    archiver.init_boards().await;
    archiver.rebase();
    archiver.poll_boards();

}

const RATELIMIT: u16 = 1000;
const REFRESH_DELAY: u8 = 11;


// Have a struct to have a place to store our variables without using global statics
pub struct YotsubaArchiver<'a> {
    conn: Connection,
    settings : serde_json::Value,
    queue: Queue<'a>,
    proxies: ProxyStream,
}

impl YotsubaArchiver<'_> {

    async fn new<'a>(connection_url: &'a str, config_path: &'a str) -> YotsubaArchiver<'a> {
        YotsubaArchiver {
                conn: Connection::connect(connection_url, TlsMode::None).expect("Error connecting"),
                settings: Self::get_config(config_path).await,
                queue: Queue::new(),
                proxies: Self::get_proxy("cache/proxylist.json").await,
            }
    }

    async fn init_boards(&mut self) {
        // Init DB
        self.conn.execute("
                    CREATE TABLE IF NOT EXISTS main
                    (
                        id smallint NOT NULL,
                        data jsonb,
                        PRIMARY KEY (id),
                        UNIQUE (id)
                    )
                    ", &[]).unwrap();
        let j = read_file("boards.json").await.unwrap();
        self.conn.execute("INSERT INTO main (id, data) VALUES (1, $1::jsonb) ON CONFLICT (id) DO NOTHING;",
                        &[&serde_json::from_str::<serde_json::Value>(&j).unwrap()]).unwrap();
        
        let check_data_result = self.conn.query("SELECT data->'boards'->0->'threads'->0 FROM main where id=1", &[]);
        if let Err(_) = check_data_result {
            self.conn.execute(r#"
                UPDATE main
                set data = js
                FROM (
                    select jsonb_build_object('boards',jsonb_agg(t)) as js from (
                                                select (jsonb_array_elements(data->'boards') || '{"threads":{}}'::jsonb) as t
                                                from main where id=1) x
                ) AS subquery
                where id=1
                "#, &[]).unwrap();
        }

        let mut fut = FuturesUnordered::new();

        // https://doc.rust-lang.org/book/ch16-03-shared-state.html
        for board in self.get_boards_raw().iter() {
            fut.push(self.assign_to_board2(board));
        }

        // Runs all tasks in the list concurrently until completion
        // Post processing after the asynchronous tasks are done
        while let Some(_) = fut.next().await {
        }
    }

    fn rebase(&mut self) {
        println!("{:?}", self.get_boards_raw());
        //println!("{}", serde_json::to_string_pretty(&self.get_default_board_settings()).unwrap());
        println!("{}", serde_json::to_string_pretty(&self.get_boards_config()).unwrap());
    }

    fn poll_boards(&self) {
    }

    /*async fn init_data(&mut self) -> Option<&mut serde_json::Map<String, serde_json::Value>> {
        let settings = self.config.get("sites").unwrap()
                                    .as_array().unwrap()[0]
                                    .get("settings").unwrap();
        let boardSettings = settings.get("boardSettings").unwrap();
        let boards = settings.get("boards").unwrap()
                                    .as_array().unwrap();
        let boards_list: Vec<&str> = boards.iter().map(|v| v.get("board").unwrap().as_str().unwrap()).collect();
            /*
        let mut vv = self.config.get_mut("settings").unwrap()
                .get_mut("boardSettings").unwrap()
                .get_mut("boards").unwrap()
                .as_array_mut().unwrap()[0]
                .as_object_mut().unwrap();
            vv.insert("asd".to_string(), json!("asdfgb"));*/
            //println!("{}",serde_json::to_string(&self.config).unwrap());
            //println!("{}",serde_json::to_string_pretty(&self.config).unwrap());
    }*/

    async fn assign_to_board2<'b>(&self, board: &'b str) {
        loop {
        //println!("{}", board);
        println!("{:?}", self.get_boards_raw2().await);
        task::sleep(Duration::from_millis(RATELIMIT.into())).await;
        }
    }
        
    async fn assign_to_board<'b>(board: &'b str,  queue_arc : Arc<Mutex<Queue<'_>>>) ->Option<()>{
        let current_time = yotsuba_time();
        let mut current_board = String::from(board);
        let mut threads_last_modified = String::from(&current_time);
        let mut archive_last_modified = String::from(&current_time);
        let mut threads_json_string = String::new(); 
        //let mut threads_list = vec![]; 
        let mut local_list = VecDeque::new();
        let mut archive_list = vec![];
        let one_millis = Duration::from_millis(1);
        loop {
            let now = Instant::now();
            

            // Download threads.json
            {
                //println!("/{}/ GET threads.json", current_board);
                let (headers, status, body) = 
                                                get_chan(&format!("{url}/{bo}/threads.json", url="http://a.4cdn.org", bo=board), &threads_last_modified);
                match status {
                    reqwest::StatusCode::OK => {
                        match headers {
                            Some(last_modified) => {
                                if threads_last_modified != last_modified {
                                    threads_last_modified.clear();
                                    threads_last_modified.push_str(&last_modified);
                                }
                            },
                            None => eprintln!("/{}/ {:?} an error has occurred getting the last_modified date", current_board, status),
                        }
                        match body {
                            Ok(tbody) => {
                                threads_json_string.clear();
                                threads_json_string.push_str(&tbody);
                            },
                            Err(e) => eprintln!("/{}/ {:?} an error has occurred getting the body\n{}", current_board, status, e),
                        }
                    },
                    reqwest::StatusCode::NOT_MODIFIED => {
                        eprintln!("/{}/ {}", current_board, status);
                    },
                    _ => {
                        eprintln!("/{}/ {:?} an error has occurred", current_board, status);
                    },
                }
                if !threads_json_string.is_empty() {
                    println!("/{}/ Received new threads on {}", current_board, Local::now().to_rfc2822());
                    // get new list using postgres' amazing FULL JOIN functionality lol
                    let mut recieved = VecDeque::new();
                    local_list.append(&mut recieved);

                }
                task::sleep(Duration::from_millis(RATELIMIT.into())).await; // Ratelimit
            }


            // Download archive.json
            {        
                let has_archives = true;
                if has_archives {
                    //println!("/{}/ GET archive.json", current_board);
                    let (headers, status, body) = 
                                                    get_chan(&format!("{url}/{bo}/archive.json", url="http://a.4cdn.org", bo=board), &archive_last_modified);
                    match status {
                        reqwest::StatusCode::OK => {
                             match headers {
                                Some(last_modified) => {
                                    if archive_last_modified != last_modified {
                                        archive_last_modified.clear();
                                        archive_last_modified.push_str(&last_modified);
                                    }
                                },
                                None => eprintln!("/{}/ {:?} an error has occurred getting the last_modified date", current_board, status),
                            }
                            match body {
                                Ok(tbody) => {
                                    let mut recieved:VecDeque<u32> = 
                                                serde_json::from_str(&tbody).unwrap();
                                    let mut recieved_clone = vec![];
                                    recieved_clone.extend(&recieved);

                                    // Only get the new threads
                                    recieved.retain(|x| !archive_list.contains(x));

                                    // Use the new json as base
                                    archive_list = recieved_clone;
                                    if !recieved.is_empty() {
                                        println!("/{}/ Received new archive threads on {}", current_board, Local::now().to_rfc2822());
                                        local_list.append(&mut recieved);
                                    }
                                },
                                Err(e) => eprintln!("/{}/ {:?} an error has occurred getting the body\n{}", current_board, status, e),
                            }
                        },
                        reqwest::StatusCode::NOT_MODIFIED => {
                            eprintln!("/{}/ {}", current_board, status);
                        },
                        _ => {
                            eprintln!("/{}/ {:?} an error has occurred", current_board, status);
                        },
                    }
                }
                task::sleep(Duration::from_millis(RATELIMIT.into())).await; // Ratelimit
            }

            {
                if let Some(mut map_guard) = queue_arc.try_lock() {
                    if let Some(mut queue) = map_guard.get_mut(current_board.as_str()) {
                        queue.append(&mut local_list);
                    }
                }
            }


            // This block of code is for workers to help other workers at differenct boards. They will leave their board and switch to another
            // to help another.
            // Blocking calls shouln't be used while the gaurd is still alive. That would cause locks
            if let Some(mut map_guard) = queue_arc.try_lock() {

                // Go to next board that's has threads
                let mut iter = map_guard.iter();
                let mut iter2 = iter.clone();
                let mut limit = 0;
                let map_len = map_guard.len();
                let pos = iter2.position(| (&k, _)| k == current_board).unwrap();
                let mut cycle = iter.cycle();
                for _ in 0..=pos {
                    cycle.next();
                }
                while let Some((&k,v)) = cycle.next() {
                    if limit > map_len {
                        break;
                    }
                    if !v.is_empty() {
                        //println!("\nBefore: [{}] {} -> {}",board, current_board , k);
                        current_board.clear();
                        current_board.push_str(k);
                        break;
                    }
                    limit += 1;
                }
            } else {
                println!("/{}/ Thread lock trying to switch boards at {}", current_board, yotsuba_time());
            }
            

            // Rather than doing a loop inside, which would cause locks because the MutexGuard would be alive the
            // whole time processing an expensive loop, we put the loop outside which means the lock gets dropped each
            // iteration. This does have some slight overhead but this essentially allows concurrent read/write access
            // across multiple threads without deadlocks.
            // This loop will drain the thread list
            //
            // Threads running concurrently reach this point
            // It will alternate from locked to unlocked, meaning other threads can have a crack at processing the thread at the board they're on.
            // Here workers have a chance to work on their own boards with all the data intact and synced across threads.
            // This is an.. interesting way to get a SINGLE value out a shared mutable data.
            'thread_loop: loop {
                let mut thread = None;
                let now = Instant::now();
                if let Some(mut map_guard) = queue_arc.try_lock() {
                    if let Some(mut queue) = map_guard.get_mut(current_board.as_str()) {
                        
                        // Pop a single value, then reloop
                        // Normally I'd do a while let here, but since we have to account for Mutexes, that won't be possible here.
                        match queue.pop_front() {
                            
                            // Good visualization:
                            // <thread> [original assigned board] -> current board this worker is working on
                            // println!("Popped: <{}> [{}] -> {}",item, board, current_board );
                            Some(item) => { thread = Some(item) },//println!("Popped {}", item),
                            None => break 'thread_loop, // End loop if there's no more
                        }
                    }
                } else {
                    println!("\nThread lock at {}", yotsuba_time());
                    break 'thread_loop;
                }
                
                // We finlly got a thread from all that code above >_>
                if let Some(t) = thread {
                    assign_to_thread(&current_board, t).await;

                    // Ratelimit on thread
                    while now.elapsed().as_millis() <= RATELIMIT.into() {
                        task::sleep(one_millis).await; 
                    }
                }
            }
            
            // Ratelimit after fetching threads
            while now.elapsed().as_secs() <= REFRESH_DELAY.into() {
                task::sleep(one_millis).await;
            }

            //println!("/{}/ -> /{}/ Done inside test {:?}", board , current_board, now.elapsed().as_millis());
        }

        //task::sleep(Duration::from_secs(1)).await;
        Some(())
    }

    async fn assign_to_thread(board: &str, thread:u32) {
        println!("/{}/{}", board, thread);
        // check if db has, thread, archived, closed, sticky -> false: END
        // dl and patch and push to db
        // check if media has, md5 -> sha256, sha256t
        // dl single media and push to db, loop until complete
        // end
    }

    fn get_boards_config(&self) -> Vec<BoardSettings>{
        self.settings.get("sites").unwrap()
            .as_array().unwrap()[0]
            .get("settings").unwrap()
            .get("boardSettings").unwrap()
            .get("boards").unwrap()
            .as_array().unwrap()
            .iter().map(|z| serde_json::from_value::<BoardSettings>(z.to_owned()).unwrap()).collect::<Vec<BoardSettings>>()
    }
    fn get_default_board_settings(&self) -> BoardSettings {
        serde_json::from_value(
        self.settings.get("sites").unwrap()
            .as_array().unwrap()[0]
            .get("settings").unwrap()
            .get("boardSettings").unwrap()
            .get("default").unwrap().to_owned()).unwrap()

    }
    fn get_boards_raw(&self) -> Vec<&str> {
        self.settings.get("sites").unwrap()
        .as_array().unwrap()[0]
        .get("settings").unwrap()
        .get("boardSettings").unwrap()
        .get("boards").unwrap()
        .as_array().unwrap()
        .iter().to_owned()
        .map(|x| x.as_object().unwrap()
                    .get("board").unwrap()
                    .as_str().unwrap()
                    ).collect::<Vec<&str>>()
    }
    async fn get_boards_raw2(&self) -> Vec<String> {
        let s = Self::get_config("ena_config.json").await.to_owned();
        s.get("sites").unwrap()
        .as_array().unwrap()[0]
        .get("settings").unwrap()
        .get("boardSettings").unwrap()
        .get("boards").unwrap()
        .as_array().unwrap()
        .iter().to_owned()
        .map(|x| x.as_object().unwrap()
                    .get("board").unwrap()
                    .as_str().unwrap().to_string()
                    ).collect::<Vec<String>>()
    }

    async fn get_config(config_path: &str) -> serde_json::Value {
        serde_json::from_str::<serde_json::Value>(
                            &read_file(config_path).await.unwrap()).unwrap()
    }
    async fn get_proxy(proxy_path: &str) -> ProxyStream {
        if let Ok(p) = read_file(proxy_path).await {
            let mut ps = ProxyStream::new();
            ps.urls.append(
            &mut serde_json::from_str::<VecDeque<String>>(&p).unwrap());
            ps
        } else {
            ProxyStream::new()
        }
    }
}

async fn build_client( timeout: u32, connect_timeout: u32, proxy: Option<String>)->reqwest::Client {
    if let Some(p) = proxy {
    reqwest::Client::builder()
                        //.cookie_store(true)
                        .proxy(reqwest::Proxy::http(&p).unwrap())
                        .timeout(Duration::from_secs(timeout.into()))
                        .connect_timeout(Duration::from_secs(connect_timeout.into())).build().unwrap()
    } else {
    reqwest::Client::builder()
                        //.cookie_store(true)
                        //.proxy(reqwest::Proxy::http(proxy).unwrap())
                        .build().unwrap()
    }
}


async fn read_file(path: &str) -> async_std::io::Result<String> {
    let contents = async_std::fs::read_to_string(path).await?;
    Ok(contents)
}

fn sha256sum(path: &str)  -> std::result::Result<String, std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let n = std::io::copy(&mut file, &mut hasher);
    let hash = hasher.result();
    //println!("Path: {}", path);
    //println!("Bytes processed: {}", n);
    //println!("Hash value: {:x}", hash);
    Ok(format!("{:x}", hash))
    //String::from_utf8(hash.to_vec())
    
}

// https://rust-lang-nursery.github.io/rust-cookbook/web/clients/download.html
fn dl_media(url: &str, out: &str) -> Result<(), reqwest::Error> {
    // Don't use for lot's of medias
    // let mut resp = get_custom(url, None, None, None, None).unwrap();
    let mut resp = reqwest::get(url)?;
    let mut dest = std::fs::File::create(out).unwrap();
    let n = std::io::copy(&mut resp, &mut dest).unwrap();
    // println!("Bytes processed: {}", n2);
    // println!("saved file Bytes processed: {}", n);
    Ok(())
}

fn dl_media_and_hash(url: &str, out: &str) -> Option<String>  {
    match dl_media(url, out) {
        Ok(w) => {
            match sha256sum(out) {
                Ok(hash) => Some(hash),
                Err(e) => None,
            }

        },
        Err(e) => Some(e.to_string()),
    }
}

fn get_chan<'a>(url: &'a str, header: &'a str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
    let mut default_headers = HeaderMap::new();
    default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0"));
    default_headers.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(header).unwrap());
    let mut res = get(url, default_headers).unwrap();

    let mut last_modified : Option<String> = None;
    if_chain! {
        if let Some(head) = res.headers().get(LAST_MODIFIED);
        if let Ok(head_str) = head.to_str();
        then {
            last_modified = Some(head_str.to_string());
        }
    }
    let status = res.status();
    let body = res.text();
    (last_modified, status, body)
}

fn yotsuba_time() -> String {
    chrono::Utc::now().to_rfc2822().replace("+0000", "GMT")
}

fn get<H: Into<Option<HeaderMap>>>(url: &str, headers: H) -> Result<reqwest::Response, reqwest::Error> {
    get_custom(url, headers.into(), None, None, None)
}

// https://hoverbear.org/blog/optional-arguments/
// https://doc.rust-lang.org/std/convert/trait.Into.html
// GET requests with proxies are build from client level, not from get methods.
 fn get_custom<H: Into<Option<HeaderMap>>,
            T: Into<Option<Duration>>,
            P: Into<Option<String>>>
            (url: &str,
            headers: H,
            timeout: T,
            connect_timeout: T,
            proxy: P) -> Result<reqwest::Response, reqwest::Error> {
    let mut client = reqwest::ClientBuilder::new();

    if let Some(prox) = proxy.into() {
        client = client.proxy(reqwest::Proxy::http(&prox)?);
    }
    if let Some(h) = headers.into() {
        client = client.default_headers(h);
    }
    if let Some(tm) = timeout.into() {
        client = client.timeout(tm);
    }
    if let Some(ctm) = connect_timeout.into() {
        client = client.connect_timeout(ctm);
    }
    let client = client.build()?;
                        
    let resp = client.get(url).send()?;
    Ok(resp)
}

type Queue<'a> = BTreeMap<&'a str, VecDeque<u32>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BoardSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    board: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    engine: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    charset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    useProxy: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryAttempts: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refreshDelay: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    throttleURL: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    throttleMillisec: Option<u32>,
}
/// A cycle stream that can append new values
#[derive(Debug)]
pub struct ProxyStream {
    urls: VecDeque<String>,
    count: i32,
}

impl ProxyStream {
    fn push(&mut self, s: String) {
        self.urls.push_front(s);
    }
    fn new() -> ProxyStream {
        ProxyStream { urls: VecDeque::new(), count: 0 }
    }
    fn len(&self) -> usize {
        self.urls.len()
    }
}

impl futures::stream::Stream for ProxyStream {

    type Item = String;

    fn poll_next(mut self: async_std::pin::Pin<&mut Self>, _cx: &mut async_std::task::Context<'_>) -> async_std::task::Poll<Option<Self::Item>> {
        self.count += 1;
        let mut c = 0;
        let max = self.urls.len();
        match self.count {
            0 => { c = 0 },
            x if x >= 1 && x < self.urls.len() as i32 => { c = x },
            _ => {
                self.count = 0;
                c = 0;
            }
        }
        if max != 0 {
            async_std::task::Poll::Ready(Some(self.urls[c as usize].to_owned()))
        } else {
            async_std::task::Poll::Ready(None)
        }

        
    }
}