#![allow(dead_code)]
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
// #[macro_use] extern crate log;

#[macro_use] extern crate if_chain;

extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;

use futures::stream::StreamExt as FutureStreamExt;
use futures::stream::FuturesUnordered;
use std::time::{Duration, Instant};
use postgres::{Connection, TlsMode};
use reqwest::header::{HeaderMap, HeaderValue, LAST_MODIFIED, USER_AGENT, IF_MODIFIED_SINCE};

use async_std::sync::{Arc, Mutex};
use std::collections::{BTreeMap, VecDeque};
use chrono::Local;
use sha2::{Sha256, Digest};
use async_std::prelude::*;
use async_std::task;
use std::io::BufReader;
use std::fs::File;
use std::thread;
use std::path::Path;
use std::ffi::OsStr;


#[macro_use] extern crate log;

const RATELIMIT: u16 = 1000;
const REFRESH_DELAY: u8 = 10;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct YotsubaPost {
    no: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    sticky: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    closed: Option<u8>,

    now: String,
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    sub: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    com: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    filedeleted: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    spoiler: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    custom_spoiler: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    filename: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    ext: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    w: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    h: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    tn_w: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    tn_h: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    tim: Option<u64>,

    time: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    md5: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    sha256: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    sha256t: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    fsize: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    m_img: Option<u8>,

    resto: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    trip: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    capcode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    country: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    country_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    archived: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    bumplimit: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    archived_on: Option<u8>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    imagelimit: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    semantic_url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    replies: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    images: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    unique_ips: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    tag: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    since4pass: Option<u16>
}

impl YotsubaPost {
    fn new() -> Self {
        Default::default()
    }
    fn prettify(&self) -> String {
        serde_json::to_string_pretty(&serde_json::to_value(self).unwrap()).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct YotsubaThread {
    posts: Vec<YotsubaPost>
}

fn main() {
    let start_time = Instant::now();
    //pretty_env_logger::init();
    println!("{}", yotsuba_time());
    /*let mut post = YotsubaPost::new();
    post.name = "Anon".to_string();

    println!("{}", post.prettify());
    let th: YotsubaThread = serde_json::from_reader(BufReader::new(std::fs::File::open("73915762.json").unwrap())).unwrap();
    */
    start_background_thread();
    /*{
    let mut archiver = YotsubaArchiver::new("postgresql://postgres:zxc@localhost:5432/archive", "ena_config.json");
    let start_time = Instant::now();
    let resp = archiver.conn.query("select * FROM a where (no=570368 or resto=570368) and (sha256 is null or sha256t is null) order by no", &[]).unwrap();
    for row in resp.iter() {

        let no : i64  = row.get("no");
        let sha256 : Option<String> = row.get("sha256");
        let sha256t : Option<String> = row.get("sha256t");
        //let j: serde_json::Value = row.get(0);
        //println!("{:?}", j.get("posts").unwrap().as_array().unwrap().len()); 
        println!("{} {:?} {:?}", no, sha256, sha256t);
    }
    println!("Program finished in {} ms", start_time.elapsed().as_millis());
        thread::sleep(Duration::from_secs(2));

    }*/
    //println!("{}", serde_json::to_string_pretty(&th).unwrap());

    println!("Program finished in {} ms", start_time.elapsed().as_millis());
    // loop {
        // thread::sleep(Duration::from_secs(1));
    // }
}

fn start_background_thread() {
    thread::spawn(move || {

        let mut archiver = YotsubaArchiver::new("postgresql://postgres:zxc@localhost:5432/archive", "ena_config.json");
        // for board in archiver.get_boards_raw2().iter() {
            // archiver.init_board(board);
        // }
        //archiver.rebase();
        archiver.init_metadata();
        archiver.poll_boards();

        /*loop {
            let j = read_json("73915762.json");
            thread::sleep(Duration::from_secs(2));
        }*/


    /*let mut reader: &[u8] = b"hello";
    let mut reader2 = reader.clone();
    let mut writer: Vec<u8> = vec![];
    println!("reader {:?}", reader);    
    println!("reader2 {:?}", reader2);    
    println!("writer {:?}", writer);    

    std::io::copy(&mut reader, &mut writer).unwrap();

    println!("\nreader {:?}", reader); 
    println!("reader2 {:?}", reader2);    
    println!("writer {:?}", writer);
    */

    }).join().unwrap();
    //let mut handles = vec![];

    // Watch config changes on another thread so it won't interfere with the async scheduler on the other thread
    /*handles.push(std::thread::spawn(move || {
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
                            
                            // Local::now().to_rfc2822()
                            println!("Config modified on {}", Utc.timestamp(since.as_secs() as i64, 0).to_rfc2822().replace("+0000", "GMT"));
                        }
                    } else {
                        //println!("Unchanged");
                    }
                }
                task::sleep(Duration::from_secs(1)).await;
            }
        });
       
    }));*/
    // Do all work in a background thread to keep the main ui thread running smoothly
    // This essentially keeps CPU and power usage extremely low 
    /*handles.push(std::thread::spawn(move || {

        // By using async tasks we can process an enormous amount of tasks concurrently
        // without the overhead of multiple native threads. 
        task::block_on(start_async());

    }));
    for handle in handles {
        handle.join().unwrap();
    }*/
}

// Have a struct to store our variables without using global statics
pub struct YotsubaArchiver {
    conn: Connection,
    settings : serde_json::Value,
    client: reqwest::Client,
    queue: Queue,
    proxies: ProxyStream,
}

impl YotsubaArchiver {

    fn new(connection_url: &str, config_path: &str) -> YotsubaArchiver {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0"));
        //default_headers.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(&yotsuba_time()).unwrap());
        std::fs::create_dir_all("./archive/tmp").unwrap();
        let y = YotsubaArchiver {
                conn: Connection::connect(connection_url, TlsMode::None).expect("Error connecting"),
                settings: read_json(config_path).unwrap(),
                client: reqwest::ClientBuilder::new().default_headers(default_headers).build().unwrap(),
                queue: Queue::new(),
                proxies: Self::get_proxy("cache/proxylist.json"),
            };
        println!("Finished Initializing");
        y
    }

    fn init_metadata(&self) {
        let sql = "CREATE TABLE IF NOT EXISTS metadata
                    (
                        board character varying NOT NULL,
                        threads jsonb,
                        archive jsonb,
                        PRIMARY KEY (board),
                        CONSTRAINT board_unique UNIQUE (board)
                    );";
        self.conn.execute(sql, &[]);
    }

    fn init_board(&self, board: &str) {
        let sql = format!("CREATE TABLE IF NOT EXISTS {board_name}
                    (
                        no bigint NOT NULL,
                        sticky smallint,
                        closed smallint,
                        deleted smallint,
                        now character varying NOT NULL,
                        name character varying NOT NULL DEFAULT 'Anonymous',
                        sub character varying,
                        com character varying,
                        filedeleted smallint,
                        spoiler smallint,
                        custom_spoiler smallint,
                        filename character varying,
                        ext character varying,
                        w int,
                        h int,
                        tn_w int,
                        tn_h int,
                        tim bigint,
                        time bigint NOT NULL,
                        md5 character varying(25),
                        sha256 character varying,
                        sha256t character varying,
                        fsize bigint,
                        m_img smallint,
                        resto int NOT NULL DEFAULT 0,
                        trip character varying,
                        id character varying,
                        capcode character varying,
                        country character varying,
                        country_name character varying,
                        archived smallint,
                        bumplimit smallint,
                        archived_on bigint,
                        imagelimit smallint,
                        semantic_url character varying,
                        replies int,
                        images int,
                        unique_ips bigint,
                        tag character varying,
                        since4pass character varying,
                        PRIMARY KEY (no),
                        CONSTRAINT unique_no UNIQUE (no)
                    )", board_name=board);
        self.conn.execute(&sql, &[]);
        self.conn.execute(&format!("create index on {board_name}(no, resto)", board_name=board), &[]);
        self.conn.execute("set enable_seqscan to off;", &[]);
    }

    /// Initialize database if it didn't exist
    fn init_boards(&self) {
        self.conn.execute("
                    CREATE TABLE IF NOT EXISTS main
                    (
                        id smallint NOT NULL,
                        data jsonb,
                        PRIMARY KEY (id),
                        UNIQUE (id)
                    )
                    ", &[]).unwrap();
        self.conn.execute("
                    CREATE TABLE IF NOT EXISTS media
                    (
                        id bigserial NOT NULL,
                        md5 character varying,
                        sha256 character varying UNIQUE,
                        sha256t character varying UNIQUE,
                        PRIMARY KEY (id),
                        CONSTRAINT media_sha256_sha256t_key UNIQUE (sha256,sha256t)
                    )
                    ", &[]).unwrap();
        let boards_json = read_json("boards.json").unwrap();
        self.conn.execute("INSERT INTO main (id, data) VALUES (1, $1::jsonb) ON CONFLICT (id) DO NOTHING;",
                        &[&boards_json]).unwrap();

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
            println!("Finished Initializing boards");
    }

    /// Get any incomplete threads and enqueue them
    fn rebase(&mut self) {
        let tt = self.get_missing_threads().unwrap();
        for row in tt.iter() {

            let board : String = row.get("board");
            let thread : i32 = row.get("thread");

            if !self.queue.contains_key(&board) {
                self.queue.insert(board.to_owned(), VecDeque::new());
            };

            // TODO Don't add to queue if the config doesn't have that board listed
            let mut q = self.queue.get_mut(board.as_str()).unwrap();
            q.push_back(thread as u32);
        }
    }

    fn upsert_metadata(&self, board: &str, col: &str, json_item: &serde_json::Value) {
        let sql = format!("INSERT INTO metadata(board, {column})
                            VALUES ($1, $2::jsonb)
                            ON CONFLICT (board) DO UPDATE
                                SET {column} = $2::jsonb;", column=col);
        self.conn.execute(&sql, &[&board, &json_item]);

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

    fn poll_boards(&mut self) {
        let boards = self.get_boards_raw();
        for board in boards.iter() {
            if !self.queue.contains_key(board) {
                self.init_board(board);
                self.queue.insert(board.to_owned(), VecDeque::new());
            }
        }
        self.assign_to_board(&boards[0]);
        /*task::block_on(async {
            let mut fut = FuturesUnordered::new();
            let workers = match self.proxies.len() {
                0 => 1,
                w => w,
            };
            let  mut boards  = self.get_boards_raw();
            let mut cycle = boards.to_owned().iter().cycle().to_owned();

            let m = Arc::new(Mutex::new(self));
            for _ in 0..workers {
                let mm = Arc::clone(&m);
                
                // Passing self here is not allowed so we use Self::
                fut.push(Self::tt(cycle.next().unwrap(), mm));
            }
            while let Some(_) = fut.next().await {
            }  
        });*/
        /*let mut fut = FuturesUnordered::new();

        // https://doc.rust-lang.org/book/ch16-03-shared-state.html
        let  boards = self.get_boards_raw();
            fut.push(self.assign_to_board(boards[0]));
            //fut.push(self.assign_to_board(boards[1]));
            //fut.push(self.assign_to_board(boards[2]));

        // Runs all tasks in the list concurrently until completion
        // Post processing after the asynchronous tasks are done
        while let Some(_) = fut.next().await {
        }*/
    }

    fn assign_to_board<'b>(&mut self, board: &'b str) ->Option<()>{
        let current_time = yotsuba_time();
        let mut current_board = String::from(board);
        let mut threads_last_modified = String::from(&current_time);
        let mut archive_last_modified = String::from(&current_time);
        
        let mut threads_json_string = String::new(); 
        // let mut threads_list:VecDeque<u32> = VecDeque::new(); 
        let mut local_list:VecDeque<u32> = VecDeque::new();
        let mut archive_list:Vec<u32> = vec![];
        
        let one_millis = Duration::from_millis(1);
        // let mut count:u32 = 0;
        loop {
            //et mut queue = self.queue.get_mut(&current_board).expect("err getting queue for board"); 
            let now = Instant::now();

            // Download threads.json
            let (last_modified_, status, body) = self.cget(&format!("{url}/{bo}/threads.json", url="http://a.4cdn.org", bo=board), &threads_last_modified);
            match status {
                reqwest::StatusCode::OK => {
                    match last_modified_ {
                        Some(last_modified) => {
                            if threads_last_modified != last_modified {
                                threads_last_modified.clear();
                                threads_last_modified.push_str(&last_modified);
                            }
                        },
                        None => eprintln!("/{}/ <{}> an error has occurred getting the last_modified date", current_board, status),
                    }
                    match body {
                        Ok(new_threads) => {
                            if !new_threads.is_empty() {
                                println!("/{}/ Received new threads on {}", current_board, Local::now().to_rfc2822());
                                let mut fetched_threads : serde_json::Value = serde_json::from_str(&new_threads).expect("Err deserializing new threads");

                                if let Some(_) = self.get_threads_from_metadata(&current_board) {
                                    // compare time modified and get the new threads
                                    // println!("Found threads in metadata..");
                                    if let Some(mut fetched_threads_list) = self.get_deleted_and_modified_threads2(&current_board, &fetched_threads) {
                                        self.queue.get_mut(&current_board).expect("err getting queue for board1").append(&mut fetched_threads_list);
                                    } else {
                                        println!("/{}/ Seems like there was no modified threads..", current_board);
                                    }
                                } else {
                                    // Use fetched_threads 
                                    // println!("DIDNT find threads in metadata..");
                                    if let Some(mut fetched_threads_list) = self.get_threads_list(&fetched_threads) {
                                        self.queue.get_mut(&current_board).expect("err getting queue for board2").append(&mut fetched_threads_list);

                                    } else {
                                        println!("/{}/ Seems like there was no modified threads in the beginning?..", current_board);
                                    }
                                }
                                // Use the new fetched threads as a base
                                self.upsert_metadata(&current_board, "threads", &fetched_threads);
                                /*

                                let mut err = false;
                                if !threads_json_string.is_empty() {
                                    // Get new list using postgres' amazing FULL JOIN functionality lol
                                    // Wtf am I looking at here
                                    if let Ok(resp) = self.get_deleted_and_modified_threads() {
                                        for row in resp.iter() {
                                            let j: serde_json::Value = row.get(0);
                                            if let Ok(mut conv) = serde_json::from_value::<VecDeque<u32>>(j) {
                                                if !conv.is_empty() {
                                                    local_list.append(&mut conv);
                                                } else {
                                                    err = true;
                                                }
                                            } else  {
                                                err = true;
                                            }
                                        }
                                    } else {
                                        err = true
                                    }
                                } else {
                                    err = true
                                }

                                if err {
                                    if let Ok(jj) = serde_json::from_str::<serde_json::Value>(&new_threads) {
                                        match self.create_list_from_threads(jj) {
                                           Ok(ts) => for row in ts.iter() {
                                                let mut sj: serde_json::Value = row.get(0); 
                                                match serde_json::from_value::<VecDeque<u32>>(sj) {
                                                    Ok(mut vv) => local_list.append(&mut vv),
                                                    Err(w) => eprintln!("/{}/ err serializing create_list_from_threads json! {:?}",current_board,w ),
                                                    
                                                }
                                                
                                            },
                                            Err(e) => eprintln!("/{}/ err in create_list_from_threads{:?}",current_board, e),
                                        }
                                    } else {
                                        eprintln!("/{}/ an err happened trying to process local_list",current_board );
                                    }
                                }

                                threads_json_string.clear();
                                threads_json_string.push_str(&new_threads);*/
                            } else {
                                eprintln!("/{}/ <{}> Fetched threads was found to be empty!", current_board, status)
                            }
                        },
                        Err(e) => eprintln!("/{}/ <{}> an error has occurred getting the body\n{}", current_board, status, e),
                    }
                },
                reqwest::StatusCode::NOT_MODIFIED => {
                    eprintln!("/{}/ [threads] <{}>", current_board, status);
                },
                _ => {
                    eprintln!("/{}/ <{}> an error has occurred!", current_board, status);
                },
            }

            thread::sleep(Duration::from_millis(RATELIMIT.into())); // Ratelimit
            //task::sleep(Duration::from_millis(RATELIMIT.into())).await; // Ratelimit


            // Download archive.json
  /*          let has_archives = true;
            if has_archives {
                let (last_modified_, status, body) = self.cget(&format!("{url}/{bo}/archive.json", url="http://a.4cdn.org", bo=board), &archive_last_modified);

                match status {
                    reqwest::StatusCode::OK => {
                         match last_modified_ {
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
                                if let Ok(mut recieved) = serde_json::from_str::<VecDeque<u32>>(&tbody) {
                                    let mut recieved_clone = vec![];
                                    recieved_clone.extend(&recieved);

                                    // Only get the new threads
                                    recieved.retain(|x| !archive_list.contains(x));
                                    local_list.append(&mut recieved);

                                    // Use the new json as base
                                    archive_list = recieved_clone;
                                    println!("/{}/ Received new archive threads on {}", current_board, Local::now().to_rfc2822());
                                    
                                }
                            },
                            Err(e) => eprintln!("/{}/ {:?} an error has occurred getting the body\n{}", current_board, status, e),
                        }
                    },
                    reqwest::StatusCode::NOT_MODIFIED => {
                        eprintln!("/{}/ [archive] <{}>", current_board, status);
                    },
                    _ => {
                        eprintln!("/{}/ <{:?}> an error has occurred", current_board, status);
                    },
                }
            }
            thread::sleep(Duration::from_millis(RATELIMIT.into())); // Ratelimit
*/
            let mut queue = self.queue.get_mut(&current_board).expect("err getting queue for board3");
            if queue.len() > 0 {
                println!("/{}/ Total New threads: {}", current_board, queue.len());
                //let mut q = self.queue.get_mut(board).expect("err getting queue b4 assining to thread");
                //q.append(&mut local_list);
            

                // BRUH I JUST WANT TO SHARE MUTABLE DATA
                // This will loop until it recieves none
                while let Some(a) = self.drain_list(board) {
                }
            }
            // No need to report if no new threads cause when it's not modified it'll tell us
            //else {
                // println!("/{}/ No new threads", current_board);
            // }
            // Enqueue all the new threads we got
            /*let mut arcq = std::sync::Arc::new(std::sync::Mutex::new(self.queue.get_mut(board).unwrap()));
            let ac = Arc::clone(&arcq);
            ac.lock().unwrap().append(&mut local_list);

            if let Ok(mut mutex) = ac.try_lock() {
                while let Some(item) = mutex.pop_front() {
                    let cn = Instant::now();
                    self.assign_to_thread(board, item);
                    while now.elapsed().as_millis() <= RATELIMIT.into() {
                        thread::sleep(one_millis);
                    }
                }
            } else {
                eprintln!("/{}/ Thread lock trying to access queue {}",board, Local::now().to_rfc2822());
            }*/

            // task::sleep(Duration::from_millis(RATELIMIT.into())).await; // Ratelimit

            //local_list.drain(..);
            //archive_list.drain(..);
            // if let Some(mut queue) = self.queue.get_mut(current_board.as_str()) {
                // queue.append(&mut local_list);
            // }


            // This block of code is for workers to help other workers at differenct boards. They will leave their board and switch to another
            // to help another.
            // Blocking calls shouln't be used while the gaurd is still alive. That would cause locks
            /*if let Some(mut map_guard) = queue_arc.try_lock() {

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
            */
            // Ratelimit after fetching threads
            while now.elapsed().as_secs() <= REFRESH_DELAY.into() {
                thread::sleep(one_millis);
                // task::sleep(one_millis).await;
            }
            // count += 1;
            // if count == 5 {
                // break;
            // }
            //println!("/{}/ -> /{}/ Done inside test {:?}", board , current_board, now.elapsed().as_millis());
        }

        //task::sleep(Duration::from_secs(1)).await;
        Some(())
    }

    // BRUH LET ME JUST SHARE MUTABLE REFERENCES AHHHHHHHHHHHHHHH
    fn drain_list(&mut self, board: &str) -> Option<u32> {
        // Repeatedly getting the list probably isn't the most efficient...
        if let Some(a) = self.queue.get_mut(board) {
            let aa = a.pop_front();
            if let Some(thread) = aa {
                // println!("{:?}/{:?} assignde",board ,newt);
                self.assign_to_thread(board, thread);
            }
            return aa;
        }
        None
    }

    // There's a lot of object creation here but they should all get dropped so it shouldn't matter
    fn assign_to_thread(&mut self, board: &str, thread: u32) {
        // only process thread if its incomplete
       // match self.get_missing_thread(board, thread) {
        //    Ok(mts) => {

                // TODO check if thread is empty or its posts are empty, the above SQL just checks their posts
                // If DB has an incomplete thread, archived, closed, or sticky
                let mut retry = 0;
                let mut status_resp = reqwest::StatusCode::OK;
            
                // dl and patch and push to db
                let mut canb=false;
                println!("/{}/{}", board, thread);
                let now = Instant::now();
                let one_millis = Duration::from_millis(1);

                'outer: loop {
                    let (last_modified_, status, body) =
                        self.cget(&format!("{domain}/{bo}/thread/{th}.json", domain="http://a.4cdn.org", bo=board, th=thread ), "");
                    status_resp = status;

                    if let Ok(jb) = body {
                        match serde_json::from_str::<serde_json::Value>(&jb) {
                            Ok(ret) => {
                                self.upsert_thread2(board, &ret);
                                self.upsert_deleteds(board, thread, &ret);
                                
                                /*match self.upsert_thread(board, thread, ret) {
                                    Ok(q) => println!("/{}/{} <{}> Success upserting the thread! {:?}",board, thread, status, q),
                                    Err(e) => eprintln!("/{}/{} <{}> An error occured upserting the thread! {}",board, thread, status, e),
                                } */
                                canb=true;
                                retry=0;
                                break;
                            },
                            Err(e) => {
                                if status == reqwest::StatusCode::NOT_FOUND {
                                    self.upsert_deleted(board, thread);
                                    break;
                                }
                                eprintln!("/{}/{} <{}> An error occured deserializing the json! {}\n{:?}",board, thread, status, e,jb);
                                while now.elapsed().as_millis() <= RATELIMIT.into() {
                                    thread::sleep(one_millis);
                                }
                                retry += 1;
                                if retry <=3 {
                                    continue 'outer;
                                } else {
                                    // TODO handle what to do with invalid thread
                                    retry = 0;
                                    break 'outer;
                                }
                            },
                        }
                        
                    }
                }

                let mut download_media = false;
                let mut download_thumbs = false;
                // DL MEDIA
                // Need to check against other md5 so we don't redownload if we have it
                if download_media || download_thumbs {
                    let media_list = self.conn.query(&format!("select * FROM {board_name} where (no={op} or resto={op}) and (md5 is not null) and (sha256 is null or sha256t is null) order by no", board_name=board, op=thread), &[]).expect("Err getting missing media");
                    let mut fut = FuturesUnordered::new();
                    let client = &self.client;
                    let mut has_media = false;
                    for row in media_list.iter() {
                        has_media = true;
                        let no : i64  = row.get("no");
                        let sha256 : Option<String> = row.get("sha256");
                        let sha256t : Option<String> = row.get("sha256t");
                        let ext : String = row.get("ext");
                        let ext2 : String = row.get("ext");
                        let tim : i64 = row.get("tim");
                        //let j: serde_json::Value = row.get(0);
                        //println!("{:?}", j.get("posts").unwrap().as_array().unwrap().len()); 
                        // println!("{} {:?} {:?}", no, sha256, sha256t);
                        if let Some(_) = sha256 {
                        } else {
                            // No media, proceed to dl
                            if download_media {
                                fut.push(Self::dl_media_post("http://i.4cdn.org", board, thread, tim, ext, no as u64, true, false, client));
                            }
                        }
                        if let Some(_) = sha256t {
                        } else {
                            // No thumbs, proceed to dl
                            if download_thumbs {
                                fut.push(Self::dl_media_post("http://i.4cdn.org", board, thread, tim, ext2, no as u64, false, true, client));
                            }
                        }
                    }
                    if has_media {
                        let s = &self;
                        task::block_on(async move {
                            while let Some(hh) = fut.next().await {
                                if let Some((no, hashsum, thumb_hash)) = hh {
                                    if let Some(hsum) = hashsum {
                                        s.upsert_hash2(board, no, "sha256", &hsum);
                                    }
                                    if let Some(hsumt) = thumb_hash {
                                        s.upsert_hash2(board, no, "sha256t", &hsumt);
                                    }
                                }
                            }
                        });
                    }
                }

                // Get back the thread after it's been patched
                /*if let Ok(mut updated_thread) = self.get_thread_in_db(board, thread) {

                    // Should only return one row, and if there is, go into this block
                    for row in updated_thread.iter() {
                        let mut jj : serde_json::Value = row.get(0);
                        //let thread_id = jj.as_object().expect("Tried to unpack get_thread_in_db from json value to Map").keys().next().unwrap();
                        let mut arr = jj.get_mut("posts").expect("Error getting posts from json")
                                        .as_array_mut().expect("Error getting array from json");

                        // check if media has, md5 -> sha256, sha256t
                        let mut patch_media = false; 
                        let mut fut = FuturesUnordered::new();
                        let iter1 = arr.iter();
                        let client = &self.client;
                        for obj in iter1 {
                            let map = obj.as_object().expect("err get map from obj");
                            if map.contains_key("md5") {
                                let sha = map.contains_key("sha256");
                                let sha_thumb = map.contains_key("sha256t");
                                let tim : u64= serde_json::from_value(map.get("tim").expect("err get tim").to_owned()).expect("err get tim end");

                                let ext :String= serde_json::from_value(map.get("ext").expect("err get ext").to_owned()).expect("err get ext end");
                                let no :u64= serde_json::from_value(map.get("no").expect("err get no").to_owned()).expect("err get no en");
                                // let media_url =  format!("{domain}/{bo}/{tim}{ext}", domain="http://i.4cdn.org", bo=board, tim=1234, ext=".png" );
                                // let thumb_url = format!("{domain}/{bo}/{tim}s.jpg", domain="http://i.4cdn.org", bo=board, tim=1234 );  
                                if !sha || !sha_thumb {
                                    patch_media = true;
                                    fut.push(
                                        Self::dl_media_post("http://i.4cdn.org", board, tim, ext, no, sha, sha_thumb, client)
                                        );
                                }
                            }
                        }


                        if status_resp == reqwest::StatusCode::NOT_FOUND {
                            if_chain! {
                                if let Some(mut object) = arr.get_mut(0);
                                if let Some(mut map) = object.as_object_mut();
                                then {
                                    map.insert("deleted".to_string(), serde_json::json!(1));
                                }
                            }
                        }

                        let upsert_hash_sql = r#"
                                INSERT INTO media (md5, sha256)
                                VALUES
                                   (
                                      $1,
                                      $2
                                   ) 
                                ON CONFLICT (sha256)
                                DO NOTHING;
                        "#;
                        
                        let conn = &self.conn;
                        if patch_media {
                            // TODO collision checks
                            // Push hash to db once a dl completes
                            task::block_on(async move {
                            //let mut iter2 = arr.iter_mut();
                                while let Some(hh) = fut.next().await {
                                    if let Some((no, hashsum, thumb_hash)) = hh {
                                        for i in 0..arr.len() {
                                            if let Some(object) = arr.get_mut(i) {
                                                if let Some(map) = object.as_object_mut() {
                                                    if let Some(num) =  map.get("no") {
                                                        //println!("{:?} {:?}", num, serde_json::json!(no));
                                                        if *num == serde_json::json!(no) {
                                                            if let Some(hs) = &hashsum {
                                                                if !hs.is_empty() {
                                                                    println!("{:?} Received sha256: {}",no, &hs);
                                                                    map.insert("sha256".to_string(), serde_json::json!(hs));
                                                                    if_chain! {
                                                                        if let Some(md5_obj) =  map.get("md5");
                                                                        if let Ok(md5) =  serde_json::from_value::<String>(md5_obj.to_owned());
                                                                        then {
                                                                            conn.execute(&upsert_hash_sql,&[&md5, &hs]);
                                                                        
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            if let Some(hst) = &thumb_hash {
                                                                if !hst.is_empty() {
                                                                    println!("{:?} Received sha256 thumb: {}", no, &hst);
                                                                    // get sha2656 from db
                                                                    // TODO allow seperation of media and thumbs downloading
                                                                    // right now thumbs rely on media downloading
                                                                    
                                                                    map.insert("sha256t".to_string(), serde_json::json!(hst));
                                                                    
                                                                    if_chain! {
                                                                        if let Some(sha_obj) =  map.get("sha256");
                                                                        if let Ok(sha) =  serde_json::from_value::<String>(sha_obj.to_owned());
                                                                        then {
                                                                            if !sha.is_empty() {
                                                                                conn.execute("UPDATE media SET sha256t = $1 WHERE sha256 = $2;",&[&hst, &sha]);
                                                                            }
                                                                        }
                                                                    }

                                                                }
                                                            }
                                                            
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                      //let mut arr = jj.get_mut("posts").expect("Error getting posts from json")
                                        //.as_array_mut().expect("Error getting array from json");
                                        // update obj
                                        //self.upsert_hash();
                                    }
                                }  
                                // Finally upsert the changes
                            });
                            // if status_resp == reqwest::StatusCode::NOT_FOUND {
                                // break;
                            // }
                            self.upsert_thread(board, thread, jj);

                        }


                    }
                }*/

                while now.elapsed().as_millis() <= 1000 {
                    thread::sleep(one_millis);
                }
                // if canb {
                    // break;
                // }
                // end
                
        //    },
        //    Err(e) => println!("error processing thread {:?}", e),
        //}
    }

    // this downloads any missing media and/or thumbs
    async fn dl_media_post(domain:&str, board: &str, thread: u32, tim:i64, ext: String ,no: u64, sha:bool, sha_thumb:bool, cl: &reqwest::Client)  -> Option<(u64, Option<String>, Option<String>)> {
        let dl = |thumb| -> Result<String, reqwest::Error> {
            let url = format!("{}/{}/{}{}{}", domain, board, tim, if thumb {"s"} else {""} , if thumb {".jpg"} else {&ext} );
            println!("/{}/{}#{} -> {}{}{}",  board, thread, no,tim, if thumb {"s"} else {""} , if thumb {".jpg"} else {&ext});
            // TODO Check MD5 & other hashes before DL
            // TODO don't dl if exists, and exists in folder
            // TODO retry here

            // Download and save to file
            let mut resp =cl.get(&url).send()?;
            let status = resp.status();
            let mut hash_str = String::new();
            match status {
                reqwest::StatusCode::OK => {
                    let temp_path = format!("./archive/tmp/{}_{}{}", no,tim,ext);
                    let mut dest = std::fs::File::create(&temp_path).expect("err file temp path");
                    let n = std::io::copy(&mut resp, &mut dest).expect("err file temp path copy");

                    // Open the file we just downloaded, and hash it
                    let mut file = std::fs::File::open(&temp_path).expect("err opening temp file temp path");
                    let mut hasher = Sha256::new();
                    let n = std::io::copy(&mut file, &mut hasher);
                    let hash = hasher.result();

                    // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
                    // /1/0a
                    // Move and rename
                    hash_str.push_str(&format!("{:x}", hash));
                    let path_hash = Path::new(&hash_str);
                    let basename = path_hash.file_stem().expect("err get basename").to_str().expect("err get basename end");
                    let first = &basename[&basename.len()-3..&basename.len()-1];
                    let second = &basename[&basename.len()-1..];
                    let final_dir_path = format!("./archive/media/{}/{}",first, second);
                    let final_path = format!("{}/{:x}{}", final_dir_path, hash, ext);

                    // discarding errors...
                    if let Ok(_) = std::fs::create_dir_all(&final_dir_path){}
                    if let Ok(_) = std::fs::rename(&temp_path, final_path){}
                },
                reqwest::StatusCode::NOT_FOUND => eprintln!("/{}/{} <{}> {}", board, no, status, url),
                _ => eprintln!("/{}/{} <{}> {}", board, no, status, url),
            }
            Ok(hash_str)
        };
        let mut hashsum = None;
        let mut thumb_hash = None;
        if sha {
            if let Ok(a) = dl(false) {
                hashsum = Some(a);
            }
        }

        if sha_thumb {
            if let Ok(a) = dl(true) {
                thumb_hash = Some(a);
            }
        }

        Some((no, hashsum, thumb_hash)
        )

    }

    async fn aget(&self, url: &str, last_modified: &str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
        self.cget(url, last_modified)
    }

    fn cget(&self, url: &str, last_modified: &str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
        let mut res = if last_modified == "" {
            self.client.get(url).send().expect("err cget!")
        } else {
            self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send().expect("err cget")
        };
        let mut last_modified_ : Option<String> = None;
        if_chain! {
            if let Some(head) = res.headers().get(LAST_MODIFIED);
            if let Ok(head_str) = head.to_str();
            then {
                last_modified_ = Some(head_str.to_string());
            }
        }

        let status = res.status();
        let body = res.text();

        (last_modified_, status, body)
    }

    // Insert hash to db
    fn upsert_hash(&self, md5: String, sha256: String, sha256t: String) {
        let sql = r#"
                INSERT INTO media (md5, sha256, sha256t)
                VALUES
                   (
                      $1,
                      $2,
                      $3
                   ) 
                ON CONFLICT (sha256)
                DO NOTHING;
        "#;
        self.conn.execute(&sql, &[&md5, &sha256, &sha256t]);
    }

    // TODO NEED TO BE FIXED
    // This takes a thread and diff & patches it, convert to db schema, upsert to db
    fn upsert_thread(&self, board: &str, thread: u32, json: serde_json::Value) {
        let mut patch = String::from(r#"
        select jsonb_build_object('posts',jsonb_agg(tt))  from (
        SELECT (COALESCE(newv,oldv) || COALESCE(oldv,newv)) as tt from
        (select jsonb_array_elements(data->'boards'->1->'threads'->'{thread}'->'posts') as oldv FROM main where id=3) x
        FULL JOIN
        (select jsonb_array_elements($1::jsonb->'posts') as newv ) z
        ON  oldv->'no' = (newv -> 'no')
            )q"#);
        patch =  patch.replace("{thread}",&thread.to_string());
        let conv = r#"
                SELECT jsonb_build_object(jdb->'posts'->0->>'no',
                                          (jdb ||
                                           jsonb_build_object('last_modified', jdb->'posts'->-1->>'time') ||
                                           jsonb_build_object('no', jdb->'posts'->0->>'no')
                                           ))
                                           from
                (select $1::jsonb as jdb )x
        "#;
        let sql = r#"
            UPDATE main
            set data = jsonb_set(data, '{boards,1,threads}',
                                 data->'boards'->1->'threads' ||
                                 $1::jsonb
                                 , true)
            where id=1;
        "#;
            match self.conn.query(&patch, &[&json]) {

                Ok(head) => for p in head.iter() {
                    let ja:serde_json::Value = p.get(0);
                        match self.conn.query(&conv, &[&ja]) {
                            Ok(shead) => {

                                            for c in shead.iter() {
                                                let pja:serde_json::Value = c.get(0);
                                                
                                                match self.conn.query(&sql, &[&pja]) {
                                                    Ok(expr) => {},
                                                    Err(e) => eprintln!("/{}/{} An error occured upserting the thread! {}",board, thread, e),
                                                }
                                            }
                                            
                                        },
                            Err(e) => eprintln!("/{}/{} An error occured upserting the thread! {}",board, thread, e),
                        }
                },
                Err(e) => eprintln!("/{}/{} An error occured upserting the thread! {}",board, thread, e),
            }
                
            
    }

    fn upsert_hash2(&self, board: &str, no:u64, hash_type: &str, hashsum: &str) {
        let sql = format!("
                    INSERT INTO {board_name}
                    SELECT *
                    FROM {board_name}
                    where no = {no_id}
                    ON CONFLICT (no) DO UPDATE
                        SET {htype} = $1", board_name=board, no_id=no, htype=hash_type);
        self.conn.execute(&sql, &[&hashsum]);
    }

    // Single upsert
    fn upsert_deleted(&self, board: &str, no:u32) {
        let sql = format!("
                    INSERT INTO {board_name}
                    SELECT *
                    FROM {board_name}
                    where no = {no_id}
                    ON CONFLICT (no) DO UPDATE
                        SET deleted = 1", board_name=board, no_id=no);
        self.conn.execute(&sql, &[]);
    }
    fn upsert_deleteds(&self, board: &str, thread:u32, json_item: &serde_json::Value) {
        let sql = format!("
                        
                        insert into {board_name}
                            SELECT x.* from
                            (select * FROM {board_name} where no={op} or resto={op} order by no) x
                            FULL JOIN
                            (select * FROM jsonb_populate_recordset(null::{board_name}, $1::jsonb->'posts')) z
                            ON  x.no  = z.no
                            where z.no is null
                        ON CONFLICT (no) 
                        DO
                            UPDATE 
                            SET deleted = 1;", board_name=board, op=thread);
        self.conn.execute(&sql, &[&json_item]);
    }
    fn upsert_thread2(&self, board: &str, json_item: &serde_json::Value) {
        let sql = format!("
                        
                        insert into {board_name}
                            select * from jsonb_populate_recordset(null::{board_name}, $1::jsonb->'posts')
                        ON CONFLICT (no) 
                        DO
                            UPDATE 
                            SET 
                                sticky = excluded.sticky,
                                closed = excluded.closed,
                                now = excluded.now,
                                name = excluded.name,
                                sub = excluded.sub,
                                com = excluded.com,
                                filedeleted = excluded.filedeleted,
                                spoiler = excluded.spoiler,
                                custom_spoiler = excluded.custom_spoiler,
                                filename = excluded.filename,
                                ext = excluded.ext,
                                w = excluded.w,
                                h = excluded.h,
                                tn_w = excluded.tn_w,
                                tn_h = excluded.tn_h,
                                tim = excluded.tim,
                                time = excluded.time,
                                md5 = excluded.md5,
                                fsize = excluded.fsize,
                                m_img = excluded.m_img,
                                resto = excluded.resto,
                                trip = excluded.trip,
                                id = excluded.id,
                                capcode = excluded.capcode,
                                country = excluded.country,
                                country_name = excluded.country_name,
                                archived = excluded.archived,
                                bumplimit = excluded.bumplimit,
                                archived_on = excluded.archived_on,
                                imagelimit = excluded.imagelimit,
                                semantic_url = excluded.semantic_url,
                                replies = excluded.replies,
                                images = excluded.images,
                                unique_ips = excluded.unique_ips,
                                tag = excluded.tag,
                                since4pass = excluded.since4pass;", board_name=board);
        self.conn.execute(&sql, &[&json_item]);
    }
    fn get_threads_list(&self, json_item: &serde_json::Value) -> Option<VecDeque<u32>> {
        let sql = "SELECT jsonb_agg(newv->'no') from
                    (select jsonb_path_query($1::jsonb, '$[*].threads[*]') as newv)z";
        let resp = self.conn.query(&sql, &[&json_item]).expect("Error getting modified and deleted threads from new threads.json");
        let mut result : Option<VecDeque<u32>> = None;
        for row in resp.iter() {
            let q :VecDeque<u32> = serde_json::from_value(row.get(0)).expect("Err deserializing get_threads_list");
            result = Some(q);
        }
        result
    }

    // Use this and not the one below so no deserialization happens and no overhead
    fn get_board_from_metadata(&self, board: &str) -> Option<String> {
        let resp = self.conn.query("select * from metadata where board = $1", &[&board]).expect("Err getting threads from metadata");
        let mut board_ : Option<String> = None;
        for row in resp.iter() {
            board_ = row.get("board");
            //let j: serde_json::Value = row.get(0);
            //println!("{:?}", j.get("posts").unwrap().as_array().unwrap().len()); 
            // println!("{} {:?} {:?}", no, sha256, sha256t);
        }
        board_
    }
    fn get_threads_from_metadata(&self, board: &str) -> Option<serde_json::Value> {
        let resp = self.conn.query("select * from metadata where board = $1", &[&board]).expect("Err getting threads from metadata");
        let mut threads : Option<serde_json::Value> = None;
        for row in resp.iter() {
            threads = row.get("threads");
            //let j: serde_json::Value = row.get(0);
            //println!("{:?}", j.get("posts").unwrap().as_array().unwrap().len()); 
            // println!("{} {:?} {:?}", no, sha256, sha256t);
        }
        threads
    }

    fn get_thread_in_db(&self, board: &str, thread: u32)  -> Result<postgres::rows::Rows, postgres::error::Error> {
        let sql = format!(r#"
                SELECT jsonb_path_query(jdb, '$.boards[*] ? (@.board == "{bo}") .threads."{th}"') from
                (select data as jdb from main where id=1 )x
        "#, bo=board, th=thread);
        self.conn.query(&sql, &[])
    }

    fn convert_thread_to_internal_thread() {
        let sql = r#"
                SELECT jsonb_build_object(jdb->'posts'->0->>'no',
                                          (jdb ||
                                           jsonb_build_object('last_modified', jdb->'posts'->-1->>'time') ||
                                           jsonb_build_object('no', jdb->'posts'->0->>'no')
                                           ))
                                           from
                (select data as jdb from main where id=7 )x
        "#;
    }

    fn create_list_from_threads(&self, json: serde_json::Value) -> Result<postgres::rows::Rows, postgres::error::Error> {
        let sql = r#"
                SELECT jsonb_agg(jdb) from
                (select jsonb_path_query($1::jsonb, '$[*].threads[*].no') as jdb )x
        "#;
        self.conn.query(&sql, &[&json])
    }

    fn get_deleted_and_modified_threads2(&self, board: &str, new_threads: &serde_json::Value) -> Option<VecDeque<u32>> {
        // Combine new and prev threads.json into one. This retains the prev threads (which the new json doesn't contain, meaning they're either pruned or archived).
        //  That's especially useful for boards without archives.
        // Use the WHERE clause to select only modified threads. Now we basically have a list of deleted and modified threads.
        // Return back this list to be processed.
        // Use the new threads.json as the base now.
        let sql = r#"
                SELECT jsonb_agg(COALESCE(newv,prev)->'no') from
                (select jsonb_path_query(threads, '$[*].threads[*]') as prev from metadata where board = $1)x
                full JOIN
                (select jsonb_path_query($2::jsonb, '$[*].threads[*]') as newv)z
                ON prev->'no' = (newv -> 'no') 
                where newv is null or not prev->'last_modified' <@ (newv -> 'last_modified')
                "#;
        let resp = self.conn.query(&sql, &[&board, &new_threads]).expect("Error getting modified and deleted threads from new threads.json");
        let mut result : Option<VecDeque<u32>> = None;
        for row in resp.iter() {
            let q :VecDeque<u32> = serde_json::from_value(row.get(0)).expect("Err deserializing get_deleted_and_modified_threads2");
            result = Some(q);
        }
        result
    }

    fn get_deleted_and_modified_threads(&self) -> Result<postgres::rows::Rows, postgres::error::Error> {
        // Combine new and prev threads.json into one. This retains the prev threads (which the new json doesn't contain, meaning they're either pruned or archived).
        //  That's especially useful for boards without archives.
        // Use the WHERE clause to select only modified threads. Now we basically have a list of deleted and modified threads.
        // Return back this list to be processed.
        // Use the new threads.json as the base now.
        let sql = r#"
            SELECT jsonb_agg(COALESCE(newv,prev)->'last_modified') from
            (select jsonb_path_query(data, '$[*].threads[*]') as prev from main where id = 5)x
            full JOIN
            (select jsonb_path_query(data, '$[*].threads[*]') as newv from main where id = 6)z
            ON prev->'no' = (newv -> 'no') 
            where newv is null or not prev->'last_modified' <@ (newv -> 'last_modified')
        "#;
        self.conn.query(&sql, &[])
    }

    fn patch_posts() {
        // FULL JOIN to get missing posts a new thread may have deleted
        // Combine into one with COALESCE
        // Another COALESCE to combine them into one but this one is baseing data from prev
        // Then concat the NEW with PREV so we preserve hashsums from our PREV
        let sql = r#"
        SELECT COALESCE(newv,oldv) || COALESCE(oldv,newv) from
        (select jsonb_array_elements(data->'posts') as oldv FROM main where id=3) x
        FULL JOIN
        (select jsonb_array_elements(data->'posts') as newv FROM main where id=4) z
        ON  oldv->'no' = (newv -> 'no')"#;
    }

    fn get_missing_media_post() {
        let sql = r#"
            select key::int as thread,posts as post
            from main t
                 , jsonb_path_query(data, '$.boards[*] ? (@.board=="3") .threads') element
                 , jsonb_each(element) first_currency
                 , jsonb_path_query(element, '$.*.posts[*]' ) as posts
            where id=1 and key::int=1234 and (posts ? 'md5' and (not posts ? 'sha256' or not posts ? 'sha256t'))
        "#;
    }

    fn get_missing_threads(&self) -> Result<postgres::rows::Rows, postgres::error::Error> {
        // Deleted key is not always present, only on 404
        // Archived key is always present when a thread is finished
        let sql = r#"
                select distinct thread, board from (
                select bstr #>> '{}' as board, key::int AS thread, posts as post 
                from main t
                     , jsonb_path_query(data, '$.boards[*]') boardsorig
                     , jsonb_path_query(boardsorig, '$.board') bstr
                     , jsonb_path_query(boardsorig, '$.threads') element
                     , jsonb_each(element) first_currency
                     , jsonb_path_query(element, '$.*.posts[*]' ) as posts
                where id=1  and ((posts @> '{"resto":0}' and 
                                ((posts @> '{"deleted":0}') or
                                 (posts @> '{"sticky":0}') or
                                 (posts @> '{"closed":0}'))
                                ) or 
                                (posts ? 'md5' and (not posts ? 'sha256' or not posts ? 'sha256t'))
                                )
                )x
        "#;
        self.conn.query(&sql, &[])       
    }

    fn get_missing_thread(&self, board: &str, thread: u32) -> Result<postgres::rows::Rows, postgres::error::Error> {
        let sql = self.get_missing_base_query(Some(board), Some(thread));
        self.conn.query(&sql, &[])
    }
    
    fn get_missing_base_query(&self, board: Option<&str>, thread: Option<u32>) -> String {
        let mut query = String::from(r#"
                select key::int as thread,posts as post
                from main t
                     , jsonb_path_query(data, '$.boards[*]{board_specifier}.threads') element
                     , jsonb_each(element) first_currency
                     , jsonb_path_query(element, '$.*.posts[*]' ) as posts
                where id=1 {thread_specifier} and ((posts @> '{"resto":0}' and 
                                ((posts @> '{"deleted":0}') or
                                 (posts @> '{"sticky":0}') or
                                 (posts @> '{"closed":0}'))
                                ) or 
                                (posts ? 'md5' and (not posts ? 'sha256' or not posts ? 'sha256t'))
                                )
        "#);
        let mut board_str = String::new();
        let mut thread_str = String::new();
        query = query.replace("{board_specifier}",
                if let Some(b) = board {
                    board_str.push_str(format!(r#" ? (@.board=="{}") "#, b).as_str());
                    &board_str
                } else {
                    ""
                }
            ).replace("{thread_specifier}",
                if let Some(th) = thread {
                    thread_str.push_str(format!("and key::int={}", th).as_str());
                    &thread_str
                } else {
                    ""
                }
            );
        query
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

    fn get_boards_raw(&self) -> Vec<String> {
        self.settings.get("sites").unwrap()
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
    
    fn get_boards_raw2(&self) -> Vec<String> {
        let s = read_json("ena_config.json").unwrap();
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

    fn get_proxy(proxy_path: &str) -> ProxyStream {
        if let Some(p) = read_json(proxy_path) {
            let mut ps = ProxyStream::new();
            ps.urls.append(
            &mut serde_json::from_value::<VecDeque<String>>(p).unwrap());
            ps
        } else {
            ProxyStream::new()
        }
    }


}

type Queue = BTreeMap<String, VecDeque<u32>>;

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

fn read_json(path: &str) -> Option<serde_json::Value>{
    
    if let Ok(file) = File::open(path) {
        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(s) => Some(s),
            Err(e) => None,
        }
    } else {
        None
    }
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

fn yotsuba_time() -> String {
    chrono::Utc::now().to_rfc2822().replace("+0000", "GMT")
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


fn get<H: Into<Option<HeaderMap>>>(url: &str, headers: H) -> Result<reqwest::Response, reqwest::Error> {
    get_custom(url, headers.into(), None, None, None)
}

// https://hoverbear.org/blog/optional-arguments/
// https://doc.rust-lang.org/std/convert/trait.Into.html
// GET requests with proxies are built from client level, not from get methods.
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
