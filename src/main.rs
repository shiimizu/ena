#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(non_snake_case)]
#![allow(unused_imports)]

mod sql; 

extern crate reqwest;
extern crate pretty_env_logger;
extern crate rand;
#[macro_use] extern crate log;

#[macro_use] extern crate if_chain;

extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;

use futures::stream::StreamExt as FutureStreamExt;
use futures::stream::FuturesUnordered;
use std::time::{Duration, Instant};
use postgres::{Connection, TlsMode};
use reqwest::header::{HeaderMap, HeaderValue, LAST_MODIFIED, USER_AGENT, IF_MODIFIED_SINCE};

use std::collections::VecDeque;
use chrono::Local;
use sha2::{Sha256, Digest};
//use async_std::prelude::*;
use async_std::task;
use std::io::BufReader;
use std::fs::File;

use std::path::Path;

extern crate ctrlc;

fn main() {
    println!(r#"
    ⡿⠋⠄⣀⣀⣤⣴⣶⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⣌⠻⣿⣿
    ⣴⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⠹⣿
    ⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⠹     
    ⣿⣿⡟⢹⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡛⢿⣿⣿⣿⣮⠛⣿⣿⣿⣿⣿⣿⡆       ____           
    ⡟⢻⡇⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣣⠄⡀⢬⣭⣻⣷⡌⢿⣿⣿⣿⣿⣿      /\  _`\     
    ⠃⣸⡀⠈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⠈⣆⢹⣿⣿⣿⡈⢿⣿⣿⣿⣿      \ \ \L\_     ___      __     
    ⠄⢻⡇⠄⢛⣛⣻⣿⣿⣿⣿⣿⣿⣿⣿⡆⠹⣿⣆⠸⣆⠙⠛⠛⠃⠘⣿⣿⣿⣿       \ \  __\  /' _ `\  /'__`\   
    ⠄⠸⣡⠄⡈⣿⣿⣿⣿⣿⣿⣿⣿⠿⠟⠁⣠⣉⣤⣴⣿⣿⠿⠿⠿⡇⢸⣿⣿⣿        \ \ \___\/\ \/\ \/\ \L\.\_ 
    ⠄⡄⢿⣆⠰⡘⢿⣿⠿⢛⣉⣥⣴⣶⣿⣿⣿⣿⣻⠟⣉⣤⣶⣶⣾⣿⡄⣿⡿⢸         \ \____/\ \_\ \_\ \__/.\_\
    ⠄⢰⠸⣿⠄⢳⣠⣤⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⣼⣿⣿⣿⣿⣿⣿⡇⢻⡇⢸          \/___/  \/_/\/_/\/__/\/_/   v{version}
    ⢷⡈⢣⣡⣶⠿⠟⠛⠓⣚⣻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣇⢸⠇⠘     
    ⡀⣌⠄⠻⣧⣴⣾⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠛⠛⠛⢿⣿⣿⣿⣿⣿⡟⠘⠄⠄   
    ⣷⡘⣷⡀⠘⣿⣿⣿⣿⣿⣿⣿⣿⡋⢀⣠⣤⣶⣶⣾⡆⣿⣿⣿⠟⠁⠄⠄⠄⠄     An ultra lightweight 4chan archiver (¬ ‿ ¬ )
    ⣿⣷⡘⣿⡀⢻⣿⣿⣿⣿⣿⣿⣿⣧⠸⣿⣿⣿⣿⣿⣷⡿⠟⠉⠄⠄⠄⠄⡄⢀
    ⣿⣿⣷⡈⢷⡀⠙⠛⠻⠿⠿⠿⠿⠿⠷⠾⠿⠟⣛⣋⣥⣶⣄⠄⢀⣄⠹⣦⢹⣿
    "#, version=option_env!("CARGO_PKG_VERSION").unwrap_or("Unknown"));

    let start_time_str = Local::now().to_rfc2822();
    pretty_env_logger::init();
    start_background_thread();
    println!("\nStarted on:\t{}\nFinished on:\t{}", start_time_str, Local::now().to_rfc2822());
}

fn start_background_thread() {
        task::block_on(async{
            let archiver = YotsubaArchiver::new();
            archiver.listen_to_exit();
            archiver.init_type();
            archiver.init_schema();
            archiver.init_metadata();
            std::thread::sleep(Duration::from_millis(1200));

            let a = &archiver;
            let mut fut = FuturesUnordered::new();
            /*let asagiCompat = if let Some(set) =  a.settings.get("settings").expect("Err get settings").get("asagiCompat") {
                set.as_bool().expect("err deserializing asagiCompat to bool")
            } else {
                false
            };*/

            // Push each board to queue to be run concurrently
            let mut config = a.settings.to_owned();
            let bb = config.to_owned();
            let boards = bb.get("boards").expect("Err getting boards").as_array().expect("Err getting boards as array");
            for board in boards {
                let default = config.get_mut("boardSettings").expect("Err getting boardSettings").as_object_mut().expect("Err boardSettings as_object_mut");
                let board_map = board.as_object().expect("Err serializing board");
                for (k,v) in board_map.iter() {
                    default.insert(k.to_string(), v.to_owned()
                        /*if asagiCompat && k == "board" {
                            serde_json::json!(v.as_str().expect("Err deserializing v from k,v").to_string() + "_ena")
                        } else {
                            v.to_owned()
                        }*/
                    );
                }
                let bs : BoardSettings2 = serde_json::from_value(serde_json::to_value(default.to_owned()).expect("Error serializing default")).expect("Error deserializing default");
                fut.push(a.assign_to_board(bs));
            }

            // Run each board concurrently
            while let Some(_) = fut.next().await {}
        });
        
}

// Have a struct to store our variables without using global statics
pub struct YotsubaArchiver {
    conn: Connection,
    settings : serde_json::Value,
    client: reqwest::Client,
    schema: String,
    finished: async_std::sync::Arc<async_std::sync::Mutex::<bool>>,
}

impl YotsubaArchiver {

    fn new() -> YotsubaArchiver {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0"));
        let settingss = read_json("ena_config.json").expect("Err get config");
        
        let defs = settingss.get("settings").expect("Err get settings");
        let ps = defs.get("path").expect("err getting path").as_str().expect("err converting path to str");
        let p = ps.trim_end_matches('/').trim_end_matches('\\').to_string();
        std::fs::create_dir_all(p + "/tmp").expect("Err create dir tmp");

        let schema = if let Some(set) =  &defs.get("schema") {
            set.as_str().expect("err deserializing schema to str").to_string()
        } else {
            "public".to_string()
        };

        let conn_url = format!(
            "postgresql://{username}:{password}@{host}:{port}/{database}",
            username=defs.get("username").expect("Err get username").as_str().expect("Err convert username to str"),
            password=defs.get("password").expect("Err get password").as_str().expect("Err convert password to str"),
            host=defs.get("host").expect("Err get localhost").as_str().expect("Err convert host to str"),
            port=defs.get("port").expect("Err get port"),
            database=defs.get("database").expect("Err get database").as_str().expect("Err convert database to str"));

        YotsubaArchiver {
            conn: Connection::connect(conn_url, TlsMode::None).expect("Error connecting"),
            settings: settingss,
            client: reqwest::ClientBuilder::new().default_headers(default_headers).build().unwrap(),
            schema: schema,
            finished: async_std::sync::Arc::new(async_std::sync::Mutex::new(false)),
        }
    }

    fn init_type(&self) {
        self.conn.execute(&sql::init_type(), &[]).expect(&format!("Err creating 4chan schema type"));
    }

    fn init_schema(&self) {
        self.conn.execute(&sql::init_schema(&self.schema), &[]).expect(&format!("Err creating schema: {}", &self.schema));
    }

    fn init_metadata(&self) {
        self.conn.batch_execute(&sql::init_metadata(&self.schema)).expect("Err creating metadata");
    }

    fn init_board(&self, board: &str) {
        self.conn.batch_execute(&sql::init_board(&self.schema, board)).expect(&format!("Err creating schema: {}", board));
    }

    fn init_views(&self, board: &str) {
        self.conn.batch_execute(&sql::init_views(&self.schema, board)).expect("Err create views");
        // self.conn.batch_execute(&sql::create_asagi_tables(&self.schema, board)).expect("Err initializing trigger functions");
        // self.conn.batch_execute(&sql::create_asagi_triggers(&self.schema, board, &(board.to_owned() + "_ena") )).expect("Err initializing trigger functions");
    }

    fn listen_to_exit(&self) {
        // let f = async_std::sync::Arc::new(async_std::sync::Mutex::new(self.finished));
        let finished_clone = async_std::sync::Arc::clone(&self.finished);
        ctrlc::set_handler( move || {
                if let Some(mut finished) = finished_clone.try_lock() {
                    *finished = true;
                } else {
                    eprintln!("Lock occurred trying to get finished in ctrlc::set_handler");
                }
        }).expect("Error setting Ctrl-C handler");
    }

    fn upsert_metadata(&self, board: &str, col: &str, json_item: &serde_json::Value) {
        self.conn.execute(&sql::upsert_metadata(&self.schema, col), &[&board, &json_item]).expect("Err executing sql: upsert_metadata");
    }

    fn get_media_posts(&self, board: &str, thread:u32) -> Result<postgres::rows::Rows, postgres::error::Error> {
        self.conn.query(&sql::media_posts(&self.schema, board, thread), &[])
    }

    fn upsert_hash2(&self, board: &str, no: u64, hash_type: &str, hashsum: Vec<u8>) {
        self.conn.execute(&sql::upsert_hash(&self.schema, board, no, hash_type), &[&hashsum]).expect("Err executing sql: upsert_hash2");
    }

    // Mark a single post for deletion
    fn upsert_deleted(&self, board: &str, no:u32) {
        self.conn.execute(&sql::upsert_deleted(&self.schema, board, no), &[]).expect("Err executing sql: upsert_deleted");
    }
    
    // Mark posts where it's deleted 
    fn upsert_deleteds(&self, board: &str, thread:u32, json_item: &serde_json::Value) {
        self.conn.execute(&sql::upsert_deleteds(&self.schema, board, thread), &[&json_item]).expect("Err executing sql: upsert_deleteds");
    }
    
    fn upsert_thread(&self, board: &str, json_item: &serde_json::Value) {
        // This method inserts a post or updates an existing one.
        // It only updates rows where there's a column change. A majority of posts in a thread don't change. This saves IO writes. 
        // (It doesn't modify/update sha256, sha25t, or deleted. Those are manually done)
        // https://stackoverflow.com/a/36406023
        // https://dba.stackexchange.com/a/39821
        // println!("{}", serde_json::to_string(json_item).unwrap());
        //
        // md5 -> 4chan for some reason inserts a backslash
        // https://stackoverflow.com/a/11449627
        self.conn.execute(&sql::upsert_thread(&self.schema, board), &[&json_item]).expect("Err executing sql: upsert_thread");
    }
    
    fn check_metadata_col(&self, board: &str, col:&str) -> bool {
        let resp = self.conn.query(&sql::check_metadata_col(&self.schema, col), &[&board]).expect("Err query check_metadata_col");
        let mut res = false;
        for row in resp.iter() {
            let ret : Option<bool> = row.get(0);
            if let Some(r) = ret {
                res = r;
            } // else it's null, meaning false
        }
        res
    }

    async fn get_threads_list(&self, json_item: &serde_json::Value) -> Option<VecDeque<u32>> {
        let resp = self.conn.query(&sql::threads_list(), &[&json_item]).expect("Error getting modified and deleted threads from new threads.json in get_threads_list");
        let mut _result : Option<VecDeque<u32>> = None;
        'outer: for _ri in 0..4 {
            for row in resp.iter() {
                let jsonb : Option<serde_json::Value> = row.get(0);
                match jsonb {
                    Some(val) => {
                        let q :VecDeque<u32> = serde_json::from_value(val).expect("Err deserializing get_threads_list");
                        _result = Some(q);
                        break 'outer;
                    },
                    None => {
                        eprintln!("Error getting get_threads_list at column 0: NULL @ {}", Local::now().to_rfc2822());
                        task::sleep(Duration::from_secs(1)).await;
                    },
                }
            }
        }
        _result
    }

    async fn get_combined_threads(&self, board: &str, new_threads: &serde_json::Value, is_threads: bool) -> Option<VecDeque<u32>> {
        // This query is only run ONCE at every startup
        // Running a JOIN to compare against the entire DB on every INSERT/UPDATE would not be that great. 
        // This gets all the threads from cache, compares it to the new json to get new + modified threads
        // Then compares that result to the database where a thread is deleted or archived, and takes only the threads where's it's not
        // deleted or archived
        let resp = self.conn.query(&sql::combined_threads(&self.schema, board, is_threads), &[&board, &new_threads]).expect("Error getting modified and deleted threads from new threads.json in get_combined_threads");
        let mut _result : Option<VecDeque<u32>> = None;
        'outer: for _ri in 0..4 {
            for row in resp.iter() {
                let jsonb : Option<serde_json::Value> = row.get(0);
                match jsonb {
                    Some(val) => {
                        let q :VecDeque<u32> = serde_json::from_value(val).expect("Err deserializing get_combined_threads");
                        _result = Some(q);
                        break 'outer;
                    },
                    None => {
                        // Null from query -> no new threads
                        //eprintln!("Error getting get_combined_threads at column 0: NULL @ {}", Local::now().to_rfc2822());
                        task::sleep(Duration::from_secs(1)).await;
                    },
                }
            }
        }
        _result
    }

    async fn get_deleted_and_modified_threads2(&self, board: &str, new_threads: &serde_json::Value, is_threads: bool) -> Option<VecDeque<u32>> {
        // Combine new and prev threads.json into one. This retains the prev threads (which the new json doesn't contain, meaning they're either pruned or archived).
        //  That's especially useful for boards without archives.
        // Use the WHERE clause to select only modified threads. Now we basically have a list of deleted and modified threads.
        // Return back this list to be processed.
        // Use the new threads.json as the base now.
        let resp = self.conn.query(&sql::deleted_and_modified_threads(&self.schema, is_threads), &[&board, &new_threads]).expect("Error getting modified and deleted threads from new threads.json");
        let mut _result : Option<VecDeque<u32>> = None;
        'outer: for _ri in 0..4 {
            for row in resp.iter() {
                let jsonb : Option<serde_json::Value> = row.get(0);
                match jsonb {
                    Some(val) => {
                        let q = if let Ok(iv) = serde_json::from_value::<i64>(val.to_owned()) {
                            let mut vd = VecDeque::new();
                            vd.push_back(iv as u32);
                            vd
                        } else if let Ok(v) = serde_json::from_value::<VecDeque<u32>>(val) {
                            v
                        } else {
                            break 'outer;
                        };
                        //let q :VecDeque<u32> = serde_json::from_value(val).expect("Err deserializing get_deleted_and_modified_threads2");
                        _result = Some(q);
                        break 'outer;
                    },
                    None => {
                        eprintln!("Error getting get_deleted_and_modified_threads2 at column 0: NULL @ {}", Local::now().to_rfc2822());
                        task::sleep(Duration::from_secs(1)).await;
                    },
                }
            }
        }
        _result
    }

    // Manages a single board
    async fn assign_to_board(&self, bs: BoardSettings2) -> Option<()> {
        let current_board = &bs.board;//.replace("_ena", "");
        self.init_board(&bs.board);
        self.init_views(current_board);
        /*if bs.board.contains("_ena") {
            self.init_views(current_board);
        }*/
        let _one_millis = Duration::from_millis(1);
        let mut threads_last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
        let mut archives_last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
        let mut local_threads_list : VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut update_metadata_archive = false;
        let mut init = true;
        let mut init_archive = true;
        let mut has_archives = true;
        // let mut count:u32 = 0;
        let ps = self.settings.get("settings").expect("Err get settings").get("path").expect("err getting path").as_str().expect("err converting path to str");
        let p = ps.trim_end_matches('/').trim_end_matches('\\');

        loop {
            // Listen to CTRL-C
            if let Some(finished) = self.finished.try_lock() {
                if *finished {
                    return Some(());
                }
            } else {
                eprintln!("Lock occurred trying to get finished in assign_to_board");
            }
            //et mut queue = self.queue.get_mut(&current_board).expect("err getting queue for board"); 
            let now = Instant::now();

            // Download threads.json
            // Scope to drop values when done
            let mut fetched_threads : Option<serde_json::Value> = None;
            {
                let (last_modified_, status, body) = self.cget(&format!("{url}/{bo}/threads.json", url=bs.api_url, bo=current_board), &threads_last_modified).await;
                match status {
                    reqwest::StatusCode::OK => {
                        match last_modified_ {
                            Some(last_modified) => {
                                if threads_last_modified != last_modified {
                                    threads_last_modified.clear();
                                    threads_last_modified.push_str(&last_modified);
                                }
                            },
                            None => eprintln!("/{}/ [threads] <{}> An error has occurred getting the last_modified date", current_board, status),
                        }
                        match body {
                            Ok(new_threads) => {
                                if !new_threads.is_empty() {
                                    println!("/{}/ [threads] Received new threads on {}", current_board, Local::now().to_rfc2822());
                                    fetched_threads = Some(serde_json::from_str::<serde_json::Value>(&new_threads).expect("Err deserializing new threads"));
                                    let ft = fetched_threads.to_owned().unwrap();

                                    if self.check_metadata_col(&current_board, "threads") {
                                        // if there's cache
                                        // if this is a first startup
                                        if init {
                                            // going here means the program was restarted
                                            // use combination of all threads from cache + new threads (excluding archived, deleted, and duplicate threads)
                                            if let Some(mut fetched_threads_list) = self.get_combined_threads(&current_board, &ft, true).await {
                                                local_threads_list.append(&mut fetched_threads_list);
                                            } else {
                                                println!("/{}/ [threads] Seems like there was no modified threads at startup..", current_board);
                                            }

                                            // update base at the end
                                            update_metadata = true;
                                            init = false;
                                        } else {
                                            // here is when we have cache and the program in continously running
                                            // only get new/modified/deleted threads
                                            // compare time modified and get the new threads
                                            if let Some(mut fetched_threads_list) = self.get_deleted_and_modified_threads2(&current_board, &ft, true).await {
                                                local_threads_list.append(&mut fetched_threads_list);
                                            } else {
                                                println!("/{}/ [threads] Seems like there was no modified threads..", current_board);
                                            }

                                            // update base at the end
                                            update_metadata = true;
                                        }

                                    } else {
                                        // No cache
                                        // Use fetched_threads 
                                        if let Some(mut fetched_threads_list) = self.get_threads_list(&ft).await {
                                            // self.queue.get_mut(&current_board).expect("err getting queue for board2").append(&mut fetched_threads_list);
                                            local_threads_list.append(&mut fetched_threads_list);
                                            self.upsert_metadata(&current_board, "threads", &ft);
                                            init = false;
                                            update_metadata = false;
                                        } else {
                                            println!("/{}/ [threads] Seems like there was no modified threads in the beginning?..", current_board);
                                        }
                                    }
                                    

                                    
                                } else {
                                    eprintln!("/{}/ [threads] <{}> Fetched threads was found to be empty!", current_board, status)
                                }
                            },
                            Err(e) => eprintln!("/{}/ [threads] <{}> An error has occurred getting the body\n{}", current_board, status, e),
                        }
                    },
                    reqwest::StatusCode::NOT_MODIFIED => {
                        eprintln!("/{}/ [threads] <{}>", current_board, status);
                    },
                    _ => {
                        eprintln!("/{}/ [threads] <{}> An error has occurred!", current_board, status);
                    },
                }

                task::sleep(Duration::from_millis(bs.throttle_millisec.into())).await; // Ratelimit
            }

            // Download archive.json
            // Scope to drop values when done
            // Am I really copy pasting the code above....
            let mut fetched_archive_threads : Option<serde_json::Value> = None;
            {
                if has_archives {
                    for rti in 0..=bs.retry_attempts {
                        let (last_modified_, status, body) = self.cget(&format!("{url}/{bo}/archive.json", url=bs.api_url, bo=current_board), &archives_last_modified).await;
                        match status {
                            reqwest::StatusCode::OK => {
                                match last_modified_ {
                                    Some(last_modified) => {
                                        if archives_last_modified != last_modified {
                                            archives_last_modified.clear();
                                            archives_last_modified.push_str(&last_modified);
                                        }
                                    },
                                    None => eprintln!("/{}/ [archive] <{}> An error has occurred getting the last_modified date", current_board, status),
                                }
                                match body {
                                    Ok(new_threads) => {
                                        if !new_threads.is_empty() {
                                            println!("/{}/ [archive] Received new threads on {}", current_board, Local::now().to_rfc2822());
                                            fetched_archive_threads = Some(serde_json::from_str::<serde_json::Value>(&new_threads).expect("Err deserializing new threads"));
                                            let ft = fetched_archive_threads.to_owned().unwrap();

                                            if self.check_metadata_col(&current_board, "archive") {
                                                // if there's cache
                                                // if this is a first startup
                                                if init_archive {
                                                    // going here means the program was restarted
                                                    // use combination of all threads from cache + new threads (excluding archived, deleted, and duplicate threads)
                                                    if let Some(mut list) = self.get_combined_threads(&current_board, &ft, false).await {
                                                        local_threads_list.append(&mut list);
                                                    } else {
                                                        println!("/{}/ [archive] Seems like there was no modified threads from startup..", current_board);
                                                    }

                                                    // update base at the end
                                                    update_metadata_archive = true;
                                                    init_archive = false;
                                                } else {
                                                    // here is when we have cache and the program in continously running
                                                    // only get new/modified/deleted threads
                                                    // compare time modified and get the new threads
                                                    if let Some(mut list) = self.get_deleted_and_modified_threads2(&current_board, &ft, false).await {
                                                        local_threads_list.append(&mut list);
                                                    } else {
                                                        println!("/{}/ [archive] Seems like there was no modified threads..", current_board);
                                                    }

                                                    // update base at the end
                                                    update_metadata_archive = true;
                                                }

                                            } else {
                                                // No cache
                                                // Use fetched_threads 
                                                    self.upsert_metadata(&current_board, "archive", &ft);
                                                    init_archive = false;
                                                    update_metadata_archive = false;
                                                if let Ok(mut list) = serde_json::from_value::<VecDeque<u32>>(ft) {
                                                    // self.queue.get_mut(&current_board).expect("err getting queue for board2").append(&mut fetched_threads_list);
                                                    local_threads_list.append(&mut list);
                                                } else {
                                                    println!("/{}/ [archive] Seems like there was no modified threads in the beginning?..", current_board);
                                                }
                                            }
                                            

                                            
                                        } else {
                                            eprintln!("/{}/ [archive] <{}> Fetched threads was found to be empty!", current_board, status)
                                        }
                                    },
                                    Err(e) => eprintln!("/{}/ [archive] <{}> An error has occurred getting the body\n{}", current_board, status, e),
                                }
                            },
                            reqwest::StatusCode::NOT_MODIFIED => {
                                eprintln!("/{}/ [archive] <{}>", current_board, status);
                            },
                            reqwest::StatusCode::NOT_FOUND => {
                                eprintln!("/{}/ [archive] <{}> No archives found! Retry attempt: #{}", current_board, status, rti);
                                task::sleep(Duration::from_millis(bs.throttle_millisec.into())).await;
                                has_archives = false;
                                continue;
                            },
                            _ => {
                                eprintln!("/{}/ [archive] <{}> An error has occurred!", current_board, status);
                            },
                        }

                        has_archives = true;
                        task::sleep(Duration::from_millis(bs.throttle_millisec.into())).await; // Ratelimit
                        break;
                    }
                }

            }

            // Process the threads
            {
                // let queue = self.queue.get(&current_board).expect("err getting queue for board3");
                let len = local_threads_list.len();
                if len > 0 {
                    println!("/{}/ Total New threads: {}", current_board, local_threads_list.len());
                    // BRUH I JUST WANT TO SHARE MUTABLE DATA
                    // This will loop until it recieves none
                    // while let Some(_) = self.drain_list(board).await {
                    // }
                    let mut position = 1;
                    while let Some(thread) = local_threads_list.pop_front() {
                        if let Some(finished) = self.finished.try_lock() {
                            if *finished {
                                return Some(());
                            }
                        } else {
                            eprintln!("Lock occurred trying to get finished");
                        }
                        self.assign_to_thread(&bs, thread, p, &position, &len).await;
                        position += 1;
                    }

                    // Update the cache at the end so that if the program was stopped while processing threads, when it restarts it'll use the same
                    // list of threads it was processing before + new ones.
                    if update_metadata {
                        if let Some(ft) = &fetched_threads {
                            self.upsert_metadata(&current_board, "threads", &ft);
                            update_metadata = false;
                        }
                    }
                    if update_metadata_archive {
                        if let Some(ft) = &fetched_archive_threads {
                            self.upsert_metadata(&current_board, "archive", &ft);
                            update_metadata_archive = false;
                        }
                    }
                } // No need to report if no new threads cause when it's not modified it'll tell us
            }
            // Ratelimit after fetching threads
            let delay:u64 = bs.refresh_delay.into();
            while now.elapsed().as_secs() <= delay {
                // Listen to CTRL-C
                if let Some(finished) = self.finished.try_lock() {
                    if *finished {
                        return Some(());
                    }
                } else {
                    eprintln!("Lock occurred trying to get finished in assign_to_board");
                }
                task::sleep(Duration::from_millis(250)).await;
            }
        }
        Some(())
    }

    // Download a single thread and its media
    async fn assign_to_thread(&self, bs: &BoardSettings2, thread: u32, path: &str, position: &u32, length: &usize) {
        let board = &bs.board;
        // let board_clean = &bs.board.replace("_ena", "");
        let mut _retry = 0;
        let mut _status_resp = reqwest::StatusCode::OK;
    
        let now = Instant::now();
        let one_millis = Duration::from_millis(1);

        let mut _canb=false;
        let delay:u128 = bs.throttle_millisec.into();

        for _ in 0..=bs.retry_attempts {
            // Listen to CTRL-C

            let (_last_modified_, status, body) =
                self.cget(&format!("{domain}/{bo}/thread/{th}.json", domain=bs.api_url, bo=board, th=thread ), "").await;
            _status_resp = status;

            match body {
                Ok(jb) => match serde_json::from_str::<serde_json::Value>(&jb) {
                    Ok(ret) => {
                        self.upsert_thread(board, &ret);
                        self.upsert_deleteds(board, thread, &ret);
                        println!("[{}/{}]\t/{}/{}",position, length, board, thread);
                        // _canb=true;
                        // _retry=0;
                        break;
                    },
                    Err(e) => {
                        if status == reqwest::StatusCode::NOT_FOUND {
                            println!("[{}/{}]\t/{}/{} <DELETED>",position, length, board, thread);
                            self.upsert_deleted(board, thread);
                            break;
                        }
                        eprintln!("/{}/{} <{}> An error occured deserializing the json! {}\n{:?}", board, thread, status, e,jb);
                        while now.elapsed().as_millis() <= delay {
                            task::sleep(one_millis).await;
                        }
                        /*_retry += 1;
                        if _retry <=(bs.retry_attempts+1) {
                            continue 'outer;
                        } else {
                            // TODO handle what to do with invalid thread
                            _retry = 0;
                            break 'outer;
                        }*/
                    },
                },
                Err(e) => {
                    println!("/{}/{} <{}> Error getting body {:?}", board, thread, status, e );
                }
            }
        }

        // FETCH MEDIA
        if bs.download_media || bs.download_thumbnails {
            match self.get_media_posts(board, thread) {
                Ok(media_list) => {
                    let mut fut = FuturesUnordered::new();
                    let client = &self.client;
                    let mut has_media = false;
                    for row in media_list.iter() {
                        has_media = true;
                        let no : i64  = row.get("no");
                        let sha256 : Option<Vec<u8>> = row.get("sha256");
                        let sha256t : Option<Vec<u8>> = row.get("sha256t");
                        let ext : String = row.get("ext");
                        let ext2 : String = row.get("ext");
                        let tim : i64 = row.get("tim");
                        if let Some(h) = sha256 {
                            // Improper sha, re-dl
                            if h.len() < (65/2) && bs.download_media {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board, thread, tim, ext, no as u64, true, false, client, path));
                            }
                        } else {
                            // No media, proceed to dl
                            if bs.download_media {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board, thread, tim, ext, no as u64, true, false, client, path));
                            }
                        }
                        if let Some(h) = sha256t {
                            // Improper sha, re-dl
                            if h.len() < (65/2) && bs.download_thumbnails {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board, thread, tim, ext2, no as u64, false, true, client, path));
                            }
                        } else {
                            // No thumbs, proceed to dl
                            if bs.download_thumbnails {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board, thread, tim, ext2, no as u64, false, true, client, path));
                            }
                        }
                    }
                    if has_media {
                        let s = &self;
                        while let Some(hh) = fut.next().await {
                            if let Some((no, hashsum, thumb_hash)) = hh {
                                // Listen to CTRL-C
                                if let Some(finished) = self.finished.try_lock() {
                                    if *finished {
                                        return;
                                    }
                                } else {
                                    eprintln!("Lock occurred trying to get finished");
                                }

                                if let Some(hsum) = hashsum {
                                    s.upsert_hash2(board, no, "sha256", hsum);
                                }
                                if let Some(hsumt) = thumb_hash {
                                    s.upsert_hash2(board, no, "sha256t", hsumt);
                                }
                            }
                        }
                    }
                },
                Err(e) => println!("/{}/{} Error getting missing media -> {:?}", board, thread, e),
            }
        }

        while now.elapsed().as_millis() <= delay {
            task::sleep(one_millis).await;
        }
    }

    // Downloads any missing media from a thread
    async fn dl_media_post(domain:&str, bs: &BoardSettings2, board: &str, thread: u32, tim:i64, ext: String ,no: u64, sha:bool, sha_thumb:bool, cl: &reqwest::Client, path:&str)  -> Option<(u64, Option<Vec<u8>>, Option<Vec<u8>>)> {
        // let board = &bs.board;
        let dl = |thumb| -> Result<Vec<u8>, reqwest::Error> {
            let url = format!("{}/{}/{}{}{}", domain, board, tim, if thumb {"s"} else {""} , if thumb {".jpg"} else {&ext} );
            println!("/{}/{}#{} -> {}{}{}",  board, thread, no,tim, if thumb {"s"} else {""} , if thumb {".jpg"} else {&ext});
            // TODO retry here

            // Download and save to file
            let mut resp = cl.get(&url).send()?;
            let status = resp.status();
            let mut hash_byte : Result<Vec<u8>, reqwest::Error> = Ok(vec![]);
            match status {
                reqwest::StatusCode::OK => {
                    let temp_path = format!("{}/tmp/{}_{}{}",path, no,tim,ext);
                    if let Ok(mut dest) = std::fs::File::create(&temp_path) {
                        if let Ok(_) = std::io::copy(&mut resp, &mut dest) {}
                        else { eprintln!("err file temp path copy"); }
                    }
                    else { eprintln!("err file temp path"); }

                    // Open the file we just downloaded, and hash it
                    let mut file = std::fs::File::open(&temp_path).expect("err opening temp file temp path");
                    let mut hasher = Sha256::new();
                    std::io::copy(&mut file, &mut hasher).expect("err io copy to hasher");
                    let hash = hasher.result();
                    hash_byte = Ok(hash.to_vec());
                    // println!("{}", &format!("{:x?}", a).as_hex());

                    // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
                    // /1/0a
                    // Move and rename
                    let hash_str = format!("{:x}", hash);
                    let basename = Path::new(&hash_str).file_stem().expect("err get basename").to_str().expect("err get basename end");
                    let second = &basename[&basename.len()-3..&basename.len()-1];
                    let first = &basename[&basename.len()-1..];
                    let final_dir_path = format!("{}/media/{}/{}",path, first, second);
                    let final_path = format!("{}/{}{}", final_dir_path, hash_str, ext);

                    if (bs.keep_media && !thumb) || (bs.keep_thumbnails && thumb) || ((bs.keep_media && !thumb) && (bs.keep_thumbnails && thumb)) {
                        
                        let path_final = Path::new(&final_path);
                        if !path_final.exists() {
                            if let Ok(_) = std::fs::create_dir_all(&final_dir_path){}
                            if let Ok(_) = std::fs::rename(&temp_path, &final_path){}
                        } else {
                            eprintln!("Already exists: {}", final_path);
                            if let Ok(_) = std::fs::remove_file(&temp_path){}
                        }
                    } else {
                        if let Ok(_) = std::fs::remove_file(&temp_path){}
                    }
                },
                reqwest::StatusCode::NOT_FOUND => eprintln!("/{}/{} <{}> {}", board, no, status, url),
                _ => eprintln!("/{}/{} <{}> {}", board, no, status, url),
            }
            hash_byte//Ok(hash_byte)
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

    // TODO: GET request will loop if nonexistant url
    async fn cget(&self, url: &str, last_modified: &str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
        let mut res = 
            match if last_modified == "" { self.client.get(url).send() } else { self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send() } {
                Ok(expr) => expr,
                Err(e) => {
                    eprintln!("{} -> {:?}", url, e);
                    task::sleep(Duration::from_secs(1)).await;
                    let a = loop {
                        match if last_modified == "" { self.client.get(url).send() } else { self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send() } {
                            Ok(resp) => break resp,
                            Err(e) => {
                                eprintln!("{} -> {:?}",url, e);
                                task::sleep(Duration::from_secs(1)).await;
                            },
                        }
                    };
                    a
                },
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

}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct BoardSettings2 {
    board: String,
    retry_attempts: u16,
    refresh_delay: u16,
    api_url: String,
    media_url: String,
    throttle_millisec: u32,
    download_media: bool,
    download_thumbnails: bool,
    keep_media: bool,
    keep_thumbnails: bool,
}

fn read_json(path: &str) -> Option<serde_json::Value>{
    if let Ok(file) = File::open(path) {
        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(s) => Some(s),
            Err(_) => None,
        }
    } else {
        None
    }
}

trait Hex {
    fn as_hex(&self) -> String;
}

impl Hex for String {
    fn as_hex(&self) -> String {
        self.chars().filter(|&c| !(c == ',' || c == ' ' || c == '[' || c == ']')).collect::<String>()
    }
}
