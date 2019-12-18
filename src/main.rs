#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(non_snake_case)]
#![allow(dead_code)]

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

use std::collections::{BTreeMap, VecDeque};
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
     ____                        
    /\  _`\                      
    \ \ \L\_\    ___      __     
     \ \  _\L  /' _ `\  /'__`\   
      \ \ \L\ \/\ \/\ \/\ \L\.\_ 
       \ \____/\ \_\ \_\ \__/.\_\
        \/___/  \/_/\/_/\/__/\/_/   An ultra lightweight 4chan archiver (¬ ‿ ¬ )
"#);
    std::thread::sleep(Duration::from_millis(1500));
    let start_time = Instant::now();
    let start_time_str = Local::now().to_rfc2822();
    start_background_thread();
    //other();
    println!("\nStarted on:\t{}\nFinished on:\t{}\nElapsed time:\t{}ms", start_time_str, Local::now().to_rfc2822(), start_time.elapsed().as_millis());
}

fn other() {
    let mut config = read_json("ena_config.json").expect("Err get config");
    /*
    let mut default : BoardSettingsJson = serde_json::from_value(
        config.get_mut("boardSettings").expect("Err getting boardSettings").to_owned()
        ).expect("Err serializing boardSettings");*/
    let bb = config.to_owned();
    let boards = bb.get("boards").expect("Err getting boards").as_array().expect("Err getting boards as array");
    let default = config.get_mut("boardSettings").expect("Err getting boardSettings").as_object_mut().expect("Err boardSettings as_object_mut");
    
    let _current_board = "a";
    
    for board in boards {
        let board_map = board.as_object().expect("Err serializing board");
        for (k,v) in board_map.iter() {
            default.insert(k.to_string(), v.to_owned());
        }
        /*if let Some(bo) = b.board {
            if bo == current_board {
                if let Some(item) = b.retryAttempts {
                    default.retryAttempts = Some(item);
                }
                if let Some(item) = b.refreshDelay {
                    default.refreshDelay = Some(item);
                }
                if let Some(item) = b.apiURL {
                    default.apiURL = Some(item);
                }
                if let Some(item) = b.mediaURL {
                    default.mediaURL = Some(item);
                }
                if let Some(item) = b.throttleMillisec {
                    default.throttleMillisec = Some(item);
                }
                break;
            }
        }*/
    }

    let bs : BoardSettings2 = serde_json::from_value(serde_json::to_value(default.to_owned()).expect("Error serializing default")).expect("Error deserializing default");

    /*
    let bs = BoardSettings {
        board: current_board.to_string(),
        retry_attempts: default.retryAttempts.unwrap(),
        refresh_delay: default.refreshDelay.unwrap(),
        api_url: default.apiURL.unwrap(),
        media_url: default.mediaURL.unwrap(),
        throttle_millisec: default.throttleMillisec.unwrap(),
    };*/
    
    println!("{:#?}", &bs);
}

fn start_background_thread() {
        task::block_on(async{
            let archiver = YotsubaArchiver::new();
            archiver.listen_to_exit();
            archiver.init_metadata();

            let a = &archiver;
            let mut fut = FuturesUnordered::new();

            // Push each board to queue to be run concurrently
            let mut config = read_json("ena_config.json").expect("Err get config");
            let bb = config.to_owned();
            let boards = bb.get("boards").expect("Err getting boards").as_array().expect("Err getting boards as array");
            for board in boards {
                let default = config.get_mut("boardSettings").expect("Err getting boardSettings").as_object_mut().expect("Err boardSettings as_object_mut");
                let board_map = board.as_object().expect("Err serializing board");
                for (k,v) in board_map.iter() {
                    if k == "board" {
                        let bv = v.to_owned().as_str().unwrap().to_string();
                        // Check if board is a number
                        if let Ok(_) = bv.parse::<u32>() {
                            default.insert(k.to_string(), serde_json::json!(format!("_{}", bv.as_str())));
                        } else {
                            if bv == "int" {
                                default.insert(k.to_string(), serde_json::json!("int_"));
                            } else if bv == "out" {
                                default.insert(k.to_string(), serde_json::json!("out_"));
                            } else {
                                default.insert(k.to_string(), v.to_owned());
                            }
                        };
                    } else {
                        default.insert(k.to_string(), v.to_owned());
                    }
                }
                let bs : BoardSettings2 = serde_json::from_value(serde_json::to_value(default.to_owned()).expect("Error serializing default")).expect("Error deserializing default");
                    
                //println!("{:#?}", &bs);
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
    queue: Queue,
    finished: async_std::sync::Arc<async_std::sync::Mutex::<bool>>,
    //proxies: ProxyStream,
}

impl YotsubaArchiver {

    fn new() -> YotsubaArchiver {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0"));
        //default_headers.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(&yotsuba_time()).unwrap());
        std::fs::create_dir_all("./archive/tmp").expect("Err create dir tmp");
        let settingss = read_json("ena_config.json").expect("Err get config");
        
        let defs = settingss.get("settings").expect("Err get settings");
        /*let defs = settingss.get("sites").expect("Err get sites")
                            .as_array().expect("Err get sites array")[0]
                            .get("settings").expect("Err get settings")
                            .get("boardSettings").expect("Err get boardSettings")
                            .get("default").expect("Err get default").to_owned();*/
        let conn_url = format!("postgresql://{username}:{password}@{host}:{port}/{database}",
                                    username=defs.get("username").expect("Err get username").as_str().expect("Err convert username to str"),
                                    password=defs.get("password").expect("Err get password").as_str().expect("Err convert password to str"),
                                    host=defs.get("host").expect("Err get localhost").as_str().expect("Err convert host to str"),
                                    port=defs.get("port").expect("Err get port"),
                                    database=defs.get("database").expect("Err get database").as_str().expect("Err convert database to str") );
        let y = YotsubaArchiver {
                conn: Connection::connect(conn_url, TlsMode::None).expect("Error connecting"),
                settings: settingss,
                client: reqwest::ClientBuilder::new().default_headers(default_headers).build().unwrap(),
                queue: Queue::new(),
                finished: async_std::sync::Arc::new(async_std::sync::Mutex::new(false)),
                //proxies: Self::get_proxy("cache/proxylist.json"),
            };
        //println!("Finished Initializing");
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
        if let Ok(_) = self.conn.execute(sql, &[]) {}
    }

    fn init_board(&self, board: &str) {
        let sql = format!("CREATE TABLE IF NOT EXISTS {board_name}
                    (
                        no bigint NOT NULL,
                        sticky smallint,
                        closed smallint,
                        deleted smallint,
                        now character varying NOT NULL,
                        name character varying,
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
                        CONSTRAINT unique_no_{board_name} UNIQUE (no)
                    )", board_name=board);
        if let Ok(_) = self.conn.execute(&sql, &[]) {}
        if let Ok(_) = self.conn.execute(&format!("create index {board_name}_no_resto_idx on {board_name}(no, resto)", board_name=board), &[]) {}
        self.conn.execute("set enable_seqscan to off;", &[]).expect("Err executing sql: set enable_seqscan to off");
    }

    fn create_board_view(&self, board: &str) {
        let sql = format!(r#"
CREATE OR REPLACE VIEW {board_name}_asagi AS
select  no as doc_id,
        no as media_id,
        0::smallint as poster_ip,
        no as num,
        0::smallint as sub_num,
        no as thread_num,
        (CASE WHEN resto=0 THEN true ELSE false END) as op,
        time as "timestamp",
        0 as "timestamp_expired",
        (tim::text || '.jpg') as preview_orig,
        (CASE WHEN tn_w is null THEN 0 ELSE tn_w END) as preview_w,
        (CASE WHEN tn_h is null THEN 0 ELSE tn_h END) as preview_h,
        (filename::text || ext) as media_filename,
        (CASE WHEN w is null THEN 0 ELSE w END) as media_w,
        (CASE WHEN h is null THEN 0 ELSE h END) as media_h,
        (CASE WHEN fsize is null THEN 0 ELSE fsize END) as media_size,
        md5 as media_hash,
        (tim::text || ext) as media_orig,
        (CASE WHEN spoiler is null or spoiler=0 THEN false ELSE true END) as spoiler,
        (CASE WHEN deleted is null or deleted=0 THEN false ELSE true END) as deleted,
        (CASE WHEN capcode is null THEN 'N' ELSE capcode END) as capcode,
        null as email,
        name,
        trip,
        sub as title,
        com as comment,
        null as delpass,
        (CASE WHEN sticky is null or sticky=0 THEN false ELSE true END) as sticky,
        (CASE WHEN closed is null or closed=0 THEN false ELSE true END) as locked,
        md5 as poster_hash,
        country as poster_country,
        country_name as poster_country_name,
        null as exif
        from {board_name}"#, board_name=board);
        if let Ok(_) = self.conn.execute(&sql, &[]) {}
    }

    fn listen_to_exit(&self) {
        //let f = async_std::sync::Arc::new(async_std::sync::Mutex::new(self.finished));
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
        let sql = format!("INSERT INTO metadata(board, {column})
                            VALUES ($1, $2::jsonb)
                            ON CONFLICT (board) DO UPDATE
                                SET {column} = $2::jsonb;", column=col);
        self.conn.execute(&sql, &[&board, &json_item]).expect("Err executing sql: upsert_metadata");

    }

    fn init_boards(&mut self) {
        let boards = self.get_boards_raw();
        // let mut fut = FuturesUnordered::new();
        for board in boards.iter() {
            if !self.queue.contains_key(board) {
                self.init_board(board);
                self.queue.insert(board.to_owned(), VecDeque::new());
            }
        }
    }
    /*async fn poll_boards(&mut self) {
        let boards = self.get_boards_raw();
        // let mut fut = FuturesUnordered::new();
        for board in boards.iter() {
            if !self.queue.contains_key(board) {
                self.init_board(board);
                self.queue.insert(board.to_owned(), VecDeque::new());
            }
        }

        let a = async_std::sync::Arc::new(async_std::sync::Mutex::new(self));
        let a_clone = async_std::sync::Arc::clone(&a);

        let b = async_std::sync::Arc::new(async_std::sync::Mutex::new(&boards));
        let b_clone = async_std::sync::Arc::clone(&b);

        let c = async_std::sync::Arc::new(async_std::sync::Mutex::new(0));
        let c_clone = async_std::sync::Arc::clone(&c);

        futures::stream::iter(0..boards.len()).for_each_concurrent(
            /* limit */ None,
            |_| async  {
                if let Some(mut se) = a_clone.try_lock() {
                    if let Some(boards_list) = b_clone.try_lock() {
                        if let Some(mut idx) = c_clone.try_lock() {
                            // println!("Assign to board {}", boards_list[*idx]);
                            // task::spawn(async { 
                                // se.assign_to_board(&boards_list[*idx]).await;
                            // });
                            self.tt(&boards_list[*idx]);
                            // Self::tt().await;
                            *idx += 1;
                        } else {
                            eprintln!("Err trying get num lock");
                        }
                    } else {
                        eprintln!("Err trying get boards_list lock");
                    }
                } else {
                    eprintln!("Err trying get self lock");
                }
            }
        ).await;
        loop {
            // println!("Sleep {}", i);
            if let Some(se) = a_clone.try_lock() {
                if let Some(finished) = se.finished.try_lock() {
                    if *finished {
                        break;
                    }
                } else {
                    eprintln!("Lock occurred trying to get finished");
                }
            } else {
                eprintln!("Lock occurred trying to get self");
            }
            task::sleep(Duration::from_millis(250)).await;
        }
        /*let a = async_std::sync::Arc::new(async_std::sync::Mutex::new(self));
        for i in 0..boards.len() {
            let a_clone = async_std::sync::Arc::clone(&a);
            let mut va=a_clone.lock().await;
                fut.push(va.assign_to_board(&boards[i]));
        }
        while let Some(_) = fut.next().await {
        }*/
        // self.init_board(&boards[0]);
        // self.assign_to_board(&boards[0]).await;
    }*/

    async fn assign_to_board<'b>(&self, bs: BoardSettings2) -> Option<()> {
        self.init_board(&bs.board);
        self.create_board_view(&bs.board);

        let current_board = &bs.board.replace("_","");
        let current_board_raw = &bs.board;
        let one_millis = Duration::from_millis(1);
        let mut threads_last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
        let mut archives_last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
        let mut local_threads_list : VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut update_metadata_archive = false;
        let mut init = true;
        let mut init_archive = true;
        let mut has_archives = true;
        // let mut count:u32 = 0;
        loop {
            // Listen to CTRL-C
            if let Some(finished) = self.finished.try_lock() {
                if *finished {
                    return Some(());
                }
            } else {
                eprintln!("Lock occurred trying to get finished");
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
                            None => eprintln!("/{}/ <{}> [threads] An error has occurred getting the last_modified date", current_board, status),
                        }
                        match body {
                            Ok(new_threads) => {
                                if !new_threads.is_empty() {
                                    println!("/{}/ [threads] Received new threads on {}", current_board, Local::now().to_rfc2822());
                                    fetched_threads = Some(serde_json::from_str::<serde_json::Value>(&new_threads).expect("Err deserializing new threads"));
                                    let ft = fetched_threads.to_owned().unwrap();

                                    if self.check_metadata_col(&current_board_raw, "threads") {
                                        // if there's cache
                                        // if this is a first startup
                                        if init {
                                            // going here means the program was restarted
                                            // use combination of all threads from cache + new threads (excluding archived, deleted, and duplicate threads)
                                            if let Some(mut fetched_threads_list) = self.get_combined_threads(&current_board_raw, &ft, true).await {
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
                                            if let Some(mut fetched_threads_list) = self.get_deleted_and_modified_threads2(&current_board_raw, &ft, true).await {
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
                                            self.upsert_metadata(&current_board_raw, "threads", &ft);
                                            init = false;
                                            update_metadata = false;
                                        } else {
                                            println!("/{}/ [threads] Seems like there was no modified threads in the beginning?..", current_board);
                                        }
                                    }
                                    

                                    
                                } else {
                                    eprintln!("/{}/ <{}> [threads] Fetched threads was found to be empty!", current_board, status)
                                }
                            },
                            Err(e) => eprintln!("/{}/ <{}> [threads] An error has occurred getting the body\n{}", current_board, status, e),
                        }
                    },
                    reqwest::StatusCode::NOT_MODIFIED => {
                        eprintln!("/{}/ <{}> [threads]", current_board, status);
                    },
                    _ => {
                        eprintln!("/{}/ <{}> [threads] An error has occurred!", current_board, status);
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
                                    None => eprintln!("/{}/ <{}> [archive] An error has occurred getting the last_modified date", current_board, status),
                                }
                                match body {
                                    Ok(new_threads) => {
                                        if !new_threads.is_empty() {
                                            println!("/{}/ [archive] Received new threads on {}", current_board, Local::now().to_rfc2822());
                                            fetched_archive_threads = Some(serde_json::from_str::<serde_json::Value>(&new_threads).expect("Err deserializing new threads"));
                                            let ft = fetched_archive_threads.to_owned().unwrap();

                                            if self.check_metadata_col(&current_board_raw, "archive") {
                                                // if there's cache
                                                // if this is a first startup
                                                if init_archive {
                                                    // going here means the program was restarted
                                                    // use combination of all threads from cache + new threads (excluding archived, deleted, and duplicate threads)
                                                    if let Some(mut list) = self.get_combined_threads(&current_board_raw, &ft, false).await {
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
                                                    if let Some(mut list) = self.get_deleted_and_modified_threads2(&current_board_raw, &ft, false).await {
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
                                                    self.upsert_metadata(&current_board_raw, "archive", &ft);
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
                                            eprintln!("/{}/ <{}> [archive] Fetched threads was found to be empty!", current_board, status)
                                        }
                                    },
                                    Err(e) => eprintln!("/{}/ <{}> [archive] An error has occurred getting the body\n{}", current_board, status, e),
                                }
                            },
                            reqwest::StatusCode::NOT_MODIFIED => {
                                eprintln!("/{}/ <{}> [archive]", current_board, status);
                            },
                            reqwest::StatusCode::NOT_FOUND => {
                                eprintln!("/{}/ <{}> No archives found! Retry attempt: #{}", current_board, status, rti);
                                task::sleep(Duration::from_millis(bs.throttle_millisec.into())).await;
                                has_archives = false;
                                continue;
                            },
                            _ => {
                                eprintln!("/{}/ <{}> [archive] An error has occurred!", current_board, status);
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
                if local_threads_list.len() > 0 {
                    println!("/{}/ Total New threads: {}", current_board, local_threads_list.len());
                    // BRUH I JUST WANT TO SHARE MUTABLE DATA
                    // This will loop until it recieves none
                    // while let Some(_) = self.drain_list(board).await {
                    // }

                    while let Some(thread) = local_threads_list.pop_front() {
                        if let Some(finished) = self.finished.try_lock() {
                            if *finished {
                                return Some(());
                            }
                        } else {
                            eprintln!("Lock occurred trying to get finished");
                        }
                        self.assign_to_thread(&bs, thread).await;
                    }

                    // Update the cache at the end so that if the program was stopped while processing threads, when it restarts it'll use the same
                    // list of threads it was processing before + new ones.
                    if update_metadata {
                        if let Some(ft) = &fetched_threads {
                            self.upsert_metadata(&current_board_raw, "threads", &ft);
                            update_metadata = false;
                        }
                    }
                    if update_metadata_archive {
                        if let Some(ft) = &fetched_archive_threads {
                            self.upsert_metadata(&current_board_raw, "archive", &ft);
                            update_metadata_archive = false;
                        }
                    }
                } // No need to report if no new threads cause when it's not modified it'll tell us
            }
            // Ratelimit after fetching threads
            let delay:u64 = bs.refresh_delay.into();
            while now.elapsed().as_secs() <= delay {
                task::sleep(one_millis).await;
            }
        }
        Some(())
    }

    // BRUH LET ME JUST SHARE MUTABLE REFERENCES AHHHHHHHHHHHHHHH
    /*async fn drain_list(&mut self, board: &str) -> Option<u32> {
        // Repeatedly getting the list probably isn't the most efficient...
        if let Some(a) = self.queue.get_mut(board) {
            let aa = a.pop_front();
            if let Some(thread) = aa {
                // println!("{:?}/{:?} assignde",board ,newt);
                self.assign_to_thread(&bs, thread).await;
            }
            return aa;
        }
        None
    }*/

    // There's a lot of object creation here but they should all get dropped so it shouldn't matter
    async fn assign_to_thread(&self, bs: &BoardSettings2, thread: u32) {
        // TODO check if thread is empty or its posts are empty
        // If DB has an incomplete thread, archived, closed, or sticky
        let board = &bs.board;
        let mut _retry = 0;
        let mut _status_resp = reqwest::StatusCode::OK;
    
        // dl and patch and push to db
        let now = Instant::now();
        let one_millis = Duration::from_millis(1);

        let mut _canb=false;
        'outer: loop {
            // Listen to CTRL-C

            let (_last_modified_, status, body) =
                self.cget(&format!("{domain}/{bo}/thread/{th}.json", domain=bs.api_url, bo=board, th=thread ), "").await;
            _status_resp = status;

            if let Ok(jb) = body {
                match serde_json::from_str::<serde_json::Value>(&jb) {
                    Ok(ret) => {
                        self.upsert_thread2(board, &ret);
                        self.upsert_deleteds(board, thread, &ret);
                        println!("/{}/{}", board, thread);
                        
                        /*match self.upsert_thread(board, thread, ret) {
                            Ok(q) => println!("/{}/{} <{}> Success upserting the thread! {:?}",board, thread, status, q),
                            Err(e) => eprintln!("/{}/{} <{}> An error occured upserting the thread! {}",board, thread, status, e),
                        } */
                        _canb=true;
                        _retry=0;
                        break;
                    },
                    Err(e) => {
                        if status == reqwest::StatusCode::NOT_FOUND {
                            self.upsert_deleted(board, thread);
                            break;
                        }
                        eprintln!("/{}/{} <{}> An error occured deserializing the json! {}\n{:?}", board, thread, status, e,jb);
                        let delay:u128 = bs.throttle_millisec.into();
                        while now.elapsed().as_millis() <= delay {
                            task::sleep(one_millis).await;
                        }
                        _retry += 1;
                        if _retry <=(bs.retry_attempts+1) {
                            continue 'outer;
                        } else {
                            // TODO handle what to do with invalid thread
                            _retry = 0;
                            break 'outer;
                        }
                    },
                }
            }
        }

        // DL MEDIA
        // Need to check against other md5 so we don't redownload if we have it
        if bs.download_media || bs.download_thumbnails {
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
                if let Some(_) = sha256 {
                } else {
                    // No media, proceed to dl
                    if bs.download_media {
                        fut.push(Self::dl_media_post(&bs.media_url, bs, thread, tim, ext, no as u64, true, false, client));
                    }
                }
                if let Some(_) = sha256t {
                } else {
                    // No thumbs, proceed to dl
                    if bs.download_thumbnails {
                        fut.push(Self::dl_media_post(&bs.media_url, bs, thread, tim, ext2, no as u64, false, true, client));
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
                            s.upsert_hash2(board, no, "sha256", &hsum);
                        }
                        if let Some(hsumt) = thumb_hash {
                            s.upsert_hash2(board, no, "sha256t", &hsumt);
                        }
                    }
                }
            }
        }

        let delay:u128 = bs.throttle_millisec.into();
        while now.elapsed().as_millis() <= delay {
            task::sleep(one_millis).await;
        }
    }

    // this downloads any missing media and/or thumbs
    async fn dl_media_post(domain:&str, bs: &BoardSettings2, thread: u32, tim:i64, ext: String ,no: u64, sha:bool, sha_thumb:bool, cl: &reqwest::Client)  -> Option<(u64, Option<String>, Option<String>)> {
        let board = "";
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

                    // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
                    // /1/0a
                    // Move and rename
                    hash_str.push_str(&format!("{:x}", hash));
                    let path_hash = Path::new(&hash_str);
                    let basename = path_hash.file_stem().expect("err get basename").to_str().expect("err get basename end");
                    let second = &basename[&basename.len()-3..&basename.len()-1];
                    let first = &basename[&basename.len()-1..];
                    let final_dir_path = format!("./archive/media/{}/{}",first, second);
                    let final_path = format!("{}/{:x}{}", final_dir_path, hash, ext);

                    // discarding errors...
                    if bs.keep_media && !thumb {
                        if let Ok(_) = std::fs::create_dir_all(&final_dir_path){}
                        if let Ok(_) = std::fs::rename(&temp_path, final_path){}
                    } else if bs.keep_thumbnails && thumb {
                        if let Ok(_) = std::fs::create_dir_all(&final_dir_path){}
                        if let Ok(_) = std::fs::rename(&temp_path, final_path){}
                    }
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

    async fn cget(&self, url: &str, last_modified: &str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
        let mut res = if last_modified == "" {
            match self.client.get(url).send() {
                Ok(expr) => expr,
                Err(e) => {
                    eprintln!("{} -> {:?}", url, e);
                    task::sleep(Duration::from_secs(1)).await;
                    let a = loop {
                        match self.client.get(url).send() {
                            Ok(resp) => break resp,
                            Err(e) => {
                                eprintln!("{} -> {:?}",url, e);
                                task::sleep(Duration::from_secs(1)).await;
                            },
                        }
                    };
                    a
                },
            }
            // self.client.get(url).send().expect("err cget!")
        } else {
            match self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send() {
                Ok(expr) => expr,
                Err(e) => {
                    eprintln!("{} -> {:?}", url, e);
                    task::sleep(Duration::from_secs(1)).await;
                    let a = loop {
                        match self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send() {
                            Ok(resp) => break resp,
                            Err(e) => {
                                eprintln!("{} -> {:?}",url, e);
                                task::sleep(Duration::from_secs(1)).await;
                            },
                        }
                    };
                    a
                },
            }
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

    fn upsert_hash2(&self, board: &str, no:u64, hash_type: &str, hashsum: &str) {
        let sql = format!("
                    INSERT INTO {board_name}
                    SELECT *
                    FROM {board_name}
                    where no = {no_id}
                    ON CONFLICT (no) DO UPDATE
                        SET {htype} = $1", board_name=board, no_id=no, htype=hash_type);
        self.conn.execute(&sql, &[&hashsum]).expect("Err executing sql: upsert_hash2");
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
        self.conn.execute(&sql, &[]).expect("Err executing sql: upsert_deleted");
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
        self.conn.execute(&sql, &[&json_item]).expect("Err executing sql: upsert_deleteds");
    }
    
    fn upsert_thread2(&self, board: &str, json_item: &serde_json::Value) {
        // This method inserts a post or updates an existing one.
        // It only updates rows where there's a column change. A majority of posts in a thread don't change. This saves IO writes. 
        // (It doesn't modify/update sha256, sha25t, or deleted. Those are manually done)
        // https://stackoverflow.com/a/36406023
        // https://dba.stackexchange.com/a/39821
        let sql = format!("
                        insert into {board_name}
                            select * from jsonb_populate_recordset(null::{board_name}, $1::jsonb->'posts')
                            where no is not null
                        ON CONFLICT (no) 
                        DO
                            UPDATE 
                            SET no = excluded.no,
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
                                unique_ips = CASE WHEN excluded.unique_ips is not null THEN excluded.unique_ips ELSE {board_name}.unique_ips END,
                                tag = excluded.tag,
                                since4pass = excluded.since4pass
                            where excluded.no is not null
                              and exists 
                                (
                                select {board_name}.no,
                                        {board_name}.sticky,
                                        {board_name}.closed,
                                        {board_name}.now,
                                        {board_name}.name,
                                        {board_name}.sub,
                                        {board_name}.com,
                                        {board_name}.filedeleted,
                                        {board_name}.spoiler,
                                        {board_name}.custom_spoiler,
                                        {board_name}.filename,
                                        {board_name}.ext,
                                        {board_name}.w,
                                        {board_name}.h,
                                        {board_name}.tn_w,
                                        {board_name}.tn_h,
                                        {board_name}.tim,
                                        {board_name}.time,
                                        {board_name}.md5,
                                        {board_name}.fsize,
                                        {board_name}.m_img,
                                        {board_name}.resto,
                                        {board_name}.trip,
                                        {board_name}.id,
                                        {board_name}.capcode,
                                        {board_name}.country,
                                        {board_name}.country_name,
                                        {board_name}.archived,
                                        {board_name}.bumplimit,
                                        {board_name}.archived_on,
                                        {board_name}.imagelimit,
                                        {board_name}.semantic_url,
                                        {board_name}.replies,
                                        {board_name}.images,
                                        {board_name}.unique_ips,
                                        {board_name}.tag,
                                        {board_name}.since4pass
                                    where {board_name}.no is not null
                                except
                                select excluded.no,
                                        excluded.sticky,
                                        excluded.closed,
                                        excluded.now,
                                        excluded.name,
                                        excluded.sub,
                                        excluded.com,
                                        excluded.filedeleted,
                                        excluded.spoiler,
                                        excluded.custom_spoiler,
                                        excluded.filename,
                                        excluded.ext,
                                        excluded.w,
                                        excluded.h,
                                        excluded.tn_w,
                                        excluded.tn_h,
                                        excluded.tim,
                                        excluded.time,
                                        excluded.md5,
                                        excluded.fsize,
                                        excluded.m_img,
                                        excluded.resto,
                                        excluded.trip,
                                        excluded.id,
                                        excluded.capcode,
                                        excluded.country,
                                        excluded.country_name,
                                        excluded.archived,
                                        excluded.bumplimit,
                                        excluded.archived_on,
                                        excluded.imagelimit,
                                        excluded.semantic_url,
                                        excluded.replies,
                                        excluded.images,
                                        excluded.unique_ips,
                                        excluded.tag,
                                        excluded.since4pass
                                where excluded.no is not null and excluded.no = {board_name}.no
                                )", board_name=board);
        self.conn.execute(&sql, &[&json_item]).expect("Err executing sql: upsert_thread2");
    }
    
    async fn get_threads_list(&self, json_item: &serde_json::Value) -> Option<VecDeque<u32>> {
        let sql = "SELECT jsonb_agg(newv->'no') from
                    (select jsonb_path_query($1::jsonb, '$[*].threads[*]') as newv)z";
        let resp = self.conn.query(&sql, &[&json_item]).expect("Error getting modified and deleted threads from new threads.json");
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

    fn check_metadata_col(&self, board: &str, col:&str) -> bool {
        let resp = self.conn.query(&format!("select CASE WHEN {col_name} is not null THEN true ELSE false END from metadata where board = $1",
                                    col_name=col), &[&board]).expect("err query check_metadata_col");
        let mut res = false;
        for row in resp.iter() {
            let ret : Option<bool> = row.get(0);
            if let Some(r) = ret {
                res = r;
            } // else it's null, meaning false
        }
        res
    }

    // Use this and not the one below so no deserialization happens and no overhead
    fn get_board_from_metadata(&self, board: &str) -> Option<String> {
        let resp = self.conn.query("select * from metadata where board = $1", &[&board]).expect("Err getting threads from metadata");
        let mut board_ : Option<String> = None;
        for row in resp.iter() {
            board_ = row.get("board");
        }
        board_
    }

    async fn get_combined_threads(&self, board: &str, new_threads: &serde_json::Value, threads: bool) -> Option<VecDeque<u32>> {
        // This query is only run ONCE at every startup
        // Running a JOIN to compare against the entire DB on every INSERT/UPDATE would not be that great. 
        // This gets all the threads from cache, compares it to the new json to get new + modified threads
        // The compares that result to the database where a thread is deleted or archived, and takes only the threads where's it's not
        // deleted or archived
        let sql = if threads {
                    format!(r#"
                        select jsonb_agg(c) from (
                        SELECT coalesce (prev->'no', newv->'no')::bigint as c from
                        (select jsonb_path_query(threads, '$[*].threads[*]') as prev from metadata where board = $1)x
                        full JOIN
                        (select jsonb_path_query($2::jsonb, '$[*].threads[*]') as newv)z
                        ON prev->'no' = (newv -> 'no') 
                        )q
                        left join
                        (select no as nno from {board_name} where resto=0 and (archived=1 or deleted=1))w
                        ON c = nno
                        where nno is null
                        "#, board_name=board)
                } else {
                    format!(r#"
                    select jsonb_agg(c) from (
                    SELECT coalesce (newv, prev)::bigint as c from
                    (select jsonb_array_elements(archive) as prev from metadata where board = $1)x
                    full JOIN
                    (select jsonb_array_elements($2::jsonb) as newv)z
                    ON prev = newv
                    )q
                    left join
                    (select no as nno from {board_name} where resto=0 and (archived=1 or deleted=1))w
                    ON c = nno
                    where nno is null
                    "#, board_name=board)
                };
        let resp = self.conn.query(&sql, &[&board, &new_threads]).expect("Error getting modified and deleted threads from new threads.json");
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


    async fn get_deleted_and_modified_threads2(&self, board: &str, new_threads: &serde_json::Value, threads: bool) -> Option<VecDeque<u32>> {
        // Combine new and prev threads.json into one. This retains the prev threads (which the new json doesn't contain, meaning they're either pruned or archived).
        //  That's especially useful for boards without archives.
        // Use the WHERE clause to select only modified threads. Now we basically have a list of deleted and modified threads.
        // Return back this list to be processed.
        // Use the new threads.json as the base now.
        let sql = if threads {
                r#"
                SELECT jsonb_agg(COALESCE(newv->'no',prev->'no')) from
                (select jsonb_path_query(threads, '$[*].threads[*]') as prev from metadata where board = $1)x
                full JOIN
                (select jsonb_path_query($2::jsonb, '$[*].threads[*]') as newv)z
                ON prev->'no' = (newv -> 'no') 
                where newv is null or not prev->'last_modified' <@ (newv -> 'last_modified')
                "#
            } else {
                r#"
                SELECT coalesce(newv,prev) from
                (select jsonb_array_elements(archive) as prev from metadata where board = $1)x
                full JOIN
                (select jsonb_array_elements($2::jsonb) as newv)z
                ON prev = newv
                where prev is null or newv is null
                "#
            };
        let resp = self.conn.query(&sql, &[&board, &new_threads]).expect("Error getting modified and deleted threads from new threads.json");
        let mut _result : Option<VecDeque<u32>> = None;
        'outer: for _ri in 0..4 {
            for row in resp.iter() {
                let jsonb : Option<serde_json::Value> = row.get(0);
                match jsonb {
                    Some(val) => {
                        let q :VecDeque<u32> = serde_json::from_value(val).expect("Err deserializing get_deleted_and_modified_threads2");
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

    fn get_boards_raw(&self) -> Vec<String> {
        self.settings.get("boards").unwrap()
        .as_array().unwrap()
        .iter().to_owned()
        .map(|x| x.as_object().unwrap()
                    .get("board").unwrap()
                    .as_str().unwrap().to_string()
                    ).collect::<Vec<String>>()
    }
    fn get_defaults(&self) -> serde_json::Value {
        self.settings.get("sites").unwrap()
        .as_array().unwrap()[0]
        .get("settings").unwrap()
        .get("boardSettings").unwrap()
        .get("default").unwrap().to_owned()
    }

    /*fn get_proxy(proxy_path: &str) -> ProxyStream {
        if let Some(p) = read_json(proxy_path) {
            let mut ps = ProxyStream::new();
            ps.urls.append(
            &mut serde_json::from_value::<VecDeque<String>>(p).unwrap());
            ps
        } else {
            ProxyStream::new()
        }
    }*/


}

type Queue = BTreeMap<String, VecDeque<u32>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BoardSettingsJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    board: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    retryAttempts: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    refreshDelay: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    apiURL: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    mediaURL: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    throttleMillisec: Option<u32>,
}

#[derive(Debug, Clone)]
struct BoardSettings {
    board: String,
    retry_attempts: u16,
    refresh_delay: u16,
    api_url: String,
    media_url: String,
    throttle_millisec: u32,
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

/// A cycle stream that can append new values
/*#[derive(Debug)]
pub struct ProxyStream {
    urls: VecDeque<String>,
    count: i32,
}

impl ProxyStream {
    fn new() -> ProxyStream {
        ProxyStream { urls: VecDeque::new(), count: 0 }
    }
    fn push(&mut self, s: String) {
        self.urls.push_front(s);
    }
    fn len(&self) -> usize {
        self.urls.len()
    }
}

impl futures::stream::Stream for ProxyStream {

    type Item = String;

    fn poll_next(mut self: async_std::pin::Pin<&mut Self>, _cx: &mut async_std::task::Context<'_>) -> async_std::task::Poll<Option<Self::Item>> {
        self.count += 1;
        
        let max = self.urls.len();
        let c = match self.count {
            //0 => { c = 0 },
            x if x >= 1 && x < self.urls.len() as i32 => x,
            _ => {
                self.count = 0;
                0
            }
        };
        if max != 0 {
            async_std::task::Poll::Ready(Some(self.urls[c as usize].to_owned()))
        } else {
            async_std::task::Poll::Ready(None)
        }

        
    }
}*/

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

fn yotsuba_time() -> String {
    chrono::Utc::now().to_rfc2822().replace("+0000", "GMT")
}
