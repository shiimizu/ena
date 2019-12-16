#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(non_snake_case)]

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
use std::thread;
use std::path::Path;

const RATELIMIT: u16 = 1000;
const REFRESH_DELAY: u8 = 10;

fn main() {
    let start_time = Instant::now();
    println!("{}", yotsuba_time());
    start_background_thread();
    println!("Program finished in {} ms", start_time.elapsed().as_millis());
}

fn start_background_thread() {
    // Do all work in a background thread to keep the main ui thread running smoothly
    // This essentially keeps CPU and power usage extremely low 
    thread::spawn(move || {
        // By using async tasks we can process an enormous amount of tasks concurrently
        // without the overhead of multiple native threads. 
        let mut archiver = YotsubaArchiver::new();
        archiver.init_metadata();
        archiver.poll_boards();
    }).join().unwrap();
}

// Have a struct to store our variables without using global statics
pub struct YotsubaArchiver {
    conn: Connection,
    settings : serde_json::Value,
    client: reqwest::Client,
    queue: Queue,
    //proxies: ProxyStream,
}

impl YotsubaArchiver {

    fn new() -> YotsubaArchiver {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0"));
        //default_headers.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(&yotsuba_time()).unwrap());
        std::fs::create_dir_all("./archive/tmp").expect("Err create dir tmp");
        let settingss = read_json("ena_config.json").expect("Err get config");
        let defs = settingss.get("sites").expect("Err get sites")
                            .as_array().expect("Err get sites array")[0]
                            .get("settings").expect("Err get settings")
                            .get("boardSettings").expect("Err get boardSettings")
                            .get("default").expect("Err get default").to_owned();
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
                //proxies: Self::get_proxy("cache/proxylist.json"),
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

    fn upsert_metadata(&self, board: &str, col: &str, json_item: &serde_json::Value) {
        let sql = format!("INSERT INTO metadata(board, {column})
                            VALUES ($1, $2::jsonb)
                            ON CONFLICT (board) DO UPDATE
                                SET {column} = $2::jsonb;", column=col);
        self.conn.execute(&sql, &[&board, &json_item]).expect("Err executing sql: upsert_metadata");

    }

    fn poll_boards(&mut self) {
        let boards = self.get_boards_raw();
        for board in boards.iter() {
            if !self.queue.contains_key(board) {
                //self.init_board(board);
                self.queue.insert(board.to_owned(), VecDeque::new());
            }
        }
        self.init_board(&boards[0]);
        self.assign_to_board(&boards[0]);
    }

    fn assign_to_board<'b>(&mut self, board: &'b str) ->Option<()>{
        let current_time = yotsuba_time();
        let current_board = String::from(board);
        let mut threads_last_modified = String::from(&current_time);
        let one_millis = Duration::from_millis(1);
        // let mut count:u32 = 0;
        loop {
            //et mut queue = self.queue.get_mut(&current_board).expect("err getting queue for board"); 
            let now = Instant::now();

            // Download threads.json
            // Scope to drop values when done
            {
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
                                    let fetched_threads : serde_json::Value = serde_json::from_str(&new_threads).expect("Err deserializing new threads");

                                    if let Some(_) = self.get_board_from_metadata(&current_board) {
                                        // compare time modified and get the new threads
                                        if let Some(mut fetched_threads_list) = self.get_deleted_and_modified_threads2(&current_board, &fetched_threads) {
                                            self.queue.get_mut(&current_board).expect("err getting queue for board1").append(&mut fetched_threads_list);
                                        } else {
                                            println!("/{}/ Seems like there was no modified threads..", current_board);
                                        }
                                    } else {
                                        // Use fetched_threads 
                                        if let Some(mut fetched_threads_list) = self.get_threads_list(&fetched_threads) {
                                            self.queue.get_mut(&current_board).expect("err getting queue for board2").append(&mut fetched_threads_list);

                                        } else {
                                            println!("/{}/ Seems like there was no modified threads in the beginning?..", current_board);
                                        }
                                    }
                                    // Use the new fetched threads as a base
                                    self.upsert_metadata(&current_board, "threads", &fetched_threads);
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
            }

            {
                let queue = self.queue.get(&current_board).expect("err getting queue for board3");
                if queue.len() > 0 {
                    println!("/{}/ Total New threads: {}", current_board, queue.len());
                    // BRUH I JUST WANT TO SHARE MUTABLE DATA
                    // This will loop until it recieves none
                    while let Some(_) = self.drain_list(board) {
                    }
                } // No need to report if no new threads cause when it's not modified it'll tell us
            }
            // Ratelimit after fetching threads
            while now.elapsed().as_secs() <= REFRESH_DELAY.into() {
                thread::sleep(one_millis);
            }
        }
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
        // TODO check if thread is empty or its posts are empty
        // If DB has an incomplete thread, archived, closed, or sticky
        let mut _retry = 0;
        let mut _status_resp = reqwest::StatusCode::OK;
    
        // dl and patch and push to db
        let now = Instant::now();
        let one_millis = Duration::from_millis(1);

        let mut _canb=false;
        'outer: loop {
            let (_last_modified_, status, body) =
                self.cget(&format!("{domain}/{bo}/thread/{th}.json", domain="http://a.4cdn.org", bo=board, th=thread ), "");
            _status_resp = status;

            if let Ok(jb) = body {
                match serde_json::from_str::<serde_json::Value>(&jb) {
                    Ok(ret) => {
                        println!("/{}/{}", board, thread);

                        self.upsert_thread2(board, &ret);
                        self.upsert_deleteds(board, thread, &ret);
                        
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
                        eprintln!("/{}/{} <{}> An error occured deserializing the json! {}\n{:?}",board, thread, status, e,jb);
                        while now.elapsed().as_millis() <= RATELIMIT.into() {
                            thread::sleep(one_millis);
                        }
                        _retry += 1;
                        if _retry <=3 {
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

        let download_media = false;
        let download_thumbs = false;
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

        while now.elapsed().as_millis() <= RATELIMIT.into() {
            thread::sleep(one_millis);
        }
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
                    std::io::copy(&mut resp, &mut dest).expect("err file temp path copy");

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

    fn cget(&self, url: &str, last_modified: &str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
        let mut res = if last_modified == "" {
            match self.client.get(url).send() {
                Ok(expr) => expr,
                Err(e) => {
                    eprintln!("{} -> {:?}", url, e);
                    thread::sleep(Duration::from_secs(1));
                    let a = loop {
                        match self.client.get(url).send() {
                            Ok(resp) => break resp,
                            Err(e) => {
                                eprintln!("{} -> {:?}",url, e);
                                thread::sleep(Duration::from_secs(1));
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
                    thread::sleep(Duration::from_secs(1));
                    let a = loop {
                        match self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send() {
                            Ok(resp) => break resp,
                            Err(e) => {
                                eprintln!("{} -> {:?}",url, e);
                                thread::sleep(Duration::from_secs(1));
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
                                unique_ips = excluded.unique_ips,
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

    /*fn get_threads_from_metadata(&self, board: &str) -> Option<serde_json::Value> {
        let resp = self.conn.query("select * from metadata where board = $1", &[&board]).expect("Err getting threads from metadata");
        let mut threads : Option<serde_json::Value> = None;
        for row in resp.iter() {
            threads = row.get("threads");
            //let j: serde_json::Value = row.get(0);
            //println!("{:?}", j.get("posts").unwrap().as_array().unwrap().len()); 
            // println!("{} {:?} {:?}", no, sha256, sha256t);
        }
        threads
    }*/

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
