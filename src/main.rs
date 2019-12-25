#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(non_snake_case)]

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
    ⠄⢰⠸⣿⠄⢳⣠⣤⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⣼⣿⣿⣿⣿⣿⣿⡇⢻⡇⢸          \/___/  \/_/\/_/\/__/\/_/   v0.3.0
    ⢷⡈⢣⣡⣶⠿⠟⠛⠓⣚⣻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣇⢸⠇⠘     
    ⡀⣌⠄⠻⣧⣴⣾⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠛⠛⠛⢿⣿⣿⣿⣿⣿⡟⠘⠄⠄   
    ⣷⡘⣷⡀⠘⣿⣿⣿⣿⣿⣿⣿⣿⡋⢀⣠⣤⣶⣶⣾⡆⣿⣿⣿⠟⠁⠄⠄⠄⠄     An ultra lightweight 4chan archiver (¬ ‿ ¬ )
    ⣿⣷⡘⣿⡀⢻⣿⣿⣿⣿⣿⣿⣿⣧⠸⣿⣿⣿⣿⣿⣷⡿⠟⠉⠄⠄⠄⠄⡄⢀
    ⣿⣿⣷⡈⢷⡀⠙⠛⠻⠿⠿⠿⠿⠿⠷⠾⠿⠟⣛⣋⣥⣶⣄⠄⢀⣄⠹⣦⢹⣿
    "#);
    let start_time = Instant::now();
    let start_time_str = Local::now().to_rfc2822();
    pretty_env_logger::init();
    start_background_thread();
    println!("\nStarted on:\t{}\nFinished on:\t{}\nElapsed time:\t{}ms", start_time_str, Local::now().to_rfc2822(), start_time.elapsed().as_millis());
}

fn start_background_thread() {
        task::block_on(async{
            let archiver = YotsubaArchiver::new();
            archiver.listen_to_exit();
            archiver.init_schema();
            archiver.init_metadata();
            std::thread::sleep(Duration::from_millis(1500));

            let a = &archiver;
            let mut fut = FuturesUnordered::new();
            let asagiCompat = if let Some(set) =  a.settings.get("settings").expect("Err get settings").get("asagiCompat") {
                set.as_bool().expect("err deserializing asagiCompat to bool")
            } else {
                false
            };

            // Push each board to queue to be run concurrently
            let mut config = a.settings.to_owned();
            let bb = config.to_owned();
            let boards = bb.get("boards").expect("Err getting boards").as_array().expect("Err getting boards as array");
            for board in boards {
                let default = config.get_mut("boardSettings").expect("Err getting boardSettings").as_object_mut().expect("Err boardSettings as_object_mut");
                let board_map = board.as_object().expect("Err serializing board");
                for (k,v) in board_map.iter() {
                    default.insert(k.to_string(), 
                            if asagiCompat && k == "board" {
                                serde_json::json!(v.as_str().expect("Err deserializing v from k,v").to_string() + "_ena")
                            } else {
                                v.to_owned()
                            }
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

        let conn_url = format!("postgresql://{username}:{password}@{host}:{port}/{database}",
                                    username=defs.get("username").expect("Err get username").as_str().expect("Err convert username to str"),
                                    password=defs.get("password").expect("Err get password").as_str().expect("Err convert password to str"),
                                    host=defs.get("host").expect("Err get localhost").as_str().expect("Err convert host to str"),
                                    port=defs.get("port").expect("Err get port"),
                                    database=defs.get("database").expect("Err get database").as_str().expect("Err convert database to str") );
        YotsubaArchiver {
                conn: Connection::connect(conn_url, TlsMode::None).expect("Error connecting"),
                settings: settingss,
                client: reqwest::ClientBuilder::new().default_headers(default_headers).build().unwrap(),
                schema: schema,
                finished: async_std::sync::Arc::new(async_std::sync::Mutex::new(false)),
            }
    }

    fn init_schema(&self) {
        self.conn.execute(&format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema}";"#, schema=self.schema), &[])
            .expect(&format!("Err creating schema: {}", self.schema));
    }

    fn init_metadata(&self) {
        let sql = format!(r#"CREATE TABLE IF NOT EXISTS "{schema}".metadata
                    (
                        board text NOT NULL,
                        threads jsonb,
                        archive jsonb,
                        PRIMARY KEY (board),
                        CONSTRAINT board_unique UNIQUE (board)
                    );
                    CREATE INDEX IF NOT EXISTS metadata_board_idx on "{schema}".metadata(board);
                    "#, schema=self.schema);
        self.conn.batch_execute(&sql).expect("Err creating metadata");
    }

    fn init_board(&self, board: &str) {
        let sql = format!(r#"CREATE TABLE IF NOT EXISTS "{schema}"."{board_name}"
                    (
                        no bigint NOT NULL,
                        sticky smallint,
                        closed smallint,
                        deleted smallint,
                        now text NOT NULL,
                        name text,
                        sub text,
                        com text,
                        filedeleted smallint,
                        spoiler smallint,
                        custom_spoiler smallint,
                        filename text,
                        ext text,
                        w int,
                        h int,
                        tn_w int,
                        tn_h int,
                        tim bigint,
                        time bigint NOT NULL,
                        md5 bytea,
                        sha256 bytea,
                        sha256t bytea,
                        fsize bigint,
                        m_img smallint,
                        resto int NOT NULL DEFAULT 0,
                        trip text,
                        id text,
                        capcode text,
                        country text,
                        country_name text,
                        archived smallint,
                        bumplimit smallint,
                        archived_on bigint,
                        imagelimit smallint,
                        semantic_url text,
                        replies int,
                        images int,
                        unique_ips int,
                        tag text,
                        since4pass smallint,
                        PRIMARY KEY (no),
                        CONSTRAINT "unique_no_{board_name}" UNIQUE (no)
                    )"#, board_name=board, schema=self.schema);
        if let Ok(_) = self.conn.execute(&sql, &[]) {}
        if let Ok(_) = self.conn.execute(&format!(r#"create index "idx_no_resto_{board_name}" on "{schema}"."{board_name}"(no, resto)"#, board_name=board, schema=self.schema), &[]) {}
        self.conn.execute("set enable_seqscan to off;", &[]).expect("Err executing sql: set enable_seqscan to off");
        self.conn.batch_execute(&format!(r#"
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            CREATE INDEX IF NOT EXISTS "trgm_idx_com_{board_name}" ON "{schema}"."{board_name}" USING gin (com gin_trgm_ops);"#, board_name=board, schema=self.schema)).expect("Err creating index for com");
    }

    fn create_board_view(&self, board: &str) {
        let board_main = board.to_owned() + "_ena";
        let sql = format!(r#"
                            CREATE OR REPLACE VIEW "{schema}"."{board_name}" AS
                            select  no as doc_id,
                                    (CASE WHEN md5 is not null THEN no ELSE null END) as media_id,
                                    0::smallint as poster_ip, --not used
                                    no as num,
                                    0::smallint as subnum, --not used
                                    (CASE WHEN not resto=0 THEN resto ELSE no END) as thread_num,
                                    (CASE WHEN resto=0 THEN true ELSE false END) as op,
                                    time as "timestamp",
                                    0 as "timestamp_expired", --not used
                                    (CASE WHEN tim is not null THEN (tim::text || 's.jpg') ELSE null END) as preview_orig,
                                    (CASE WHEN tn_w is null THEN 0 ELSE tn_w END) as preview_w,
                                    (CASE WHEN tn_h is null THEN 0 ELSE tn_h END) as preview_h,
                                    (CASE WHEN filename is not null THEN (filename::text || ext) ELSE null END) as media_filename,
                                    (CASE WHEN w is null THEN 0 ELSE w END) as media_w,
                                    (CASE WHEN h is null THEN 0 ELSE h END) as media_h,
                                    (CASE WHEN fsize is null THEN 0 ELSE fsize END) as media_size,
                                    encode(md5, 'base64') as media_hash,
                                    (CASE WHEN tim is not null and ext is not null THEN (tim::text || ext) ELSE null END) as media_orig,
                                    (CASE WHEN spoiler is null or spoiler=0 THEN false ELSE true END) as spoiler,
                                    (CASE WHEN deleted is null or deleted=0 THEN false ELSE true END) as deleted,
                                    (CASE WHEN capcode is null THEN 'N' ELSE capcode END) as capcode,
                                    null as email, --deprecated
                                    name,
                                    trip,
                                    sub as title,
                                    com as comment,
                                    null as delpass, --not used
                                    (CASE WHEN sticky is null or sticky=0 THEN false ELSE true END) as sticky,
                                    (CASE WHEN closed is null or closed=0 THEN false ELSE true END) as locked,
                                    (CASE WHEN id='Developer' THEN 'Dev' ELSE id END) as poster_hash, --not the same as media_hash
                                    country as poster_country,
                                    country_name as poster_country_name,
                                    null as exif --not used
                                    from "{schema}"."{board_name_main}""#, board_name=board, board_name_main=board_main, schema=self.schema);
        self.conn.execute(&sql, &[]).expect("err create view");
        self.conn.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "{schema}".index_counters (
                          id character varying(50) NOT NULL,
                          val integer NOT NULL,
                          PRIMARY KEY (id)
                        )"#, schema=self.schema), &[]).expect("err create index_counters");
        self.conn.batch_execute(&format!(r#"
                                        CREATE TABLE IF NOT EXISTS "{schema}"."{board}_threads" (
                                          thread_num integer NOT NULL,
                                          time_op bigint NOT NULL,
                                          time_last bigint NOT NULL,
                                          time_bump bigint NOT NULL,
                                          time_ghost bigint DEFAULT NULL,
                                          time_ghost_bump bigint DEFAULT NULL,
                                          time_last_modified bigint NOT NULL,
                                          nreplies integer NOT NULL DEFAULT '0',
                                          nimages integer NOT NULL DEFAULT '0',
                                          sticky boolean DEFAULT false NOT NULL,
                                          locked boolean DEFAULT false NOT NULL,

                                          PRIMARY KEY (thread_num)
                                        );

                                        CREATE INDEX IF NOT EXISTS "{board}_threads_time_op_index" on "{schema}"."{board}_threads" (time_op);
                                        CREATE INDEX IF NOT EXISTS "{board}_threads_time_bump_index" on "{schema}"."{board}_threads" (time_bump);
                                        CREATE INDEX IF NOT EXISTS "{board}_threads_time_ghost_bump_index" on "{schema}"."{board}_threads" (time_ghost_bump);
                                        CREATE INDEX IF NOT EXISTS "{board}_threads_time_last_modified_index" on "{schema}"."{board}_threads" (time_last_modified);
                                        CREATE INDEX IF NOT EXISTS "{board}_threads_sticky_index" on "{schema}"."{board}_threads" (sticky);
                                        CREATE INDEX IF NOT EXISTS "{board}_threads_locked_index" on "{schema}"."{board}_threads" (locked);

                                        CREATE TABLE IF NOT EXISTS "{schema}"."{board}_users" (
                                          user_id SERIAL NOT NULL,
                                          name character varying(100) NOT NULL DEFAULT '',
                                          trip character varying(25) NOT NULL DEFAULT '',
                                          firstseen bigint NOT NULL,
                                          postcount bigint NOT NULL,

                                          PRIMARY KEY (user_id),
                                          UNIQUE (name, trip)
                                        );

                                        CREATE INDEX IF NOT EXISTS "{board}_users_firstseen_index" on "{schema}"."{board}_users" (firstseen);
                                        CREATE INDEX IF NOT EXISTS "{board}_users_postcount_index "on "{schema}"."{board}_users" (postcount);

                                        CREATE TABLE IF NOT EXISTS "{schema}"."{board}_images" (
                                          media_id SERIAL NOT NULL,
                                          media_hash character varying(25) NOT NULL,
                                          media character varying(20),
                                          preview_op character varying(20),
                                          preview_reply character varying(20),
                                          total integer NOT NULL DEFAULT '0',
                                          banned smallint NOT NULL DEFAULT '0',

                                          PRIMARY KEY (media_id),
                                          UNIQUE (media_hash)
                                        );

                                        CREATE INDEX IF NOT EXISTS "{board}_images_total_index" on "{schema}"."{board}_images" (total);
                                        CREATE INDEX IF NOT EXISTS "{board}_images_banned_index" ON "{schema}"."{board}_images" (banned);

                                        CREATE TABLE IF NOT EXISTS "{schema}"."{board}_daily" (
                                          day bigint NOT NULL,
                                          posts integer NOT NULL,
                                          images integer NOT NULL,
                                          sage integer NOT NULL,
                                          anons integer NOT NULL,
                                          trips integer NOT NULL,
                                          names integer NOT NULL,

                                          PRIMARY KEY (day)
                                        );

                                        CREATE TABLE IF NOT EXISTS "{schema}"."{board}_deleted" (
                                          LIKE "{schema}"."{board}" INCLUDING ALL
                                        );

                                        "#, board=board, schema=self.schema)).expect("Err initializing trigger functions");
        self.conn.batch_execute(&format!(r#"
                CREATE OR REPLACE FUNCTION "{board_name}_update_thread"(n_row "{schema}"."{board_name_main}") RETURNS void AS $$
                BEGIN
                  UPDATE
                    "{schema}"."{board_name}_threads" AS op
                  SET
                    time_last = (
                      COALESCE(GREATEST(
                        op.time_op,
                        (SELECT MAX(timestamp) FROM "{schema}"."{board_name}" re WHERE
                          re.thread_num = $1.no AND re.subnum = 0)
                      ), op.time_op)
                    ),
                    time_bump = (
                      COALESCE(GREATEST(
                        op.time_op,
                        (SELECT MAX(timestamp) FROM "{schema}"."{board_name}" re WHERE
                          re.thread_num = $1.no AND (re.email <> 'sage' OR re.email IS NULL)
                          AND re.subnum = 0)
                      ), op.time_op)
                    ),
                    time_ghost = (
                      SELECT MAX(timestamp) FROM "{schema}"."{board_name}" re WHERE
                        re.thread_num = $1.no AND re.subnum <> 0
                    ),
                    time_ghost_bump = (
                      SELECT MAX(timestamp) FROM "{schema}"."{board_name}" re WHERE
                        re.thread_num = $1.no AND re.subnum <> 0 AND (re.email <> 'sage' OR
                          re.email IS NULL)
                    ),
                    time_last_modified = (
                      COALESCE(GREATEST(
                        op.time_op,
                        (SELECT GREATEST(MAX(timestamp), MAX(timestamp_expired)) FROM "{schema}"."{board_name}" re WHERE
                          re.thread_num = $1.no)
                      ), op.time_op)
                    ),
                    nreplies = (
                      SELECT COUNT(*) FROM "{schema}"."{board_name}" re WHERE
                        re.thread_num = $1.no
                    ),
                    nimages = (
                      SELECT COUNT(media_hash) FROM "{schema}"."{board_name}" re WHERE
                        re.thread_num = $1.no
                    )
                    WHERE op.thread_num = $1.no;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_create_thread"(n_row "{schema}"."{board_name_main}") RETURNS void AS $$
                BEGIN
                  IF not n_row.resto = 0 THEN RETURN; END IF;
                  INSERT INTO "{schema}"."{board_name}_threads" SELECT $1.no, $1.time,$1.time,
                      $1.time, NULL, NULL, $1.time, 0, 0, false, false WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{board_name}_threads" WHERE thread_num=$1.no);
                  RETURN;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_delete_thread"(n_parent integer) RETURNS void AS $$
                BEGIN
                  DELETE FROM "{schema}"."{board_name}_threads" WHERE thread_num = n_parent;
                  RETURN;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_insert_image"(n_row "{schema}"."{board_name_main}") RETURNS integer AS $$
                DECLARE
                    img_id INTEGER;
                    n_row_preview_orig text;
                    n_row_media_hash text;
                    n_row_media_orig text;
                BEGIN
                    n_row_preview_orig := (CASE WHEN n_row.tim is not null THEN (n_row.tim::text || 's.jpg') ELSE null END);
                    n_row_media_hash := encode(n_row.md5, 'base64');
                    n_row_media_orig := (CASE WHEN n_row.tim is not null and n_row.ext is not null THEN (n_row.tim::text || n_row.ext) ELSE null END);
                  INSERT INTO "{schema}"."{board_name}_images"
                    (media_hash, media, preview_op, preview_reply, total)
                    SELECT n_row_media_hash, n_row_media_orig, NULL, NULL, 0
                    WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{board_name}_images" WHERE media_hash = n_row_media_hash);

                  IF n_row.resto = 0 THEN
                    UPDATE "{schema}"."{board_name}_images" SET total = (total + 1), preview_op = COALESCE(preview_op, n_row_preview_orig) WHERE media_hash = n_row_media_hash RETURNING media_id INTO img_id;
                  ELSE
                    UPDATE "{schema}"."{board_name}_images" SET total = (total + 1), preview_reply = COALESCE(preview_reply, n_row_preview_orig) WHERE media_hash = n_row_media_hash RETURNING media_id INTO img_id;
                  END IF;
                  RETURN img_id;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_delete_image"(n_media_id integer) RETURNS void AS $$
                BEGIN
                  UPDATE "{schema}"."{board_name}_images" SET total = (total - 1) WHERE id = n_media_id;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_insert_post"(n_row "{schema}"."{board_name_main}") RETURNS void AS $$
                DECLARE
                  d_day integer;
                  d_image integer;
                  d_sage integer;
                  d_anon integer;
                  d_trip integer;
                  d_name integer;
                BEGIN
                  d_day := FLOOR($1.time/86400)*86400;
                  d_image := CASE WHEN $1.md5 IS NOT NULL THEN 1 ELSE 0 END;
                  d_sage := CASE WHEN $1.name ILIKE '%sage%' THEN 1 ELSE 0 END;
                  d_anon := CASE WHEN $1.name = 'Anonymous' AND $1.trip IS NULL THEN 1 ELSE 0 END;
                  d_trip := CASE WHEN $1.trip IS NOT NULL THEN 1 ELSE 0 END;
                  d_name := CASE WHEN COALESCE($1.name <> 'Anonymous' AND $1.trip IS NULL, TRUE) THEN 1 ELSE 0 END;

                  INSERT INTO "{schema}"."{board_name}_daily"
                    SELECT d_day, 0, 0, 0, 0, 0, 0
                    WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{board_name}_daily" WHERE day = d_day);

                  UPDATE "{schema}"."{board_name}_daily" SET posts=posts+1, images=images+d_image,
                    sage=sage+d_sage, anons=anons+d_anon, trips=trips+d_trip,
                    names=names+d_name WHERE day = d_day;

                  IF (SELECT trip FROM "{schema}"."{board_name}_users" WHERE trip = $1.trip) IS NOT NULL THEN
                    UPDATE "{schema}"."{board_name}_users" SET postcount=postcount+1,
                      firstseen = LEAST($1.time, firstseen),
                      name = COALESCE($1.name, '')
                      WHERE trip = $1.trip;
                  ELSE
                    INSERT INTO "{schema}"."{board_name}_users" (name, trip, firstseen, postcount)
                      SELECT COALESCE($1.name,''), COALESCE($1.trip,''), $1.time, 0
                      WHERE NOT EXISTS (SELECT 1 FROM "{schema}"."{board_name}_users" WHERE name = COALESCE($1.name,'') AND trip = COALESCE($1.trip,''));

                    UPDATE "{schema}"."{board_name}_users" SET postcount=postcount+1,
                      firstseen = LEAST($1.time, firstseen)
                      WHERE name = COALESCE($1.name,'') AND trip = COALESCE($1.trip,'');
                  END IF;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_delete_post"(n_row "{schema}"."{board_name_main}") RETURNS void AS $$
                DECLARE
                  d_day integer;
                  d_image integer;
                  d_sage integer;
                  d_anon integer;
                  d_trip integer;
                  d_name integer;
                BEGIN
                  d_day := FLOOR($1.time/86400)*86400;
                  d_image := CASE WHEN $1.md5 IS NOT NULL THEN 1 ELSE 0 END;
                  d_sage := CASE WHEN $1.name ILIKE '%sage%' THEN 1 ELSE 0 END;
                  d_anon := CASE WHEN $1.name = 'Anonymous' AND $1.trip IS NULL THEN 1 ELSE 0 END;
                  d_trip := CASE WHEN $1.trip IS NOT NULL THEN 1 ELSE 0 END;
                  d_name := CASE WHEN COALESCE($1.name <> 'Anonymous' AND $1.trip IS NULL, TRUE) THEN 1 ELSE 0 END;

                  UPDATE "{schema}"."{board_name}_daily" SET posts=posts-1, images=images-d_image,
                    sage=sage-d_sage, anons=anons-d_anon, trips=trips-d_trip,
                    names=names-d_name WHERE day = d_day;

                  IF (SELECT trip FROM "{schema}"."{board_name}_users" WHERE trip = $1.trip) IS NOT NULL THEN
                    UPDATE "{schema}"."{board_name}_users" SET postcount=postcount-1,
                      firstseen = LEAST($1.time, firstseen)
                      WHERE trip = $1.trip;
                  ELSE
                    UPDATE "{schema}"."{board_name}_users" SET postcount=postcount-1,
                      firstseen = LEAST($1.time, firstseen)
                      WHERE (name = $1.name OR $1.name IS NULL) AND (trip = $1.trip OR $1.trip IS NULL);
                  END IF;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_before_insert"() RETURNS trigger AS $$
                BEGIN
                  IF NEW.md5 IS NOT NULL THEN
                    --SELECT "{schema}"."{board_name}_insert_image"(NEW) INTO NEW.no;
                    --INTO asagi.media_id;
                    PERFORM "{board_name}_insert_image"(NEW);
                  END IF;
                  RETURN NEW;
                END
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_after_insert"() RETURNS trigger AS $$
                BEGIN
                  IF NEW.md5 IS NOT NULL THEN
                    --SELECT "{schema}"."{board_name}_insert_image"(NEW) INTO NEW.no;
                    --INTO asagi.media_id;
                    PERFORM "{board_name}_insert_image"(NEW);
                  END IF;
                  IF NEW.resto = 0 THEN
                    PERFORM "{board_name}_create_thread"(NEW);
                  END IF;
                  PERFORM "{board_name}_update_thread"(NEW);
                  PERFORM "{board_name}_insert_post"(NEW);
                  RETURN NULL;
                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION "{board_name}_after_del"() RETURNS trigger AS $$
                BEGIN
                  PERFORM "{board_name}_update_thread"(OLD);
                  IF OLD.resto = 0 THEN
                    PERFORM "{board_name}_delete_thread"(OLD.no);
                  END IF;
                  PERFORM "{board_name}_delete_post"(OLD);
                  IF OLD.md5 IS NOT NULL THEN
                    PERFORM "{board_name}_delete_image"(OLD.no);
                  END IF;
                  RETURN NULL;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS "{board_name}_after_delete" ON "{schema}"."{board_name_main}";
                CREATE TRIGGER "{board_name}_after_delete" after DELETE ON "{schema}"."{board_name_main}"
                  FOR EACH ROW EXECUTE PROCEDURE "{board_name}_after_del"();

                --DROP TRIGGER IF EXISTS "{board_name}_before_insert" ON "{schema}"."{board_name_main}";
                --CREATE TRIGGER "{board_name}_before_insert" before INSERT ON "{schema}"."{board_name_main}"
                  --FOR EACH ROW EXECUTE PROCEDURE "{board_name}_before_insert"();

                DROP TRIGGER IF EXISTS "{board_name}_after_insert" ON "{schema}"."{board_name_main}";
                CREATE TRIGGER "{board_name}_after_insert" after INSERT ON "{schema}"."{board_name_main}"
                  FOR EACH ROW EXECUTE PROCEDURE "{board_name}_after_insert"();
            "#, board_name=board, board_name_main=board_main, schema=self.schema)).expect("Err initializing trigger functions");

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
        let sql = format!(r#"INSERT INTO "{schema}".metadata(board, {column})
                            VALUES ($1, $2::jsonb)
                            ON CONFLICT (board) DO UPDATE
                                SET {column} = $2::jsonb;"#, column=col, schema=self.schema);
        self.conn.execute(&sql, &[&board, &json_item]).expect("Err executing sql: upsert_metadata");

    }

    async fn assign_to_board(&self, bs: BoardSettings2) -> Option<()> {
        self.init_board(&bs.board);
        let current_board = &bs.board.replace("_ena", "");
        if bs.board.contains("_ena") {
            self.create_board_view(current_board);
        }
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
        let ps = self.settings.get("settings").expect("Err get settings").get("path").expect("err getting path").as_str().expect("err converting path to str");
        let p = ps.trim_end_matches('/').trim_end_matches('\\');

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

    async fn assign_to_thread(&self, bs: &BoardSettings2, thread: u32, path: &str, position: &u32, length: &usize) {
        let board = &bs.board;
        let board_clean = &bs.board.replace("_ena", "");
        let mut _retry = 0;
        let mut _status_resp = reqwest::StatusCode::OK;
    
        let now = Instant::now();
        let one_millis = Duration::from_millis(1);

        let mut _canb=false;
        let delay:u128 = bs.throttle_millisec.into();

        for _ in 0..=bs.retry_attempts {
            // Listen to CTRL-C

            let (_last_modified_, status, body) =
                self.cget(&format!("{domain}/{bo}/thread/{th}.json", domain=bs.api_url, bo=board_clean, th=thread ), "").await;
            _status_resp = status;

            match body {
                Ok(jb) => match serde_json::from_str::<serde_json::Value>(&jb) {
                    Ok(ret) => {
                        self.upsert_thread2(board, &ret);
                        self.upsert_deleteds(board, thread, &ret);
                        println!("[{}/{}]\t/{}/{}",position, length, board_clean, thread);
                        // _canb=true;
                        // _retry=0;
                        break;
                    },
                    Err(e) => {
                        if status == reqwest::StatusCode::NOT_FOUND {
                            self.upsert_deleted(board, thread);
                            break;
                        }
                        eprintln!("/{}/{} <{}> An error occured deserializing the json! {}\n{:?}", board_clean, thread, status, e,jb);
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
                    println!("/{}/{} <{}> Error getting body {:?}", board_clean, thread, status, e );
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
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board_clean, thread, tim, ext, no as u64, true, false, client, path));
                            }
                        } else {
                            // No media, proceed to dl
                            if bs.download_media {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board_clean, thread, tim, ext, no as u64, true, false, client, path));
                            }
                        }
                        if let Some(h) = sha256t {
                            // Improper sha, re-dl
                            if h.len() < (65/2) && bs.download_thumbnails {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board_clean, thread, tim, ext2, no as u64, false, true, client, path));
                            }
                        } else {
                            // No thumbs, proceed to dl
                            if bs.download_thumbnails {
                                fut.push(Self::dl_media_post(&bs.media_url, bs, board_clean, thread, tim, ext2, no as u64, false, true, client, path));
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
                Err(e) => println!("/{}/{} Error getting missing media -> {:?}", board_clean, thread, e),
            }
        }

        while now.elapsed().as_millis() <= delay {
            task::sleep(one_millis).await;
        }
    }

    // This downloads any missing media and/or thumbs
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

    // GET request will loop if nonexistant url
    async fn cget(&self, url: &str, last_modified: &str) -> (Option<String>, reqwest::StatusCode, Result<String, reqwest::Error>) {
        let mut res = 
            match if last_modified == "" { self.client.get(url).send() } else {self.client.get(url).header(IF_MODIFIED_SINCE,last_modified).send() }{
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

    fn get_media_posts(&self, board: &str, thread:u32) -> Result<postgres::rows::Rows, postgres::error::Error> {
        self.conn.query(&format!(r#"select * FROM "{schema}"."{board_name}" where (no={op} or resto={op}) and (md5 is not null) and (sha256 is null or sha256t is null) order by no"#,
                                board_name=board, op=thread, schema=self.schema), &[])
    }

    fn upsert_hash2(&self, board: &str, no:u64, hash_type: &str, hashsum: Vec<u8>) {
        let sql = format!(r#"
                    INSERT INTO "{schema}"."{board_name}"
                    SELECT *
                    FROM "{schema}"."{board_name}"
                    where no = {no_id}
                    ON CONFLICT (no) DO UPDATE
                        SET {htype} = $1"#, board_name=board, no_id=no, htype=hash_type, schema=self.schema);
        self.conn.execute(&sql, &[&hashsum]).expect("Err executing sql: upsert_hash2");
    }

    // Single upsert
    fn upsert_deleted(&self, board: &str, no:u32) {
        let sql = format!(r#"
                    INSERT INTO "{schema}"."{board_name}"
                    SELECT *
                    FROM "{schema}"."{board_name}"
                    where no = {no_id}
                    ON CONFLICT (no) DO UPDATE
                        SET deleted = 1"#, board_name=board, no_id=no, schema=self.schema);
        self.conn.execute(&sql, &[]).expect("Err executing sql: upsert_deleted");
    }
    
    fn upsert_deleteds(&self, board: &str, thread:u32, json_item: &serde_json::Value) {
        // The specific insert into and null::board_base is all to avoid inserting into
        // a generated column: com_search
        let sql = format!(r#"
                        INSERT INTO "{schema}"."{board_name}"
                            SELECT x.* FROM
                            (SELECT * FROM "{schema}"."{board_name}" where no={op} or resto={op} order by no) x
                            FULL JOIN
                            (SELECT * FROM jsonb_populate_recordset(null::"{schema}"."{board_name}", $1::jsonb->'posts')) z
                            ON  x.no  = z.no
                            where z.no is null
                        ON CONFLICT (no) 
                        DO
                            UPDATE 
                            SET deleted = 1;"#, board_name=board, op=thread, schema=self.schema);
        self.conn.execute(&sql, &[&json_item]).expect("Err executing sql: upsert_deleteds");
    }
    
    fn upsert_thread2(&self, board: &str, json_item: &serde_json::Value) {
        // This method inserts a post or updates an existing one.
        // It only updates rows where there's a column change. A majority of posts in a thread don't change. This saves IO writes. 
        // (It doesn't modify/update sha256, sha25t, or deleted. Those are manually done)
        // https://stackoverflow.com/a/36406023
        // https://dba.stackexchange.com/a/39821
        // println!("{}", serde_json::to_string(json_item).unwrap());
        //
        // md5 -> 4chan for some reason inserts a backslash
        // https://stackoverflow.com/a/11449627
        let sql = format!(r#"
                        insert into "{schema}"."{board_name}" (no,sticky,closed,now,name,sub,com,filedeleted,spoiler,
                                    custom_spoiler,filename,ext,w,h,tn_w,tn_h,tim,time,md5,
                                    fsize, m_img, resto,trip,id,capcode,country,country_name,archived,bumplimit,
                                    archived_on,imagelimit,semantic_url,replies,images,unique_ips,tag,since4pass)
                            select no,sticky,closed,now,name,sub,com,filedeleted,spoiler,
                                    custom_spoiler,filename,ext,w,h,tn_w,tn_h,tim,time, (CASE WHEN length(q.md5)>20 and q.md5 is not null THEN  decode(REPLACE (encode(q.md5, 'escape'::text), E'\\', '')::text, 'base64'::text) ELSE null::bytea END) as md55,
                                    fsize, m_img, resto,trip,q.id,capcode,country,country_name,archived,bumplimit,
                                    archived_on,imagelimit,semantic_url,replies,images,unique_ips,tag,since4pass
                            from jsonb_populate_recordset(null::"{schema}"."{board_name}", $1::jsonb->'posts') q
                            where q.no is not null
                        ON CONFLICT (no) 
                        DO
                            UPDATE 
                            SET 
                                no = excluded.no,
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
                                unique_ips = CASE WHEN excluded.unique_ips is not null THEN excluded.unique_ips ELSE "{schema}"."{board_name}".unique_ips END,
                                tag = excluded.tag,
                                since4pass = excluded.since4pass
                            where excluded.no is not null
                              and exists 
                                (
                                select 
                                        "{schema}"."{board_name}".no,
                                        "{schema}"."{board_name}".sticky,
                                        "{schema}"."{board_name}".closed,
                                        "{schema}"."{board_name}".now,
                                        "{schema}"."{board_name}".name,
                                        "{schema}"."{board_name}".sub,
                                        "{schema}"."{board_name}".com,
                                        "{schema}"."{board_name}".filedeleted,
                                        "{schema}"."{board_name}".spoiler,
                                        "{schema}"."{board_name}".custom_spoiler,
                                        "{schema}"."{board_name}".filename,
                                        "{schema}"."{board_name}".ext,
                                        "{schema}"."{board_name}".w,
                                        "{schema}"."{board_name}".h,
                                        "{schema}"."{board_name}".tn_w,
                                        "{schema}"."{board_name}".tn_h,
                                        "{schema}"."{board_name}".tim,
                                        "{schema}"."{board_name}".time,
                                        "{schema}"."{board_name}".md5,
                                        "{schema}"."{board_name}".fsize,
                                        "{schema}"."{board_name}".m_img,
                                        "{schema}"."{board_name}".resto,
                                        "{schema}"."{board_name}".trip,
                                        "{schema}"."{board_name}".id,
                                        "{schema}"."{board_name}".capcode,
                                        "{schema}"."{board_name}".country,
                                        "{schema}"."{board_name}".country_name,
                                        "{schema}"."{board_name}".archived,
                                        "{schema}"."{board_name}".bumplimit,
                                        "{schema}"."{board_name}".archived_on,
                                        "{schema}"."{board_name}".imagelimit,
                                        "{schema}"."{board_name}".semantic_url,
                                        "{schema}"."{board_name}".replies,
                                        "{schema}"."{board_name}".images,
                                        "{schema}"."{board_name}".unique_ips,
                                        "{schema}"."{board_name}".tag,
                                        "{schema}"."{board_name}".since4pass
                                    where "{schema}"."{board_name}".no is not null
                                except
                                select 
                                        excluded.no,
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
                                where excluded.no is not null and excluded.no = "{schema}"."{board_name}".no
                                )"#, board_name=board, schema=self.schema);
        self.conn.execute(&sql, &[&json_item]).expect("Err executing sql: upsert_thread2");
    }
    
    async fn get_threads_list(&self, json_item: &serde_json::Value) -> Option<VecDeque<u32>> {
        let sql = "SELECT jsonb_agg(newv->'no') from
                    (select jsonb_array_elements(jsonb_array_elements($1::jsonb)->'threads') as newv)z";
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
        let resp = self.conn.query(&format!(r#"select CASE WHEN {col_name} is not null THEN true ELSE false END from "{schema}".metadata where board = $1"#,
                                    col_name=col, schema=self.schema), &[&board]).expect("err query check_metadata_col");
        let mut res = false;
        for row in resp.iter() {
            let ret : Option<bool> = row.get(0);
            if let Some(r) = ret {
                res = r;
            } // else it's null, meaning false
        }
        res
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
                        (select jsonb_array_elements(jsonb_array_elements(threads)->'threads') as prev from "{schema}".metadata where board = $1)x
                        full JOIN
                        (select jsonb_array_elements(jsonb_array_elements($2::jsonb)->'threads') as newv)z
                        ON prev->'no' = (newv -> 'no') 
                        )q
                        left join
                        (select no as nno from "{schema}"."{board_name}" where resto=0 and (archived=1 or deleted=1))w
                        ON c = nno
                        where nno is null
                        "#, board_name=board, schema=self.schema)
                } else {
                    // archives
                    format!(r#"
                    select jsonb_agg(c) from (
                    SELECT coalesce (newv, prev)::bigint as c from
                    (select jsonb_array_elements(archive) as prev from "{schema}".metadata where board = $1)x
                    full JOIN
                    (select jsonb_array_elements($2::jsonb) as newv)z
                    ON prev = newv
                    )q
                    left join
                    (select no as nno from "{schema}"."{board_name}" where resto=0 and (archived=1 or deleted=1))w
                    ON c = nno
                    where nno is null
                    "#, board_name=board, schema=self.schema)
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
                format!(r#"
                SELECT jsonb_agg(COALESCE(newv->'no',prev->'no')) from
                (select jsonb_array_elements(jsonb_array_elements(threads)->'threads') as prev from "{schema}".metadata where board = $1)x
                full JOIN
                (select jsonb_array_elements(jsonb_array_elements($2::jsonb)->'threads') as newv)z
                ON prev->'no' = (newv -> 'no') 
                where newv is null or not prev->'last_modified' <@ (newv -> 'last_modified')
                "#, schema=self.schema)
            } else {
                format!(r#"
                SELECT coalesce(newv,prev) from
                (select jsonb_array_elements(archive) as prev from "{schema}".metadata where board = $1)x
                full JOIN
                (select jsonb_array_elements($2::jsonb) as newv)z
                ON prev = newv
                where prev is null or newv is null
                "#, schema=self.schema)
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
