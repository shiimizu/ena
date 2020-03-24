//! PostgreSQL implementation.

use crate::{archiver::YotsubaArchiver, enums::YotsubaEndpoint, sql::*};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, convert::TryFrom};
use tokio_postgres::{Client, Row, Statement};

/// Unused for now. This is just for the Cargo doc.
#[cold]
#[allow(dead_code)]
pub mod core {
    //! Implementation of the default schema.  
    //!
    //! - [`Client`] The PostgreSQL implementation ([`tokio_postgres`]) to run the SQL queries
    //! - [`Schema`] The schema and list of SQL queries that `ena` uses
    #[allow(unused_imports)]
    use super::*;
    use std::ops::Deref;

    /// Client wrapper for the implementation of the database ([`tokio_postgres`])
    pub struct Client<T>(T);

    impl<T> Client<T> {
        fn new(x: T) -> Client<T> {
            Client(x)
        }
    }

    impl<T> Deref for Client<T> {
        type Target = T;

        fn deref(&self) -> &T {
            &self.0
        }
    }

    /// Core schema and SQL queries to use
    pub struct Schema;

    impl Schema {
        fn new() -> Self {
            Self {}
        }
    }

    /// The schema for each row.  
    ///
    /// This is just for documentation.  
    /// Optimized specifically for Postgres by using column tetris to save space.  
    /// Reasonable changes were also made to the original schema.
    ///
    /// ## Added
    /// - [`subnum`](struct.Post.html#structfield.subnum) For ghost posts
    /// - [`deleted_on`](struct.Post.html#structfield.deleted_on) For context
    /// - [`last_modified`](struct.Post.html#structfield.last_modified) For context and possible use
    ///   of search engines
    /// - [`sha256`](struct.Post.html#structfield.sha256) For file dedup and to prevent [MD5 collisions](https://github.com/4chan/4chan-API/issues/70)
    /// - [`sha256t`](struct.Post.html#structfield.sha256t) For file dedup and to prevent [MD5 collisions](https://github.com/4chan/4chan-API/issues/70)
    /// - [`extra`](struct.Post.html#structfield.extra) For any future schema changes
    /// ## Removed
    /// - [`now`](struct.Post.html#structfield.now) Redundant with
    ///   [`time`](struct.Post.html#structfield.time)
    /// - [`archived`](struct.Post.html#structfield.archived) Redundant with
    ///   [`archived_on`](struct.Post.html#structfield.archived_on)  
    /// ## Modified  
    /// - [`md5`](struct.Post.html#structfield.md5) From base64 to binary, to save space
    /// - [`country`](struct.Post.html#structfield.country) Use `troll_country` if [`country`] is `NULL`
    /// - To boolean, to save space
    ///     - [`sticky`](struct.Post.html#structfield.sticky)
    ///     - [`closed`](struct.Post.html#structfield.closed)
    ///     - [`filedeleted`](struct.Post.html#structfield.filedeleted)
    ///     - [`spoiler`](struct.Post.html#structfield.spoiler)
    ///     - [`m_img`](struct.Post.html#structfield.m_img)
    ///     - [`bumplimit`](struct.Post.html#structfield.bumplimit)
    ///     - [`imagelimit`](struct.Post.html#structfield.imagelimit)
    /// ## Side notes  
    /// Postgres doesn't have a numeric smaller than [`i16`] or unsigned integers.  
    /// For example: [`custom_spoiler`](struct.Post.html#structfield.custom_spoiler) should ideally
    /// be [`u8`].
    ///
    /// ---  
    /// The following below is taken from the [official docs](https://github.com/4chan/4chan-API) where applicable.  
    #[cold]
    #[allow(dead_code)]
    #[rustfmt::skip]
    #[derive(Deserialize, Serialize, Debug, Clone)]
    #[serde(default)]
    pub struct Post {
        /// Appears: `always`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> The numeric post ID</font>
        pub no: i64,
    
        /// Appears: `always if post is a ghost post`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Used in FF for ghost posting</font>
        pub subnum: Option<i64>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `integer`  
        /// <font style="color:#789922;">> Unix timestamp + microtime that an image was
        /// uploaded</font>
        pub tim: Option<i64>,
    
        /// Appears: `always`  
        /// Possible values: `0` or `any positive integer`  
        /// <font style="color:#789922;">> For replies: this is the ID of the thread being replied
        /// to. For OP: this value is zero</font>
        pub resto: i64,
    
        /// Appears: `always`  
        /// Possible values: `UNIX timestamp`  
        /// <font style="color:#789922;">> UNIX timestamp the post was created</font>
        pub time: i64,
    
        /// Appears: `always`  
        /// Possible values: `UNIX timestamp`  
        /// <font style="color:#789922;">> UNIX timestamp the post had any of its fields
        /// modified</font>
        pub last_modified: i64,
    
        /// Appears: `OP only, if thread has been archived`  
        /// Possible values: `UNIX timestamp`  
        /// <font style="color:#789922;">> UNIX timestamp the post was archived</font>
        pub archived_on: Option<i64>,
    
        /// Appears: `always if post has been deleted`  
        /// Possible values: `UNIX timestamp`  
        /// <font style="color:#789922;">> UNIX timestamp the post was deleted</font>
        pub deleted_on: Option<i64>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Size of uploaded file in bytes</font>
        pub fsize: Option<i64>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Image width dimension</font>
        pub w: Option<i32>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Image height dimension</font>
        pub h: Option<i32>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Thumbnail image width dimension</font>
        pub tn_w: Option<i32>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Thumbnail image height dimension</font>
        pub tn_h: Option<i32>,
    
        /// Appears: `OP only`  
        /// Possible values: `0` or `any positive integer`  
        /// <font style="color:#789922;">> Total number of replies to a thread</font>
        pub replies: Option<i32>,
    
        /// Appears: `OP only`  
        /// Possible values: `0` or `any positive integer`  
        /// <font style="color:#789922;">> Total number of image replies to a thread</font>
        pub images: Option<i32>,
    
        /// Appears: `OP only, only if thread has NOT been archived`  
        /// Possible values: `any positive integer`  
        /// <font style="color:#789922;">> Number of unique posters in a thread</font>
        pub unique_ips: Option<i32>,
    
        /// Appears: `if post has attachment and attachment is spoilered`  
        /// Possible values: `1-10` or not set  
        /// <font style="color:#789922;">> The custom spoiler ID for a spoilered image </font>
        pub custom_spoiler: Option<i16>,
    
        /// Appears: `if poster put 'since4pass' in the options field`  
        /// Possible values: `any 4 digit year`  
        /// <font style="color:#789922;">> Year 4chan pass bought</font>
        pub since4pass: Option<i16>,
    
        /// Appears: `OP only, if thread is currently stickied`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> If the thread is being pinned to the top of the page</font>
        pub sticky: Option<bool>,
    
        /// Appears: `OP only, if thread is currently closed`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> If the thread is closed to replies</font>
        pub closed: Option<bool>,
        
        /// Appears: `if post had attachment and attachment is deleted`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> If the file was deleted from the post</font>
        pub filedeleted: Option<bool>,
        
        /// Appears: `if post has attachment and attachment is spoilered`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> If the image was spoilered or not</font>
        pub spoiler: Option<bool>,
        
        /// Appears: `any post that has a mobile-optimized image`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> Mobile optimized image exists for post</font>
        pub m_img: Option<bool>,
        
        /// Appears: `OP only, only if bump limit has been reached`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> If a thread has reached bumplimit, it will no longer bump</font>
        pub bumplimit: Option<bool>,
        
        /// Appears: `OP only, only if image limit has been reached`  
        /// Possible values: `1` or not set  
        /// <font style="color:#789922;">> If an image has reached image limit, no more image replies can be made</font>
        pub imagelimit: Option<bool>,
    
        /// Appears: `always` <sup><b>Note:</b> Can be empty if user has a tripcode and opted out of a name</sup>  
        /// Possible values: `any string`  
        /// <font style="color:#789922;">> Name user posted with. Defaults to `Anonymous`</font>
        pub name: Option<String>,
    
        /// Appears: `OP only, if subject was included`  
        /// Possible values: `any string`  
        /// <font style="color:#789922;">> OP Subject text</font>
        pub sub: Option<String>,
    
        /// Appears: `if comment was included`  
        /// Possible values: `any HTML escaped string`  
        /// <font style="color:#789922;">> Comment (HTML escaped)</font>
        pub com: Option<String>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `any string`  
        /// <font style="color:#789922;">> Filename as it appeared on the poster's device</font>
        pub filename: Option<String>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values: `.jpg`, `.png`, `.gif`, `.pdf`, `.swf`, `.webm`  
        /// <font style="color:#789922;">> Filetype</font>
        pub ext: Option<String>,
    
        /// Appears: `if post has tripcode`  
        /// Possible values: `any string`  
        /// <font style="color:#789922;">> The user's tripcode, in format: `!tripcode` or `!!securetripcode`</font>
        pub trip: Option<String>,
    
        /// Appears: `if post has ID`  
        /// Possible values: `any 8 characters`  
        /// <font style="color:#789922;">> The poster's ID</font>
        pub id: Option<String>,
    
        /// Appears: `if post has capcode`  
        /// Possible values: Not set, `mod`, `admin`, `admin_highlight`, `manager`, `developer`, `founder`  
        /// <font style="color:#789922;">> The capcode identifier for a post</font>
        pub capcode: Option<String>,
    
        /// Appears: `if country flags are enabled`  
        /// Possible values: `2 character string` or `XX` if unknown or `troll_country` if present 
        /// <font style="color:#789922;">> Poster's [ISO 3166-1 alpha-2 country code](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)</font>
        pub country: Option<String>,
    
        /// Appears: `Name of any country`  
        /// Possible values: `any string`  
        /// <font style="color:#789922;">> Poster's country name</font>
        pub country_name: Option<String>,
    
        /// Appears: `OP only`  
        /// Possible values: `string`  
        /// <font style="color:#789922;">> SEO URL slug for thread</font>
        pub semantic_url: Option<String>,
    
        /// Appears: `OP only`, `/f/ only`  
        /// Possible values: `Game`, `Loop`, etc..  
        /// <font style="color:#789922;">> The category of `.swf` upload</font>
        pub tag: Option<String>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values:   
        /// <font style="color:#789922;">> 24 character (base64), decoded binary MD5 hash of file</font>
        pub md5: Option<Vec<u8>>,
    
        /// Appears: `always if post has attachment`  
        /// Possible values:   
        /// <font style="color:#789922;">> 65 character (hex), binary SHA256 hash of file</font>
        pub sha256: Option<Vec<u8>>,
    
        /// Appears: `always if post has attachment`, excludes `/f/`  
        /// Possible values:  
        /// <font style="color:#789922;">> 65 character (hex), binary SHA256 hash of thumbnail</font>
        pub sha256t: Option<Vec<u8>>,
        
        /// Appears: ` `  
        /// Possible values:  
        /// <font style="color:#789922;">> Optional field for extra schema additions</font>
        pub extra: Option<Vec<u8>>
    }

    impl Default for Post {
        fn default() -> Self {
            let t = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
            Self {
                no:             t.as_secs() as i64,
                subnum:         None,
                tim:            None,
                resto:          t.as_secs() as i64,
                time:           t.as_secs() as i64,
                last_modified:  t.as_secs() as i64,
                archived_on:    None,
                deleted_on:     None,
                fsize:          None,
                w:              None,
                h:              None,
                tn_w:           None,
                tn_h:           None,
                replies:        None,
                images:         None,
                unique_ips:     None,
                custom_spoiler: None,
                since4pass:     None,
                sticky:         None,
                closed:         None,
                filedeleted:    None,
                spoiler:        None,
                m_img:          None,
                bumplimit:      None,
                imagelimit:     None,
                name:           None,
                sub:            None,
                com:            None,
                filename:       None,
                ext:            None,
                trip:           None,
                id:             None,
                capcode:        None,
                country:        None,
                country_name:   None,
                semantic_url:   None,
                tag:            None,
                md5:            None,
                sha256:         None,
                sha256t:        None,
                extra:          None
            }
        }
    }
}

pub mod asagi {
    //! Implementation of the Asagi schema.  
    //!
    //! - [`Schema`] The schema and list of SQL queries that `asagi` uses

    /// Nonexistent :)
    pub struct Schema;
}
#[async_trait]
impl Archiver
    for YotsubaArchiver<Statement, tokio_postgres::Row, tokio_postgres::Client, reqwest::Client>
{
    async fn run_inner(&self) -> Result<()> {
        Ok(self.run().await?)
    }
}

impl QueryRaw for Client {
    fn inquiry(&self, statement: YotsubaStatement, id: QueryIdentifier) -> String {
        let endpoint = id.endpoint;
        match statement {
            YotsubaStatement::InitSchema => format!(
                r#"
            CREATE SCHEMA IF NOT EXISTS "{schema}";
            SET search_path TO "{schema}";
            "#,
                schema = id.schema.unwrap()
            ),
            YotsubaStatement::InitMetadata => r#"
    CREATE TABLE IF NOT EXISTS metadata (
        board text NOT NULL,
        threads jsonb,
        archive jsonb,
        PRIMARY KEY (board),
        CONSTRAINT board_unique UNIQUE (board));
  
    CREATE INDEX IF NOT EXISTS metadata_board_idx on metadata(board);
    "#
            .into(),
            YotsubaStatement::InitBoard => {
                format!(
                    r#"
            CREATE TABLE IF NOT EXISTS "{board}" (
              no bigint NOT NULL,
              subnum bigint,
              tim bigint,
              resto bigint NOT NULL DEFAULT 0,
              time bigint NOT NULL DEFAULT 0,
              last_modified bigint,
              archived_on bigint,
              deleted_on bigint,
              fsize bigint,
              w int,
              h int,
              tn_w int,
              tn_h int,
              replies int,
              images int,
              unique_ips int,
              custom_spoiler smallint,
              since4pass smallint,
              sticky boolean,
              closed boolean,
              filedeleted boolean,
              spoiler boolean,
              m_img boolean,
              bumplimit boolean,
              imagelimit boolean,
              name text,
              sub text,
              com text,
              filename text,
              ext text,
              trip text,
              id text,
              capcode text,
              country text,
              country_name text,
              semantic_url text,
              tag text,
              md5 bytea,
              sha256 bytea,
              sha256t bytea,
              extra jsonb,
              PRIMARY KEY (no{extra_constraint}),
              CONSTRAINT "unique_no_{board}" UNIQUE (no{extra_constraint}));
            
            {timescale_extra}
            
            CREATE INDEX IF NOT EXISTS "idx_{board}_no_resto" on "{board}"(no, resto);
            
            -- Needs to be superuser
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            
            CREATE INDEX IF NOT EXISTS "trgm_idx_{board}_com" ON "{board}" USING gin (com gin_trgm_ops);
            -- SET enable_seqscan TO OFF;"#,
                    board = id.board,
                    extra_constraint =
                        if matches!(id.engine, Database::TimescaleDB) { r#","time""# } else { "" },
                    timescale_extra = if matches!(id.engine, Database::TimescaleDB) {
                        r#"SELECT create_hypertable('c', 'time', chunk_time_interval => 2592000);"#
                    } else {
                        ""
                    },
                )
                // 1day => sec: 86400
                // 1month => sec: 2592000
            }
            YotsubaStatement::Metadata => {
                // ?? This endpoint will always be threads because archived threads are handled
                // locally because it's just an array
                if matches!(id.endpoint, YotsubaEndpoint::Threads) {
                    format!(
                        r#"
                    SELECT (CASE WHEN (
                        SELECT jsonb_agg(newv->'no') FROM
                            (SELECT jsonb_array_elements(jsonb_array_elements("{endpoint}")->'threads') as newv FROM "metadata" WHERE board = $1) z
                        WHERE newv->'no' IS NOT NULL
                        ) IS NOT NULL AND "{endpoint}" IS NOT NULL
                        THEN TRUE ELSE FALSE END) as "check"
                    FROM "metadata" WHERE board = $1;
                    "#,
                        endpoint = id.endpoint
                    )
                } else {
                    format!(
                        r#"
                 SELECT (CASE WHEN (
                    SELECT jsonb_agg(newv) FROM
                        (SELECT jsonb_array_elements("{endpoint}") as newv FROM "metadata" WHERE board = $1) z
                    WHERE newv IS NOT NULL
                    ) IS NOT NULL AND "{endpoint}" IS NOT NULL
                    THEN TRUE ELSE FALSE END) as "check"
                FROM "metadata" WHERE board = $1;
                 "#,
                        endpoint = id.endpoint
                    )
                }
            }
            YotsubaStatement::UpdateMetadata => format!(
                r#"
            INSERT INTO metadata(board, {endpoint})
              VALUES ($1, $2::jsonb)
              ON CONFLICT (board)
              DO UPDATE
                SET {endpoint} = $2::jsonb;
            "#,
                endpoint = id.endpoint
            ),
            YotsubaStatement::Threads => {
                // This endpoint will always be threads because archived threads
                // are already in array format, we can just return them
                "SELECT jsonb_agg(newv->'no')
                FROM
                (SELECT jsonb_array_elements(jsonb_array_elements($1::jsonb)->'threads') as newv)z
                WHERE newv->'no' is not null;"
                    .into()
            }

            // Combine new and prev threads.json into one. This retains the prev
            // threads (which the new json doesn't contain, meaning they're either
            // pruned or archived).  That's especially useful for boards without
            // archives. Use the WHERE clause to select only modified threads. Now
            // we basically have a list of deleted and modified threads.
            // Return back this list to be processed.
            // Use the new threads.json as the base now.
            // Additionally the fetched thread is checksummed against the thread in db
            // if any changes have been made.
            YotsubaStatement::ThreadsModified => format!(
                r#"
        SELECT (
        CASE WHEN new_hash IS DISTINCT FROM prev_hash THEN
        (
          SELECT jsonb_agg({}) from
          (select jsonb_array_elements({}) as prev from metadata where board = $1)x
          full JOIN
          (select jsonb_array_elements({}) as newv)z
          ON {}
          where newv is null or prev is null {}
        ) END
        ) FROM 
        (SELECT sha256(decode({} #>> '{{}}', 'escape')) as prev_hash from metadata where board=$1) w
        FULL JOIN
        (SELECT sha256(decode($2::jsonb #>> '{{}}', 'escape')) as new_hash) q
        ON TRUE;
        "#,
                if endpoint == YotsubaEndpoint::Threads {
                    r#"COALESCE(newv->'no',prev->'no')"#
                } else {
                    "coalesce(newv,prev)"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"jsonb_array_elements(threads)->'threads'"#
                } else {
                    "archive"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"jsonb_array_elements($2::jsonb)->'threads'"#
                } else {
                    "$2::jsonb"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"prev->'no' = (newv -> 'no')"#
                } else {
                    "prev = newv"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"or not prev->'last_modified' <@ (newv -> 'last_modified')"#
                } else {
                    ""
                },
                if endpoint == YotsubaEndpoint::Threads { "threads" } else { "archive" }
            ),

            // This query is only run ONCE at every startup
            // Running a JOIN to compare against the entire DB on EVERY INSERT/UPDATE
            // would not be that great. That is not done here.
            // This gets all the threads from cache, compares it to the new json to get
            // new + modified threads Then compares that result to the database
            // where a thread is deleted or archived, and takes only the threads
            // where's it's not deleted or archived.
            // This might be unwanted as the size of the database grows.
            YotsubaStatement::ThreadsCombined => format!(
                r#"
        select jsonb_agg(c) from (
          SELECT coalesce {1} as c from
            (select jsonb_array_elements({2}) as prev from metadata where board = $1)x
          full JOIN
            (select jsonb_array_elements({3}) as newv)z
          ON {4}
        )q
        left join
          (select no as nno from "{0}" where resto=0 and (archived_on is not null or deleted_on is not null))w
        ON c = nno
        where nno is null;
        "#,
                id.board,
                if endpoint == YotsubaEndpoint::Threads {
                    r#"(prev->'no', newv->'no')::bigint"#
                } else {
                    "(newv, prev)::bigint"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"jsonb_array_elements(threads)->'threads'"#
                } else {
                    "archive"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"jsonb_array_elements($2::jsonb)->'threads'"#
                } else {
                    "$2::jsonb"
                },
                if endpoint == YotsubaEndpoint::Threads {
                    r#"prev->'no' = (newv -> 'no')"#
                } else {
                    "prev = newv"
                }
            ),
            YotsubaStatement::UpdateThread => format!(
                r#"
            INSERT INTO "{board}" (
              no,sticky,closed,name,sub,com,filedeleted,spoiler,
              custom_spoiler,filename,ext,w,h,tn_w,tn_h,tim,time,md5,
              fsize, m_img,resto,trip,id,capcode,country,country_name,bumplimit,
              archived_on,imagelimit,semantic_url,replies,images,unique_ips,tag,since4pass,last_modified)
              SELECT
              no,sticky::int::bool,closed::int::bool,name,sub,com,filedeleted::int::bool,spoiler::int::bool,
              custom_spoiler,filename,ext,w,h,tn_w,tn_h,tim,time, (CASE WHEN length(q.md5)>20 and q.md5 IS NOT NULL THEN decode(REPLACE (q.md5, E'\\', '')::text, 'base64'::text) ELSE null::bytea END) AS md5,
              fsize, m_img::int::bool, resto,trip,q.id,capcode,COALESCE(country, troll_country) as country,country_name,bumplimit::int::bool,
              archived_on,imagelimit::int::bool,semantic_url,replies,images,unique_ips,
              tag,since4pass, extract(epoch from now())::bigint as last_modified
              FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts') q
              WHERE q.no IS NOT NULL
            ON CONFLICT (no{extra_constraint}) 
            DO
              UPDATE SET 
              no = excluded.no,
              sticky = excluded.sticky::int::bool,
              closed = excluded.closed::int::bool,
              name = excluded.name,
              sub = excluded.sub,
              com = excluded.com,
              filedeleted = excluded.filedeleted::int::bool,
              spoiler = excluded.spoiler::int::bool,
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
              m_img = excluded.m_img::int::bool,
              resto = excluded.resto,
              trip = excluded.trip,
              id = excluded.id,
              capcode = excluded.capcode,
              country = excluded.country,
              country_name = excluded.country_name,
              bumplimit = excluded.bumplimit::int::bool,
              archived_on = excluded.archived_on,
              imagelimit = excluded.imagelimit::int::bool,
              semantic_url = excluded.semantic_url,
              replies = excluded.replies,
              images = excluded.images,
              unique_ips = CASE WHEN excluded.unique_ips is not null THEN excluded.unique_ips ELSE "{board}".unique_ips END,
              tag = excluded.tag,
              since4pass = excluded.since4pass,
              last_modified = extract(epoch from now())::bigint
              WHERE excluded.no IS NOT NULL AND EXISTS (
              SELECT 
                "{board}".no,
                "{board}".sticky,
                "{board}".closed,
                "{board}".name,
                "{board}".sub,
                "{board}".com,
                "{board}".filedeleted,
                "{board}".spoiler,
                "{board}".custom_spoiler,
                "{board}".filename,
                "{board}".ext,
                "{board}".w,
                "{board}".h,
                "{board}".tn_w,
                "{board}".tn_h,
                "{board}".tim,
                "{board}".time,
                "{board}".md5,
                "{board}".fsize,
                "{board}".m_img,
                "{board}".resto,
                "{board}".trip,
                "{board}".id,
                "{board}".capcode,
                "{board}".country,
                "{board}".country_name,
                "{board}".bumplimit,
                "{board}".archived_on,
                "{board}".imagelimit,
                "{board}".semantic_url,
                "{board}".replies,
                "{board}".images,
                --"{board}".unique_ips,
                "{board}".tag,
                "{board}".since4pass
                WHERE "{board}".no IS NOT NULL
              EXCEPT
              SELECT 
                excluded.no,
                excluded.sticky,
                excluded.closed,
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
                excluded.bumplimit,
                excluded.archived_on,
                excluded.imagelimit,
                excluded.semantic_url,
                excluded.replies,
                excluded.images,
                --excluded.unique_ips,
                excluded.tag,
                excluded.since4pass
              WHERE excluded.no IS NOT NULL AND excluded.no = "{board}".no )"#,
                board = id.board,
                extra_constraint =
                    if matches!(id.engine, Database::TimescaleDB) { r#","time""# } else { "" }
            ),
            YotsubaStatement::Delete => format!(
                r#"
          UPDATE "{board}"
          SET deleted_on    = extract(epoch from now())::bigint,
            last_modified = extract(epoch from now())::bigint
          WHERE
          no = $1 AND deleted_on is NULL;
          "#,
                board = id.board
            ),
            YotsubaStatement::UpdateDeleteds => format!(
                r#"
            INSERT INTO "{board}" (no, time, resto)
            SELECT x.* FROM
              (
                SELECT no, time, resto FROM "{board}" where (no=$2 or resto=$2) and no >= (
                    (SELECT min(no) FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts'))
                ) order by no
              ) x
              --(SELECT * FROM "{board}" where no=$2 or resto=$2 order by no) x
            FULL JOIN
              (SELECT no, time, resto FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts')) z
              --(SELECT * FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts')) z
            ON x.no = z.no
            WHERE z.no is null
            ON CONFLICT (no{extra_constraint}) 
            DO
            UPDATE
            SET deleted_on = extract(epoch from now())::bigint,
              last_modified = extract(epoch from now())::bigint
            WHERE
            "{board}".deleted_on is NULL; "#,
                board = id.board,
                extra_constraint =
                    if matches!(id.engine, Database::TimescaleDB) { r#","time""# } else { "" }
            ),
            YotsubaStatement::InitType => r#"
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT typname FROM pg_type WHERE typname = 'schema_4chan') THEN
            CREATE TYPE "schema_4chan" AS (
              "no" bigint,
              sticky smallint,
              closed smallint,
              "now" text,
              "name" text,
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
              "time" bigint,
              "md5" text,
              fsize bigint,
              m_img smallint,
              resto int,
              trip text,
              id text,
              capcode text,
              country text,
              troll_country text,
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
              since4pass smallint
            );
          END IF;
        END
        $$;"#
                .into(),
            YotsubaStatement::InitViews => {
                let board = id.board;
                let safe_create_view = |n, stmt| {
                    format!(
                        r#"
        DO $$
        BEGIN
        CREATE VIEW "{}{}" AS
          {}
        EXCEPTION
        WHEN SQLSTATE '42P07' THEN
          NULL;
        END;
        $$;
        "#,
                        board, n, stmt
                    )
                };

                let main_view = |is_main| {
                    safe_create_view(
                        if is_main { "_asagi" } else { "_deleted" },
                        format!(
                            r#"
            SELECT
            no AS doc_id,
            (CASE WHEN md5 IS NOT NULL THEN no ELSE NULL END) AS media_id,
            0::smallint AS poster_ip, -- Unused in Asagi. Used in FF.
            no AS num,
            subnum, -- Unused in Asagi. Used in FF for ghost posts.
            (CASE WHEN resto=0 THEN no ELSE resto END) AS thread_num,
            (CASE WHEN resto=0 THEN true ELSE false END) AS op,
            "time" AS "timestamp",
            (CASE WHEN deleted_on IS NULL THEN 0 ELSE deleted_on END) AS "timestamp_expired",
            (CASE WHEN tim IS NOT NULL THEN (tim::text || 's.jpg') ELSE NULL END) AS preview_orig,
            (CASE WHEN tn_w IS NULL THEN 0 ELSE tn_w END) AS preview_w,
            (CASE WHEN tn_h IS NULL THEN 0 ELSE tn_h END) AS preview_h,
            (CASE WHEN filename IS NOT NULL THEN (filename::text || ext) ELSE NULL END) AS media_filename,
            (CASE WHEN w IS NULL THEN 0 ELSE w END) AS media_w,
            (CASE WHEN h IS NULL THEN 0 ELSE h END) AS media_h,
            (CASE WHEN fsize IS NULL THEN 0 ELSE fsize END) AS media_size,
            encode(md5, 'base64') AS media_hash,
            (CASE WHEN tim IS NOT NULL and ext IS NOT NULL THEN (tim::text || ext) ELSE NULL END) AS media_orig,
            (CASE WHEN spoiler IS NULL THEN false ELSE spoiler END) AS spoiler,
            (CASE WHEN deleted_on IS NULL THEN false ELSE true END) AS deleted,
            (CASE WHEN capcode='manager' OR capcode='Manager' THEN 'G' ELSE coalesce (upper(left(capcode, 1)),'N') end) AS capcode,
            NULL AS email, -- Used by Asagi but no longer in the API. Used by FF.
            name,
            trip,
            sub AS title,
            (select r29 from 
              regexp_replace (
              com, E'&#039;', E'\'', 'g') r0
              , regexp_replace(r0, E'&gt;', '>', 'g') r1
              , regexp_replace(r1, E'&lt;', '<', 'g') r2
              , regexp_replace(r2, E'&quot;', E'\"', 'g') r3
              , regexp_replace(r3, E'&amp;', E'&', 'g') r4
              , regexp_replace(r4, E'\\s*$', '', 'g') r5
              , regexp_replace(r5, E'^\\s*$', '', 'g') r6
              , regexp_replace(r6, E'<span class=\"capcodeReplies\"><span style=\"font-size: smaller;\"><span style=\"font-weight: bold;\">(?:Administrator|Moderator|Developer) Repl(?:y|ies):</span>.*?</span><br></span>', '', 'g') r7
              , regexp_replace(r7, E'\\[(/?(banned|moot|spoiler|code))]', '[\1:lit]', 'g') r8
              , regexp_replace(r8, E'<span class=\"abbr\">.*?</span>', '', 'g') r9
              , regexp_replace(r9, E'<table class=\"exif\"[^>]*>.*?</table>', '', 'g') r10
              , regexp_replace(r10, E'<br><br><small><b>Oekaki Post</b>.*?</small>', '', 'g') r11
              , regexp_replace(r11, E'<(?:b|strong) style=\"color:\\s*red;\">(.*?)</(?:b|strong)>', '[banned]\1[/banned]', 'g') r12
              , regexp_replace(r12, E'<div style=\"padding: 5px;margin-left: \\.5em;border-color: #faa;border: 2px dashed rgba\\(255,0,0,\\.1\\);border-radius: 2px\">(.*?)</div>', '[moot]\1[/moot]', 'g') r13
              , regexp_replace(r13, E'<span class=\"fortune\" style=\"color:(.*?)\"><br><br><b>(.*?)</b></span>', '\n\n[fortune color=\"\1\"]$2[/fortune]', 'g') r14
              , regexp_replace(r14, E'<(?:b|strong)>(.*?)</(?:b|strong)>', '[b]\1[/b]', 'g') r15
              , regexp_replace(r15, E'<pre[^>]*>', '[code]', 'g') r16
              , replace(r16, '</pre>', '[/code]') r17
              , regexp_replace(r17, E'<span class=\"math\">(.*?)</span>', '[math]\1[/math]', 'g') r18
              , regexp_replace(r18, E'<div class=\"math\">(.*?)</div>', '[eqn]\1[/eqn]', 'g') r19
              , regexp_replace(r19, E'<font class=\"unkfunc\">(.*?)</font>', '\1', 'g') r20
              , regexp_replace(r20, E'<span class=\"quote\">(.*?)</span>', '\1', 'g') r21
              , regexp_replace(r21, E'<span class=\"(?:[^\"]*)?deadlink\">(.*?)</span>', '\1', 'g') r22 
              , regexp_replace(r22, E'<a.*?>(.*?)</a>', '\1', 'g') r23 -- Changed for postgres regex
              , regexp_replace(r23, E'<span class=\"spoiler\"[^>]*>(.*?)</span>', '[spoiler]\1[/spoiler]', 'g') r24
              , regexp_replace(r24, E'<span class=\"sjis\">(.*?)</span>', '[shiftjis]\1[/shiftjis]', 'g') r25 
              , regexp_replace(r25, E'<s>', '[spoiler]', 'g') r26
              , regexp_replace(r26, E'</s>', '[/spoiler]', 'g') r27
              , regexp_replace(r27, E'<br\\s*/?>', E'\n', 'g') r28
              , regexp_replace(r28, E'<wbr>', '', 'g') r29
              ) AS comment,
            NULL AS delpass, -- Unused in Asagi. Used in FF.
            (CASE WHEN sticky IS NULL or sticky=false THEN false ELSE sticky END) AS sticky,
            (CASE WHEN (closed IS NOT NULL or closed=true) AND (archived_on is null) THEN closed ELSE FALSE END) AS locked,
            (CASE WHEN id='Developer' THEN 'Dev' ELSE id END) AS poster_hash, --not the same AS media_hash
            country AS poster_country,
            country_name AS poster_country_name,
            NULLIF(
                jsonb_strip_nulls(jsonb_build_object('uniqueIps', unique_ips::text, 'since4pass', since4pass::text, 'trollCountry', 
                    case when 
                    country = ANY ('{{AC,AN,BL,CF,CM,CT,DM,EU,FC,GN,GY,JH,KN,MF,NB,NZ,PC,PR,RE,TM,TR,UN,WP}}'::text[])
                    then
                    country
                    else
                    null
                    end
                ))::text, '{{}}') as exif, -- JSON in text format of uniqueIps, since4pass, and trollCountry. Has some deprecated fields but still used by Asagi and FF.
            (CASE WHEN archived_on IS NULL THEN false ELSE true END) AS archived,
            archived_on
            FROM "{board}"
            {extra};
            "#,
                            board = board,
                            extra = if is_main { "" } else { "WHERE deleted_on is not null" }
                        )
                    )
                };

                let board_threads = safe_create_view(
                    "_threads",
                    format!(
                        r#"
          SELECT
          no as thread_num,
          "time" as time_op,
          last_modified as time_last,
          last_modified as time_bump,
          (CASE WHEN subnum is not null then "time" else NULL END ) as time_ghost,
          (CASE WHEN subnum is not null then last_modified else NULL END )  as time_ghost_bump,
          last_modified as time_last_modified,
          (SELECT COUNT(no) FROM "{board}" re WHERE t.no = resto or t.no = no) as nreplies,
          (SELECT COUNT(md5) FROM "{board}" re WHERE t.no = resto or t.no = no) as nimages,
          (CASE WHEN sticky IS NULL or sticky=false THEN false ELSE sticky END) AS sticky,
          (CASE WHEN (closed IS NOT NULL or closed=true) AND (archived_on is null) THEN closed ELSE FALSE END) AS locked
          from "{board}" t where resto=0;
        "#,
                        board = board
                    )
                );

                let board_users = safe_create_view(
                    "_users",
                    format!(
                        r#"
        SELECT
          ROW_NUMBER() OVER (ORDER by min(t.time)) AS user_id,
          t.n AS name,
          t.tr AS trip,
          MIN(t.time) AS firstseen, COUNT(*) AS postcount
          FROM (SELECT *, COALESCE(name,'') AS n, COALESCE(trip,'') AS tr
        FROM "{board}") t GROUP BY t.n,t.tr;
        "#,
                        board = board
                    )
                );

                let board_images = safe_create_view(
                    "_images",
                    format!(
                        r#"
          SELECT ROW_NUMBER() OVER(ORDER by x.media) AS media_id, * FROM (
            SELECT
              ENCODE(md5, 'base64') AS media_hash,
              MAX(tim)::text || max(ext) as media,
              (CASE WHEN MAX(resto) = 0 THEN MAX(tim)::text || 's.jpg' ELSE NULL END) AS preview_op,
              (CASE WHEN MAX(resto) != 0 THEN MAX(tim)::text || 's.jpg' ELSE NULL END) AS preview_reply,
              COUNT(md5)::int AS total,
              0::smallint AS banned, --unused in asagi, used in FF
              encode(sha256, 'hex')::text || max(ext) as media_sha256,
              (CASE WHEN MAX(resto) = 0 THEN encode(sha256, 'hex')::text || 's.jpg' ELSE NULL END) AS preview_op_sha256,
              (CASE WHEN MAX(resto) != 0 THEN encode(sha256t, 'hex')::text || 's.jpg' ELSE NULL END) AS preview_reply_sha256
            FROM "{board}" WHERE md5 IS NOT NULL GROUP BY md5, sha256, sha256t)x;
        "#,
                        board = board
                    )
                );

                let board_daily = safe_create_view(
                    "_daily",
                    format!(
                        r#"
        SELECT
          MIN(t.no) AS firstpost,
          t.day AS day,
          COUNT(*) AS posts, COUNT(md5) AS images, COUNT(CASE WHEN name ~* '.*sage.*' THEN name ELSE  NULL END) AS sage,
          COUNT(CASE WHEN name = 'Anonymous' AND trip IS NULL THEN name ELSE NULL END) AS anons, COUNT(trip) AS trips,
          COUNT(CASE WHEN COALESCE(name <> 'Anonymous' AND trip IS NULL, TRUE) THEN name ELSE NULL END) AS names
        FROM (SELECT *, (FLOOR(time/86400)*86400)::bigint AS day FROM "{board}")t GROUP BY t.day ORDER BY day;
        "#,
                        board = board
                    )
                );

                format!(
                    r#"
        {1}
      
        {2}
      
        {3}
      
        {4}
      
        {5}
      
        {6}
      
        CREATE INDEX IF NOT EXISTS "idx_{0}_time" on "{0}"(((floor((("{0}"."time" / 86400))::double precision) * '86400'::double precision)::bigint));
      
        CREATE TABLE IF NOT EXISTS index_counters (
                      id character varying(50) NOT NULL,
                      val integer NOT NULL,
                      PRIMARY KEY (id));
        "#,
                    board,
                    main_view(true),
                    main_view(false),
                    board_threads,
                    board_users,
                    board_images,
                    board_daily
                )
            }
            YotsubaStatement::Medias => format!(
                r#"
        SELECT * FROM "{board}"
        WHERE (md5 is not null) {media_mode} AND filedeleted IS NULL AND (no=$1 or resto=$1)
        ORDER BY no desc;"#,
                board = id.board,
                media_mode = match id.media_mode {
                    YotsubaStatement::UpdateHashThumbs => "AND (sha256t IS NULL)",
                    YotsubaStatement::UpdateHashMedia => "AND (sha256 IS NULL)",
                    _ => "AND (sha256 IS NULL OR sha256t IS NULL)"
                }
            ),
            YotsubaStatement::UpdateHashMedia | YotsubaStatement::UpdateHashThumbs => format!(
                r#"
        UPDATE "{0}"
        SET last_modified = extract(epoch from now())::bigint,
              "{1}" = $2
        WHERE
        no = $1 AND "{1}" IS NULL;
        "#,
                id.board,
                if statement == YotsubaStatement::UpdateHashThumbs {
                    format!("{}t", id.hash_type)
                } else {
                    format!("{}", id.hash_type)
                }
            )
        }
    }
}

/// For the rest of [`YotsubaStatement`]
#[async_trait]
impl Query<Statement, Row> for Client {
    async fn first(
        &self, statement: YotsubaStatement, id: &QueryIdentifier,
        statements: &StatementStore<Statement>, item: Option<&[u8]>, no: Option<u64>
    ) -> Result<u64>
    {
        log::debug!("|Query| Running: {} /{}/", statement, id.board);
        let id = QueryIdentifier { media_mode: statement, ..id.clone() };
        let board = id.board;
        let received_statement = statements
            .get(&id)
            .ok_or_else(|| anyhow!("|Query::{}| Empty statement from id: {:?}", statement, id));
        let item = item.ok_or_else(|| anyhow!("|Query::{}| Empty `json` item received", statement));
        let no = no.ok_or_else(|| anyhow!("|Query::{}| Empty `no` received", statement));
        match statement {
            YotsubaStatement::InitSchema
            | YotsubaStatement::InitType
            | YotsubaStatement::InitMetadata
            | YotsubaStatement::InitBoard
            | YotsubaStatement::InitViews => {
                self.batch_execute(&self.inquiry(statement, id)).await?;
                Ok(1)
            }
            YotsubaStatement::UpdateMetadata => Ok(self
                .execute(received_statement?, &[
                    &board.to_string(),
                    &serde_json::from_slice::<serde_json::Value>(item?)?
                ])
                .await?),
            YotsubaStatement::UpdateThread => Ok(self
                .execute(received_statement?, &[&serde_json::from_slice::<serde_json::Value>(
                    item?
                )?])
                .await?),
            YotsubaStatement::Delete =>
                Ok(self.execute(received_statement?, &[&i64::try_from(no?)?]).await?),
            YotsubaStatement::UpdateDeleteds => Ok(self
                .execute(received_statement?, &[
                    &serde_json::from_slice::<serde_json::Value>(item?)?,
                    &i64::try_from(no?)?
                ])
                .await?),
            YotsubaStatement::UpdateHashMedia | YotsubaStatement::UpdateHashThumbs => {
                self.execute(received_statement?, &[&(no? as i64), &item?]).await?;
                Ok(1)
            }
            // YotsubaStatement::Medias => {},
            // YotsubaStatement::Threads => {},
            // YotsubaStatement::ThreadsModified => {},
            // YotsubaStatement::ThreadsCombined => {},
            YotsubaStatement::Metadata => Ok(self
                .query(received_statement?, &[&board.to_string()])
                .await
                .ok()
                .filter(|re| !re.is_empty())
                .map(|re| re[0].try_get(0))
                .ok_or_else(|| anyhow!("|Query::{}| Empty entry in metadata", statement))?
                .map(|b: bool| if b { 1u64 } else { 0u64 })?),
            _ => Err(anyhow!("|Query| Unknown statement: {}", statement))
        }
    }

    async fn get_list(
        &self, statement: YotsubaStatement, id: &QueryIdentifier,
        statements: &StatementStore<Statement>, item: Option<&[u8]>, _no: Option<u64>
    ) -> Result<Queue>
    {
        log::debug!("|Query| Running: {} /{}/", statement, id.board);
        // This `get_list` method could be run with any one of the following:
        if !matches!(
            statement,
            YotsubaStatement::Threads
                | YotsubaStatement::ThreadsModified
                | YotsubaStatement::ThreadsCombined
        ) {
            return Err(anyhow!(
                "|Query::{}| Unknown statement: {}",
                YotsubaStatement::Threads,
                statement
            ));
        }
        // Inside `create_statements` we use `media_mode` as the `statement`
        // Se we have to put in the right `statement` to get the right id

        let id = QueryIdentifier { media_mode: statement, ..id.clone() };
        // log::info!("{:?}", &id);
        let endpoint = id.endpoint;
        let board = id.board;

        let item = item.ok_or_else(|| anyhow!("|Query::{}| Empty `json` item received", statement));

        // Return the archive thread as a `Queue` because it's already an array
        if matches!(endpoint, YotsubaEndpoint::Archive)
            && matches!(statement, YotsubaStatement::Threads)
        {
            return Ok(serde_json::from_slice(item?)?);
        }

        let json = serde_json::from_slice::<serde_json::Value>(item?)?;
        // let u: Vec<Threads> = serde_json::from_slice(item.unwrap())?; // artifact from mysql
        let received_statement = statements
            .get(&id)
            .ok_or_else(|| anyhow!("|Query::{}| Empty statement from id: {:?}", statement, &id));
        let res: Queue = if matches!(statement, YotsubaStatement::Threads) {
            self.query_one(received_statement?, &[&json]).await
        } else {
            self.query_one(received_statement?, &[&board.to_string(), &json]).await
        }
        .map(|row| row.try_get(0))?
        .map(|r: Option<serde_json::Value>| r)?
        .map(|res| serde_json::from_value::<HashSet<Option<u64>>>(res))
        .ok_or_else(|| {
            anyhow!("|Query::{}| Empty or null in getting from `{}` endpoint", statement, endpoint)
        })?
        .map(|v| v.into_iter().filter(Option::is_some).map(Option::unwrap).collect())?;
        Ok(res)
    }

    async fn get_rows(
        &self, statement: YotsubaStatement, id: &QueryIdentifier,
        statements: &StatementStore<Statement>, _item: Option<&[u8]>, no: Option<u64>
    ) -> Result<Vec<Row>>
    {
        log::debug!("|Query| Running: {} /{}/", statement, id.board);
        if !matches!(statement, YotsubaStatement::Medias) {
            return Err(anyhow!(
                "|Query::{}| Unknown statement: {}",
                YotsubaStatement::Medias,
                statement
            ));
        }
        let id = QueryIdentifier { media_mode: statement, ..id.clone() };
        let no = no.ok_or_else(|| anyhow!("|Query::{}| Empty `no` received", statement));
        let received_statement = statements
            .get(&id)
            .ok_or_else(|| anyhow!("|Query::{}| Empty statement from id: {:?}", statement, id));
        Ok(self.query(received_statement?, &[&(no? as i64)]).await?)
    }

    async fn create_statements(
        &self, engine: Database, endpoint: YotsubaEndpoint, board: crate::enums::YotsubaBoard
    ) -> StatementStore<Statement> {
        let mut statement_store = std::collections::HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        let hash_type = crate::enums::YotsubaHash::Sha256;
        if matches!(endpoint, YotsubaEndpoint::Media) {
            for statement in statements {
                match statement {
                    YotsubaStatement::Medias
                    | YotsubaStatement::UpdateHashMedia
                    | YotsubaStatement::UpdateHashThumbs => {
                        let id = QueryIdentifier::new(
                            engine, endpoint, board, None, None, hash_type, statement
                        );
                        statement_store.insert(
                            id.clone(),
                            self.prepare(&self.inquiry(statement, id)).await.unwrap()
                        );
                    }
                    _ => {}
                }
            }
            return statement_store;
        }
        for statement in statements {
            match statement {
                YotsubaStatement::Medias
                | YotsubaStatement::UpdateHashMedia
                | YotsubaStatement::UpdateHashThumbs
                | YotsubaStatement::InitSchema
                | YotsubaStatement::InitType
                | YotsubaStatement::InitMetadata
                | YotsubaStatement::InitBoard
                | YotsubaStatement::InitViews => {}
                _ => {
                    let id = QueryIdentifier::new(
                        engine,
                        endpoint,
                        board,
                        None,
                        None,
                        crate::enums::YotsubaHash::Sha256,
                        statement
                    );
                    statement_store.insert(
                        id.clone(),
                        self.prepare(&self.inquiry(statement, id)).await.unwrap()
                    );
                }
            }
        }

        statement_store
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::enums::{YotsubaBoard, YotsubaHash};
    use once_cell::sync::Lazy;
    #[allow(unused_imports)]
    #[cfg(test)]
    use pretty_assertions::{assert_eq, assert_ne};
    use serde_json::json;
    static BOARD: Lazy<YotsubaBoard> = Lazy::new(|| YotsubaBoard::a);
    const DB_URL: &str = "postgresql://postgres:zxc@localhost:5432/fdb2";
    const SCHEMA: &str = "asagi";
    const ENGINE: Database = Database::PostgreSQL;
    // const CHARSET: &str = "utf8";

    enum JsonType {
        Unknown,
        Valid,
        DeprecatedFields,
        AddedFields,
        MixedFields
    }

    macro_rules! send_thread_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[tokio::test]
                async fn $name() -> Result<()> {
                    let ( endpoint, board, mode, json_type) = $value;
                    test_send_single_thread(endpoint, board, mode, json_type).await?;
                    // assert_eq!(expected, fib(input));
                    Ok(())
                }
            )*
        }
    }
    macro_rules! get_threads_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[tokio::test]
                // #[should_panic(expected = "|threads| Empty or null in getting threads")] // for unknowns
                async fn $name() -> Result<()> {
                    let ( endpoint, board, mode, json_type) = $value;
                    test_get_threads(endpoint, board, mode, json_type).await?;
                    // assert_eq!(expected, fib(input));
                    Ok(())
                }
            )*
        }
    }

    macro_rules! get_threads_tests_panic {
        ($($name:ident: $value:expr,)*) => {
            $(
                // #[should_panic(expected = "|threads| Empty or null in getting threads")] // for unknowns
                #[tokio::test]
                #[should_panic]
                async fn $name() {
                    let (endpoint, board, mode, json_type) = $value;
                    test_get_threads(endpoint, board, mode, json_type).await.unwrap();
                    // assert_eq!(expected, fib(input));
                }
            )*
        }
    }
    #[cfg(test)]
    send_thread_tests! {
        // Send single thread
        send_single_thread_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::Valid),
        send_single_thread_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::DeprecatedFields),
        send_single_thread_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::AddedFields),
        send_single_thread_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::MixedFields),
    }

    #[cfg(test)]
    get_threads_tests! {
        // threads.json
        get_threads_send_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::Valid),
        get_threads_send_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::AddedFields),
        get_threads_send_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::MixedFields),

        get_threads_modified_send_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Valid),
        get_threads_modified_send_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        get_threads_modified_send_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        get_threads_modified_send_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::MixedFields),

        get_threads_combined_send_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Valid),
        get_threads_combined_send_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),
        get_threads_combined_send_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),
        get_threads_combined_send_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),

        get_archive_send_valid_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::Valid),
        get_archive_modified_send_valid_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Valid),
        get_archive_combined_send_valid_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Valid),

        get_archive_send_deprecated_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::DeprecatedFields),
        get_archive_modified_send_deprecated_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        get_archive_combined_send_deprecated_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),

    }

    #[cfg(test)]
    get_threads_tests_panic! {
        // threads.json
        get_threads_send_unknown_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::Unknown),
        get_threads_send_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::DeprecatedFields),
        get_threads_modified_send_unknown_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Unknown),
        get_threads_combined_send_unknown_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Unknown),

        // archive.json
        get_archive_send_added_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::AddedFields),
        get_archive_modified_send_added_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        get_archive_combined_send_added_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),

        get_archive_send_mixed_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::MixedFields),
        get_archive_modified_send_mixed_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::MixedFields),
        get_archive_combined_send_mixed_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),

        get_archive_send_unknown_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::Unknown),
        get_archive_modified_send_unknown_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Unknown),
        get_archive_combined_send_unknown_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Unknown),
    }

    async fn test_get_threads(
        endpoint: YotsubaEndpoint, board: YotsubaBoard, mode: YotsubaStatement, json_type: JsonType
    ) -> Result<Queue> {
        let _json = match json_type {
            JsonType::Unknown =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!({"test":1, "test2":2, "test3":3})
                } else {
                    json!(["1243234", "5645756", "75686786", "456454325", "test", "test1"])
                },
            JsonType::Valid =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349 },
                            { "no": 196656555, "last_modified": 1576266881, "replies": 6 },
                            { "no": 196654076, "last_modified": 1576266880, "replies": 191 },
                            { "no": 196637792, "last_modified": 1576266880, "replies": 233 },
                            { "no": 196647457, "last_modified": 1576266880, "replies": 110 },
                            { "no": 196624742, "last_modified": 1576266873, "replies": 103 },
                            { "no": 196656097, "last_modified": 1576266868, "replies": 7 },
                            { "no": 196645355, "last_modified": 1576266866, "replies": 361 },
                            { "no": 196655995, "last_modified": 1576266867, "replies": 3 },
                            { "no": 196655998, "last_modified": 1576266860, "replies": 5 },
                            { "no": 196652782, "last_modified": 1576266858, "replies": 42 },
                            { "no": 196656536, "last_modified": 1576266853, "replies": 5 },
                            { "no": 196621039, "last_modified": 1576266853, "replies": 189 },
                            { "no": 196640441, "last_modified": 1576266851, "replies": 495 },
                            { "no": 196637247, "last_modified": 1576266850, "replies": 101 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 29 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 387 },
                            { "no": 196648390, "last_modified": 1576266844, "replies": 36 },
                            { "no": 196651494, "last_modified": 1576266832, "replies": 10 },
                            { "no": 196656773, "last_modified": 1576266827, "replies": 0 },
                            { "no": 196653207, "last_modified": 1576266827, "replies": 20 },
                            { "no": 196643737, "last_modified": 1576266825, "replies": 82 },
                            { "no": 196626714, "last_modified": 1576266824, "replies": 467 },
                            { "no": 196654299, "last_modified": 1576266821, "replies": 9 },
                            { "no": 196636729, "last_modified": 1576266819, "replies": 216 },
                            { "no": 196655015, "last_modified": 1576266819, "replies": 3 },
                            { "no": 196642084, "last_modified": 1576266818, "replies": 233 },
                            { "no": 196649533, "last_modified": 1576266816, "replies": 122 },
                            { "no": 196640416, "last_modified": 1576266806, "replies": 381 },
                            { "no": 196656724, "last_modified": 1576266794, "replies": 1 }
                          ]
                        }
                    ])
                } else {
                    json!([1243234, 5645756, 75686786, 456454325, 231412, 564576567, 34523234])
                },
            JsonType::DeprecatedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no_more": 196649146, "last_modified": 1576266882, "replies": 349 },
                            { "no_more": 196656555, "last_modified": 1576266881, "replies": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no_more": 196650664, "last_modified": 1576266846, "replies": 387},
                            { "no_more": 196646963, "last_modified": 1576266845, "replies": 487 }
                          ]
                        }
                    ])
                } else {
                    json!([])
                },
            JsonType::AddedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349, "new_field": 349 },
                            { "no": 196656555, "last_modified": 1576266881, "replies": 6,  "new_field": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 387, "new_field": 387 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 487, "new_field": 487 },
                            { "no": 196648390, "last_modified": 1576266844, "replies": 36 , "new_field": 36 }
                          ]
                        }
                    ])
                } else {
                    json!([{}, "1243234", "5645756", "75686786", "456454325", "test", "test1"])
                },
            JsonType::MixedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349, "new_field": 349 },
                            { "no_more": 196656555, "last_modified": 1576266881, "replies": 6,  "new_field": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 387, "new_field": 387 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 487, "new_field": 487 },
                            { "no_more": 196648390, "last_modified": 1576266844, "replies": 36 , "new_field": 36 }
                          ]
                        }
                    ])
                } else {
                    json!(["1243234", 5645756, "75686786", 456454325, "test", "test1"])
                },
        };

        let json = serde_json::to_vec(&_json).unwrap();

        let (db_client, connection) =
            tokio_postgres::connect(DB_URL, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        // Set search path
        db_client.query(format!(r#"SET search_path TO "{}";"#, SCHEMA).as_str(), &[]).await?;

        let statements = db_client.create_statements(ENGINE, endpoint, board).await;

        // let gen_id = |statement| {
        //     QueryIdentifier::new(ENGINE, endpoint, board, Some(SCHEMA.into()),
        // Some(CHARSET.into()), YotsubaHash::Sha256, statement) };

        // This is the ID used by `create_statements` for the get`threads_*` variants.
        let id = &QueryIdentifier::new(
            ENGINE,
            endpoint,
            board,
            None,
            None,
            YotsubaHash::Sha256,
            YotsubaStatement::Medias
        );

        match mode {
            YotsubaStatement::Threads
            | YotsubaStatement::ThreadsModified
            | YotsubaStatement::ThreadsCombined =>
                Ok(db_client.get_list(mode, &id, &statements, Some(json.as_slice()), None).await?),
            _ => Err(anyhow!("Error. Entered this test with : `{}` statement", mode))
        }
    }
    async fn test_send_single_thread(
        endpoint: YotsubaEndpoint, board: YotsubaBoard, mode: YotsubaStatement, json_type: JsonType
    ) -> Result<u64> {
        let _json = match json_type {
            JsonType::Unknown => json!({"test":1, "test2":2, "test3":3}),
            JsonType::Valid => json!(
                {
                    "posts": [{
                      "no": 5679879,
                      "sticky": 1,
                      "closed": 1,
                      "now": r#"12\/31\/18(Mon)17:05:48"#,
                      "name": "Anonymous",
                      "sub": r#"Welcome to \/po\/!"#,
                      "com": r#"Welcome to \/po\/! We specialize in origami, papercraft, and everything that\u2019s relevant to paper engineering. This board is also an great library of relevant PDF books and instructions, one of the best resource of its kind on the internet.<br><br>Questions and discussions of papercraft and origami are welcome. Threads for topics covered by paper engineering in general are also welcome, such as kirigami, bookbinding, printing technology, sticker making, gift boxes, greeting cards, and more.<br><br>Requesting is permitted, even encouraged if it\u2019s a good request; fulfilled requests strengthens this board\u2019s role as a repository of books and instructions. However do try to keep requests in relevant threads, if you can.<br><br>\/po\/ is a slow board! Do not needlessly bump threads."#,
                      "filename": "yotsuba_folding",
                      "ext": ".png",
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "md5": "uZUeZeB14FVR+Mc2ScHvVA==",
                      "fsize": 516657,
                      "resto": 0,
                      "capcode": "mod",
                      "semantic_url": "welcome-to-po",
                      "replies": 2,
                      "images": 2,
                      "unique_ips": 1
                    }]}
            ),
            JsonType::DeprecatedFields => json!(
                {
                    "posts": [{
                      "no": 4588723,
                      "sticky": 1,
                      "closed": 1,
                      "ext": ".png",
                      "resto": 123457,
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "unique_ips": 1
                    }]}
            ),
            JsonType::AddedFields => json!(
                {
                    "posts": [{
                      "no": 462537,
                      "sticky": 1,
                      "closed": 1,
                      "now": r#"12\/31\/18(Mon)17:05:48"#,
                      "name": "Anonymous",
                      "sub": "Wrg!",
                      "com": "sdf",
                      "filename": "yotsuba_folding",
                      "ext": ".png",
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "md5": "uZUeZeB14FVR+Mc2ScHvVA==",
                      "fsize": 516657,
                      "resto": 0,
                      "capcode": "mod",
                      "semantic_url": "welcome-to-po",
                      "replies": 2,
                      "images": 2,
                      "unique_ips": 1,
                      "test": 1,
                      "added": 1
                    }]}
            ),
            JsonType::MixedFields => json!(
                {
                    "posts": [{
                      "no": 6745672,
                      "sticky": 1,
                      "closed": 1,
                      "ext": ".png",
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "resto": 123457,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "unique_ips": 1,
                      "test": 1,
                      "test2": 1
                    }]}
            )
        };

        let json = serde_json::to_vec(&_json).unwrap();

        let (db_client, connection) =
            tokio_postgres::connect(DB_URL, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        // Set search path
        db_client.query(format!(r#"SET search_path TO "{}";"#, SCHEMA).as_str(), &[]).await?;

        let statements = db_client.create_statements(ENGINE, endpoint, board).await;

        // This is the ID used by `create_statements` for the get`threads_*` variants.
        let id = &QueryIdentifier::new(
            ENGINE,
            endpoint,
            board,
            None,
            None,
            YotsubaHash::Sha256,
            YotsubaStatement::Medias
        );

        match mode {
            YotsubaStatement::UpdateThread =>
                Ok(db_client.first(mode, &id, &statements, Some(json.as_slice()), None).await?),
            _ => Err(anyhow!("Error. Entered this test with : `{}` statement", mode))
        }
    }
}
