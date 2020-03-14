use crate::{sql::*, YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::{collections::VecDeque, convert::TryFrom};
use tokio_postgres::Statement;

/// PostgreSQL version of the schema. This is also the default one used.  
/// If another schema is thought of, feel free to use the `SchemaTrait` implement it.
#[derive(Debug, Copy, Clone)]
pub struct Schema;

impl Schema {
    pub fn new() -> Self {
        Self {}
    }
}

/// PostgreSQL version is using tokio_postgres
#[async_trait]
impl SqlQueries for tokio_postgres::Client {
    // async fn prepare(&self, query: &str) -> Result<Statement, tokio_postgres::error::Error> {
    //   self.prepare(query).await
    // }

    // async fn prepare<R>(&self, query: &str) -> Result<PrepareStatment<R>> {
    //   Ok(self.prepare(query).await.map(|z| PrepareStatment::PostgreSQL(z))?)
    // }

    async fn init_type(&self, schema: &str) -> Result<u64, tokio_postgres::error::Error> {
        self.execute(Schema::new().init_type(schema).as_str(), &[]).await
    }

    async fn init_schema(&self, schema: &str) {
        self.batch_execute(&Schema::new().init_schema(schema).as_str())
            .await
            .expect(&format!("Err creating schema: {}", schema));
    }

    async fn init_metadata(&self) {
        self.batch_execute(&Schema::new().init_metadata()).await.expect("Err creating metadata");
    }

    async fn init_board(&self, board: YotsubaBoard, schema: &str) {
        self.batch_execute(&Schema::new().init_board(board, schema))
            .await
            .expect(&format!("Err creating schema: {}", board));
    }

    async fn init_views(&self, board: YotsubaBoard, schema: &str) {
        self.batch_execute(&Schema::new().init_views(schema, board))
            .await
            .expect("Err create views");
    }

    async fn update_metadata(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                             board: YotsubaBoard, item: &[u8])
                             -> Result<u64>
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateMetadata };
        let statement = statements.get(&id).unwrap();
        Ok(self.execute(statement, &[&board.to_string(),
                                     &serde_json::from_slice::<serde_json::Value>(item)?])
               .await?)
    }

    async fn medias(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                    board: YotsubaBoard, no: u32)
                    -> Result<Vec<tokio_postgres::row::Row>, tokio_postgres::error::Error>
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Medias };
        let statement = statements.get(&id).unwrap();
        self.query(statement, &[&(no as i64)]).await
    }

    async fn update_hash(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                         board: YotsubaBoard, no: u64, hash_type: YotsubaStatement,
                         hashsum: Vec<u8>)
    {
        // info!("Creating Identifier");
        let id = YotsubaIdentifier { endpoint, board, statement: hash_type };
        let statement = statements.get(&id).unwrap();
        // info!("Executing");
        self.execute(statement, &[&(no as i64), &hashsum])
            .await
            .expect("Err executing sql: update_hash");
    }

    /// Mark a single post as deleted.
    async fn delete(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                    board: YotsubaBoard, no: u32)
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Delete };
        let statement = statements.get(&id).unwrap();
        self.execute(statement, &[&i64::try_from(no).unwrap()])
            .await
            .expect("Err executing sql: delete");
    }

    /// Mark posts from a thread where it's deleted.
    async fn update_deleteds(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                             board: YotsubaBoard, thread: u32, item: &[u8])
                             -> Result<u64>
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateDeleteds };
        let statement = statements.get(&id).unwrap();
        Ok(self.execute(statement, &[&serde_json::from_slice::<serde_json::Value>(item)?,
                                     &i64::try_from(thread).unwrap()])
               .await?)
    }

    async fn update_thread(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                           board: YotsubaBoard, item: &[u8])
                           -> Result<u64>
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateThread };
        let statement = statements.get(&id).unwrap();
        Ok(self.execute(statement, &[&serde_json::from_slice::<serde_json::Value>(item)?]).await?)
    }

    async fn metadata(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                      board: YotsubaBoard)
                      -> bool
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Metadata };
        let statement = statements.get(&id).unwrap();
        self.query(statement, &[&board.to_string()])
            .await
            .ok()
            .filter(|re| !re.is_empty())
            .map(|re| re[0].try_get(0).ok())
            .flatten()
            .unwrap_or(false)
    }

    async fn threads(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                     board: YotsubaBoard, item: &[u8])
                     -> Result<VecDeque<u32>>
    {
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Threads };
        let statement = statements.get(&id).unwrap();
        let i = serde_json::from_slice::<serde_json::Value>(item)?;
        Ok(self.query(statement, &[&i])
               .await
               .ok()
               .filter(|re| !re.is_empty())
               .map(|re| re[0].try_get(0).ok())
               .flatten()
               .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
               .flatten()
               .ok_or_else(|| anyhow!("Error in get_threads_list"))?)
    }

    /// This query is only run ONCE at every startup
    /// Running a JOIN to compare against the entire DB on every INSERT/UPDATE
    /// would not be that great. That is not done here.
    /// This gets all the threads from cache, compares it to the new json to get
    /// new + modified threads Then compares that result to the database
    /// where a thread is deleted or archived, and takes only the threads
    /// where's it's not deleted or archived
    async fn threads_combined(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                              board: YotsubaBoard, new_threads: &[u8])
                              -> Result<VecDeque<u32>>
    {
        let id =
            YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::ThreadsCombined };
        let statement = statements.get(&id).unwrap();
        let i = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        Ok(self.query(statement, &[&board.to_string(), &i])
               .await
               .ok()
               .filter(|re| !re.is_empty())
               .map(|re| re[0].try_get(0).ok())
               .flatten()
               .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
               .flatten()
               .ok_or_else(|| anyhow!("Error in get_combined_threads"))?)
    }

    /// Combine new and prev threads.json into one. This retains the prev
    /// threads (which the new json doesn't contain, meaning they're either
    /// pruned or archived).  That's especially useful for boards without
    /// archives. Use the WHERE clause to select only modified threads. Now
    /// we basically have a list of deleted and modified threads.
    /// Return back this list to be processed.
    /// Use the new threads.json as the base now.
    async fn threads_modified(&self, board: YotsubaBoard, new_threads: &[u8],
                              statement: &Statement)
                              -> Result<VecDeque<u32>>
    {
        let i = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        Ok(self.query(statement, &[&board.to_string(), &i])
               .await
               .ok()
               .filter(|re| !re.is_empty())
               .map(|re| re[0].try_get(0).ok())
               .flatten()
               .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
               .flatten()
               .ok_or_else(|| anyhow!("Error in get_combined_threads"))?)
    }
}

impl SchemaTrait for Schema {
    fn init_schema(&self, schema: &str) -> String {
        format!(
                r#"
    CREATE SCHEMA IF NOT EXISTS "{0}";
    SET search_path TO "{0}";
    "#,
                schema
        )
    }

    fn init_metadata(&self) -> String {
        format!(
                r#"
        CREATE TABLE IF NOT EXISTS metadata (
                board text NOT NULL,
                threads jsonb,
                archive jsonb,
                PRIMARY KEY (board),
                CONSTRAINT board_unique UNIQUE (board));

        CREATE INDEX IF NOT EXISTS metadata_board_idx on metadata(board);
        "#
        )
    }

    /// This will find an already existing post
    fn delete(&self, schema: &str, board: YotsubaBoard) -> String {
        format!(
                r#"
          UPDATE "{0}"."{1}"
          SET deleted_on    = extract(epoch from now())::bigint,
              last_modified = extract(epoch from now())::bigint
          WHERE
            no = $1 AND deleted_on is NULL;
          "#,
                schema, board
        )
    }

    fn update_deleteds(&self, schema: &str, board: YotsubaBoard) -> String {
        // This will find an already existing post due to the WHERE clause, meaning it's
        // ok to only select no
        format!(
                r#"
        INSERT INTO "{0}"."{1}" (no, time, resto)
            SELECT x.* FROM
                (SELECT no, time, resto FROM "{0}"."{1}" where no=$2 or resto=$2 order by no) x
                --(SELECT * FROM "{0}"."{1}" where no=$2 or resto=$2 order by no) x
            FULL JOIN
                (SELECT no, time, resto FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts')) z
                --(SELECT * FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts')) z
            ON x.no = z.no
            WHERE z.no is null
      ON CONFLICT (no) 
      DO
          UPDATE
          SET deleted_on = extract(epoch from now())::bigint,
              last_modified = extract(epoch from now())::bigint
          WHERE
            "{0}"."{1}".deleted_on is NULL;
      "#,
                schema, board
        )
    }

    /// This will find an already existing post
    fn update_hash(&self, board: YotsubaBoard, hash_type: YotsubaHash, thumb: YotsubaStatement)
                   -> String {
        format!(
                r#"
      UPDATE "{0}"
      SET last_modified = extract(epoch from now())::bigint,
                  "{1}" = $2
      WHERE
        no = $1 AND "{1}" IS NULL;
      "#,
                board,
                if thumb == YotsubaStatement::UpdateHashThumbs {
                    format!("{}t", hash_type)
                } else {
                    format!("{}", hash_type)
                }
        )
    }

    fn update_metadata(&self, schema: &str, column: YotsubaEndpoint) -> String {
        format!(
                r#"
      INSERT INTO "{0}".metadata(board, {1})
          VALUES ($1, $2::jsonb)
          ON CONFLICT (board)
          DO UPDATE
              SET {1} = $2::jsonb;
      "#,
                schema, column
        )
    }

    fn medias(&self, board: YotsubaBoard, media_mode: YotsubaStatement) -> String {
        format!(r#"
                SELECT * FROM "{0}"
                WHERE (md5 is not null) {1} AND filedeleted IS NULL AND (no=$1 or resto=$1)
                ORDER BY no desc;"#,
                board,
                match media_mode {
                    YotsubaStatement::UpdateHashThumbs => "AND (sha256t IS NULL)",
                    YotsubaStatement::UpdateHashMedia => "AND (sha256 IS NULL)",
                    _ => "AND (sha256 IS NULL OR sha256t IS NULL)"
                })
    }

    fn threads_modified(&self, schema: &str, endpoint: YotsubaEndpoint) -> String {
        format!(
                r#"
      SELECT (
        CASE WHEN new_hash IS DISTINCT FROM prev_hash THEN
        (
          SELECT jsonb_agg({1}) from
          (select jsonb_array_elements({2}) as prev from "{0}".metadata where board = $1)x
          full JOIN
          (select jsonb_array_elements({3}) as newv)z
          ON {4}
          where newv is null or prev is null {5}
        ) END
      ) FROM 
      (SELECT sha256(decode({6} #>> '{{}}', 'escape')) as prev_hash from "{0}".metadata where board=$1) w
        FULL JOIN
      (SELECT sha256(decode($2::jsonb #>> '{{}}', 'escape')) as new_hash) q
      ON TRUE;
      "#,
                schema,
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
        )
    }

    fn threads(&self) -> String {
        "SELECT jsonb_agg(newv->'no')
          FROM
          (SELECT jsonb_array_elements(jsonb_array_elements($1::jsonb)->'threads') as newv)z".to_string()
    }

    fn metadata(&self, schema: &str, column: YotsubaEndpoint) -> String {
        format!(r#"select CASE WHEN {1} is not null THEN true ELSE false END from "{0}".metadata where board = $1"#,
                schema, column)
    }

    fn threads_combined(&self, schema: &str, board: YotsubaBoard, endpoint: YotsubaEndpoint)
                        -> String {
        format!(
                r#"
      select jsonb_agg(c) from (
          SELECT coalesce {2} as c from
              (select jsonb_array_elements({3}) as prev from "{0}".metadata where board = $1)x
          full JOIN
              (select jsonb_array_elements({4}) as newv)z
          ON {5}
      )q
      left join
          (select no as nno from "{0}"."{1}" where resto=0 and (archived_on is not null or deleted_on is not null))w
      ON c = nno
      where nno is null;
      "#,
                schema,
                board,
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
        )
    }

    fn init_board(&self, board: YotsubaBoard, schema: &str) -> String {
        format!(
                r#"
        CREATE TABLE IF NOT EXISTS "{0}"."{1}" (
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
            PRIMARY KEY (no),
            CONSTRAINT "unique_no_{1}" UNIQUE (no));

        CREATE INDEX IF NOT EXISTS "idx_{1}_no_resto" on "{0}"."{1}"(no, resto);
        
        -- Needs to be superuser
        CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA "{0}";
        
        CREATE INDEX IF NOT EXISTS "trgm_idx_{1}_com" ON "{0}"."{1}" USING gin (com "{0}".gin_trgm_ops);
        -- SET enable_seqscan TO OFF;
        "#,
                schema, board
        )
    }

    fn init_type(&self, schema: &str) -> String {
        format!(
                r#"
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT typname FROM pg_type WHERE typname = 'schema_4chan') THEN
                CREATE TYPE "{}"."schema_4chan" AS (
                    no bigint,
                    sticky smallint,
                    closed smallint,
                    now text,
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
                    time bigint,
                    md5 text,
                    fsize bigint,
                    m_img smallint,
                    resto int,
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
                    since4pass smallint
                );
            END IF;
        END
        $$;
    "#,
                schema
        )
    }

    fn init_views(&self, schema: &str, board: YotsubaBoard) -> String {
        let safe_create_view = |n, stmt| {
            format!(
                    r#"
        DO $$
        BEGIN
        CREATE VIEW "{0}"."{1}{3}" AS
            {2}
        EXCEPTION
        WHEN SQLSTATE '42P07' THEN
            NULL;
        END;
        $$;
        "#,
                    schema, board, stmt, n
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
                (CASE WHEN NOT resto=0 THEN resto ELSE no END) AS thread_num,
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
                (CASE WHEN capcode IS NULL THEN 'N' ELSE capcode END) AS capcode,
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
                (CASE WHEN sticky IS NULL THEN false ELSE sticky END) AS sticky,
                (CASE WHEN closed IS NULL THEN false ELSE closed END) AS locked,
                (CASE WHEN id='Developer' THEN 'Dev' ELSE id END) AS poster_hash, --not the same AS media_hash
                country AS poster_country,
                country_name AS poster_country_name,
                (case when
                  jsonb_strip_nulls(jsonb_build_object('uniqueIps', unique_ips, 'since4pass', since4pass, 'trollCountry', (
                  case when 
                  country = ANY ('{{AC,AN,BL,CF,CM,CT,DM,EU,FC,GN,GY,JH,KN,MF,NB,NZ,PC,PR,RE,TM,TR,UN,WP}}'::text[])
                  then
                  country
                  else
                  null
                  end
                  ))) != '{{}}'::jsonb then 
                jsonb_strip_nulls(jsonb_build_object('uniqueIps', unique_ips, 'since4pass', since4pass, 'trollCountry', (
                  case when 
                  country = ANY ('{{AC,AN,BL,CF,CM,CT,DM,EU,FC,GN,GY,JH,KN,MF,NB,NZ,PC,PR,RE,TM,TR,UN,WP}}'::text[])
                  then
                  country
                  else
                  null
                  end
                  ))) 
                   end )::text as exif, -- JSON in text format of uniqueIps, since4pass, and trollCountry. Has some deprecated fields but still used by Asagi and FF.
                (CASE WHEN archived_on IS NULL THEN false ELSE true END) AS archived,
                archived_on
                FROM "{0}"."{1}"
                {2};
                "#,
                schema,
                board,
                if is_main { "" } else { "WHERE deleted_on is not null" }
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
            (SELECT COUNT(no) FROM "{0}"."{1}" re WHERE t.no = resto or t.no = no) as nreplies,
            (SELECT COUNT(md5) FROM "{0}"."{1}" re WHERE t.no = resto or t.no = no) as nimages,
            (CASE WHEN sticky IS NULL THEN false ELSE sticky END) AS sticky,
            (CASE WHEN closed IS NULL THEN false ELSE closed END) AS locked
        from "{0}"."{1}" t where resto=0;
        "#,
            schema, board
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
        FROM "{0}"."{1}") t GROUP BY t.n,t.tr;
        "#,
            schema, board
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
              FROM "{0}"."{1}" WHERE md5 IS NOT NULL GROUP BY md5, sha256, sha256t)x;
        "#,
            schema, board
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
        FROM (SELECT *, (FLOOR(time/86400)*86400)::bigint AS day FROM "{0}"."{1}")t GROUP BY t.day ORDER BY day;
        "#,
            schema, board
        )
        );

        format!(
                r#"
        {2}

        {3}
    
        {4}

        {5}

        {6}

        {7}

        CREATE INDEX IF NOT EXISTS "idx_{1}_time" on "{0}"."{1}"(((floor((("{1}"."time" / 86400))::double precision) * '86400'::double precision)::bigint));

        CREATE TABLE IF NOT EXISTS "{0}".index_counters (
                                  id character varying(50) NOT NULL,
                                  val integer NOT NULL,
                                  PRIMARY KEY (id));
        "#,
                schema,
                board,
                main_view(true),
                main_view(false),
                board_threads,
                board_users,
                board_images,
                board_daily
        )
    }

    fn update_thread(&self, schema: &str, board: YotsubaBoard) -> String {
        format!(
                r#"
      INSERT INTO "{0}"."{1}" (
          no,sticky,closed,name,sub,com,filedeleted,spoiler,
          custom_spoiler,filename,ext,w,h,tn_w,tn_h,tim,time,md5,
          fsize, m_img,resto,trip,id,capcode,country,country_name,bumplimit,
          archived_on,imagelimit,semantic_url,replies,images,unique_ips,tag,since4pass,last_modified)
          SELECT
              no,sticky::int::bool,closed::int::bool,name,sub,com,filedeleted::int::bool,spoiler::int::bool,
              custom_spoiler,filename,ext,w,h,tn_w,tn_h,tim,time, (CASE WHEN length(q.md5)>20 and q.md5 IS NOT NULL THEN decode(REPLACE (q.md5, E'\\', '')::text, 'base64'::text) ELSE null::bytea END) AS md5,
              fsize, m_img::int::bool, resto,trip,q.id,capcode,country,country_name,bumplimit::int::bool,
              archived_on,imagelimit::int::bool,semantic_url,replies,images,unique_ips,
              tag,since4pass, extract(epoch from now())::bigint as last_modified
          FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts') q
          WHERE q.no IS NOT NULL
      ON CONFLICT (no) 
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
              unique_ips = CASE WHEN excluded.unique_ips is not null THEN excluded.unique_ips ELSE "{0}"."{1}".unique_ips END,
              tag = excluded.tag,
              since4pass = excluded.since4pass,
              last_modified = extract(epoch from now())::bigint
          WHERE excluded.no IS NOT NULL AND EXISTS (
              SELECT 
                  "{0}"."{1}".no,
                  "{0}"."{1}".sticky,
                  "{0}"."{1}".closed,
                  "{0}"."{1}".name,
                  "{0}"."{1}".sub,
                  "{0}"."{1}".com,
                  "{0}"."{1}".filedeleted,
                  "{0}"."{1}".spoiler,
                  "{0}"."{1}".custom_spoiler,
                  "{0}"."{1}".filename,
                  "{0}"."{1}".ext,
                  "{0}"."{1}".w,
                  "{0}"."{1}".h,
                  "{0}"."{1}".tn_w,
                  "{0}"."{1}".tn_h,
                  "{0}"."{1}".tim,
                  "{0}"."{1}".time,
                  "{0}"."{1}".md5,
                  "{0}"."{1}".fsize,
                  "{0}"."{1}".m_img,
                  "{0}"."{1}".resto,
                  "{0}"."{1}".trip,
                  "{0}"."{1}".id,
                  "{0}"."{1}".capcode,
                  "{0}"."{1}".country,
                  "{0}"."{1}".country_name,
                  "{0}"."{1}".bumplimit,
                  "{0}"."{1}".archived_on,
                  "{0}"."{1}".imagelimit,
                  "{0}"."{1}".semantic_url,
                  "{0}"."{1}".replies,
                  "{0}"."{1}".images,
                  --"{0}"."{1}".unique_ips,
                  "{0}"."{1}".tag,
                  "{0}"."{1}".since4pass
                  WHERE "{0}"."{1}".no IS NOT NULL
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
              WHERE excluded.no IS NOT NULL AND excluded.no = "{0}"."{1}".no
  )"#,
                schema, board
        )
    }

    /*
    #[allow(dead_code)]
    fn create_asagi_tables(schema: &str, board: YotsubaBoard) -> String {
        format!(
            r#"
        CREATE TABLE IF NOT EXISTS "{0}"."{1}_threads" (
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

        CREATE INDEX IF NOT EXISTS "{1}_threads_time_op_index" on "{0}"."{1}_threads" (time_op);
        CREATE INDEX IF NOT EXISTS "{1}_threads_time_bump_index" on "{0}"."{1}_threads" (time_bump);
        CREATE INDEX IF NOT EXISTS "{1}_threads_time_ghost_bump_index" on "{0}"."{1}_threads" (time_ghost_bump);
        CREATE INDEX IF NOT EXISTS "{1}_threads_time_last_modified_index" on "{0}"."{1}_threads" (time_last_modified);
        CREATE INDEX IF NOT EXISTS "{1}_threads_sticky_index" on "{0}"."{1}_threads" (sticky);
        CREATE INDEX IF NOT EXISTS "{1}_threads_locked_index" on "{0}"."{1}_threads" (locked);

        CREATE TABLE IF NOT EXISTS "{0}"."{1}_users" (
          user_id SERIAL NOT NULL,
          name character varying(100) NOT NULL DEFAULT '',
          trip character varying(25) NOT NULL DEFAULT '',
          firstseen bigint NOT NULL,
          postcount bigint NOT NULL,

          PRIMARY KEY (user_id),
          UNIQUE (name, trip)
        );

        CREATE INDEX IF NOT EXISTS "{1}_users_firstseen_index" on "{0}"."{1}_users" (firstseen);
        CREATE INDEX IF NOT EXISTS "{1}_users_postcount_index "on "{0}"."{1}_users" (postcount);

        CREATE TABLE IF NOT EXISTS "{0}"."{1}_images" (
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

        CREATE INDEX IF NOT EXISTS "{1}_images_total_index" on "{0}"."{1}_images" (total);
        CREATE INDEX IF NOT EXISTS "{1}_images_banned_index" ON "{0}"."{1}_images" (banned);

        CREATE TABLE IF NOT EXISTS "{0}"."{1}_daily" (
          day bigint NOT NULL,
          posts integer NOT NULL,
          images integer NOT NULL,
          sage integer NOT NULL,
          anons integer NOT NULL,
          trips integer NOT NULL,
          names integer NOT NULL,

          PRIMARY KEY (day)
        );

        CREATE TABLE IF NOT EXISTS "{0}"."{1}_deleted" (
          LIKE "{0}"."{1}" INCLUDING ALL
        );
        "#,
            schema, board
        )
    }

    #[allow(dead_code)]
    fn create_asagi_triggers(schema: &str, board: YotsubaBoard, board_name_main: &str) -> String {
        format!(
            r#"
        CREATE OR REPLACE FUNCTION "{1}_update_thread"(n_row "{0}"."{2}") RETURNS void AS $$
        BEGIN
          UPDATE
            "{0}"."{1}_threads" AS op
          SET
            time_last = (
              COALESCE(GREATEST(
                op.time_op,
                (SELECT MAX(timestamp) FROM "{0}"."{1}" re WHERE
                  re.thread_num = $1.no AND re.subnum = 0)
              ), op.time_op)
            ),
            time_bump = (
              COALESCE(GREATEST(
                op.time_op,
                (SELECT MAX(timestamp) FROM "{0}"."{1}" re WHERE
                  re.thread_num = $1.no AND (re.email <> 'sage' OR re.email IS NULL)
                  AND re.subnum = 0)
              ), op.time_op)
            ),
            time_ghost = (
              SELECT MAX(timestamp) FROM "{0}"."{1}" re WHERE
                re.thread_num = $1.no AND re.subnum <> 0
            ),
            time_ghost_bump = (
              SELECT MAX(timestamp) FROM "{0}"."{1}" re WHERE
                re.thread_num = $1.no AND re.subnum <> 0 AND (re.email <> 'sage' OR
                  re.email IS NULL)
            ),
            time_last_modified = (
              COALESCE(GREATEST(
                op.time_op,
                (SELECT GREATEST(MAX(timestamp), MAX(timestamp_expired)) FROM "{0}"."{1}" re WHERE
                  re.thread_num = $1.no)
              ), op.time_op)
            ),
            nreplies = (
              SELECT COUNT(*) FROM "{0}"."{1}" re WHERE
                (re.thread_num = $1.no or re.thread_num = $1.resto)
            ),
            nimages = (
              SELECT COUNT(media_hash) FROM "{0}"."{1}" re WHERE
                re.thread_num = $1.no
            )
            WHERE op.thread_num = $1.no;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_create_thread"(n_row "{0}"."{2}") RETURNS void AS $$
        BEGIN
          IF not n_row.resto = 0 THEN RETURN; END IF;
          INSERT INTO "{0}"."{1}_threads" SELECT $1.no, $1.time,$1.time,
              $1.time, NULL, NULL, $1.time, 0, 0, false, false WHERE NOT EXISTS (SELECT 1 FROM "{0}"."{1}_threads" WHERE thread_num=$1.no);
          RETURN;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_delete_thread"(n_parent integer) RETURNS void AS $$
        BEGIN
          DELETE FROM "{0}"."{1}_threads" WHERE thread_num = n_parent;
          RETURN;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_insert_image"(n_row "{0}"."{2}") RETURNS integer AS $$
        DECLARE
            img_id INTEGER;
            n_row_preview_orig text;
            n_row_media_hash text;
            n_row_media_orig text;
        BEGIN
            n_row_preview_orig := (CASE WHEN n_row.tim is not null THEN (n_row.tim::text || 's.jpg') ELSE null END);
            n_row_media_hash := encode(n_row.md5, 'base64');
            n_row_media_orig := (CASE WHEN n_row.tim is not null and n_row.ext is not null THEN (n_row.tim::text || n_row.ext) ELSE null END);
          INSERT INTO "{0}"."{1}_images"
            (media_hash, media, preview_op, preview_reply, total)
            SELECT n_row_media_hash, n_row_media_orig, NULL, NULL, 0
            WHERE NOT EXISTS (SELECT 1 FROM "{0}"."{1}_images" WHERE media_hash = n_row_media_hash);

          IF n_row.resto = 0 THEN
            UPDATE "{0}"."{1}_images" SET total = (total + 1), preview_op = COALESCE(preview_op, n_row_preview_orig) WHERE media_hash = n_row_media_hash RETURNING media_id INTO img_id;
          ELSE
            UPDATE "{0}"."{1}_images" SET total = (total + 1), preview_reply = COALESCE(preview_reply, n_row_preview_orig) WHERE media_hash = n_row_media_hash RETURNING media_id INTO img_id;
          END IF;
          RETURN img_id;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_delete_image"(n_media_id integer) RETURNS void AS $$
        BEGIN
          UPDATE "{0}"."{1}_images" SET total = (total - 1) WHERE id = n_media_id;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_insert_post"(n_row "{0}"."{2}") RETURNS void AS $$
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

          INSERT INTO "{0}"."{1}_daily"
            SELECT d_day, 0, 0, 0, 0, 0, 0
            WHERE NOT EXISTS (SELECT 1 FROM "{0}"."{1}_daily" WHERE day = d_day);

          UPDATE "{0}"."{1}_daily" SET posts=posts+1, images=images+d_image,
            sage=sage+d_sage, anons=anons+d_anon, trips=trips+d_trip,
            names=names+d_name WHERE day = d_day;

          IF (SELECT trip FROM "{0}"."{1}_users" WHERE trip = $1.trip) IS NOT NULL THEN
            UPDATE "{0}"."{1}_users" SET postcount=postcount+1,
              firstseen = LEAST($1.time, firstseen),
              name = COALESCE($1.name, '')
              WHERE trip = $1.trip;
          ELSE
            INSERT INTO "{0}"."{1}_users" (name, trip, firstseen, postcount)
              SELECT COALESCE($1.name,''), COALESCE($1.trip,''), $1.time, 0
              WHERE NOT EXISTS (SELECT 1 FROM "{0}"."{1}_users" WHERE name = COALESCE($1.name,'') AND trip = COALESCE($1.trip,''));

            UPDATE "{0}"."{1}_users" SET postcount=postcount+1,
              firstseen = LEAST($1.time, firstseen)
              WHERE name = COALESCE($1.name,'') AND trip = COALESCE($1.trip,'');
          END IF;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_delete_post"(n_row "{0}"."{2}") RETURNS void AS $$
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

          UPDATE "{0}"."{1}_daily" SET posts=posts-1, images=images-d_image,
            sage=sage-d_sage, anons=anons-d_anon, trips=trips-d_trip,
            names=names-d_name WHERE day = d_day;

          IF (SELECT trip FROM "{0}"."{1}_users" WHERE trip = $1.trip) IS NOT NULL THEN
            UPDATE "{0}"."{1}_users" SET postcount=postcount-1,
              firstseen = LEAST($1.time, firstseen)
              WHERE trip = $1.trip;
          ELSE
            UPDATE "{0}"."{1}_users" SET postcount=postcount-1,
              firstseen = LEAST($1.time, firstseen)
              WHERE (name = $1.name OR $1.name IS NULL) AND (trip = $1.trip OR $1.trip IS NULL);
          END IF;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_before_insert"() RETURNS trigger AS $$
        BEGIN
          IF NEW.md5 IS NOT NULL THEN
            --SELECT "{0}"."{1}_insert_image"(NEW) INTO NEW.no;
            --INTO asagi.media_id;
            PERFORM "{1}_insert_image"(NEW);
          END IF;
          RETURN NEW;
        END
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_after_insert"() RETURNS trigger AS $$
        BEGIN
          IF NEW.md5 IS NOT NULL THEN
            --SELECT "{0}"."{1}_insert_image"(NEW) INTO NEW.no;
            --INTO asagi.media_id;
            PERFORM "{1}_insert_image"(NEW);
          END IF;
          IF NEW.resto = 0 THEN
            PERFORM "{1}_create_thread"(NEW);
          END IF;
          PERFORM "{1}_update_thread"(NEW);
          PERFORM "{1}_insert_post"(NEW);
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_after_update"() RETURNS trigger AS $$
        BEGIN
          PERFORM "{1}_update_thread"(NEW);
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION "{1}_after_del"() RETURNS trigger AS $$
        BEGIN
          PERFORM "{1}_update_thread"(OLD);
          IF OLD.resto = 0 THEN
            PERFORM "{1}_delete_thread"(OLD.no);
          END IF;
          PERFORM "{1}_delete_post"(OLD);
          IF OLD.md5 IS NOT NULL THEN
            PERFORM "{1}_delete_image"(OLD.no);
          END IF;
          RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS "{1}_after_delete" ON "{0}"."{2}";
        CREATE TRIGGER "{1}_after_delete" after DELETE ON "{0}"."{2}"
          FOR EACH ROW EXECUTE PROCEDURE "{1}_after_del"();

        --DROP TRIGGER IF EXISTS "{1}_before_insert" ON "{0}"."{2}";
        --CREATE TRIGGER "{1}_before_insert" before INSERT ON "{0}"."{2}"
          --FOR EACH ROW EXECUTE PROCEDURE "{1}_before_insert"();

        DROP TRIGGER IF EXISTS "{1}_after_insert" ON "{0}"."{2}";
        CREATE TRIGGER "{1}_after_insert" after INSERT ON "{0}"."{2}"
          FOR EACH ROW EXECUTE PROCEDURE "{1}_after_insert"();

        DROP TRIGGER IF EXISTS "{1}_after_update" ON "{0}"."{2}";
        CREATE TRIGGER "{1}_after_update" after UPDATE ON "{0}"."{2}"
          FOR EACH ROW EXECUTE PROCEDURE "{1}_after_update"();
        "#,
            schema, board, board_name_main
        )
    }*/
}
