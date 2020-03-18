//! PostgreSQL implementation.

use crate::{
    archiver::YotsubaArchiver,
    enums::{YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier},
    sql::*
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::{collections::VecDeque, convert::TryFrom};
use tokio_postgres::{Row, Statement};

/// Unused for now. This is just for the Cargo doc.
pub mod core {
    //! Implementation of the default schema.  
    //!
    //! - [`Client`] The PostgreSQL implementation ([`tokio_postgres`]) to run the SQL queries
    //! - [`Schema`] The schema and list of SQL queries that `ena` uses
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

    // PostgreSQL version of the schema. This is also the default one used.  
    //
    // If another schema is thought of, feel free to use the [`Queries`] to implement it.
    
    // PostgreSQL version is using tokio_postgres
    // #[async_trait]
    
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
    async fn run_inner(&self) {
        self.run().await
    }
}

/// PostgreSQL version of the schema. This is also the default one used.  
///
/// If another schema is thought of, feel free to use the [`Queries`] to implement it.
impl Queries for tokio_postgres::Client {
    fn query_init_schema(&self, schema: &str) -> String {
        format!(
            r#"
        CREATE SCHEMA IF NOT EXISTS "{0}";
        SET search_path TO "{0}";
        "#,
            schema
        )
    }

    fn query_init_metadata(&self) -> String {
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

    fn query_delete(&self, board: YotsubaBoard) -> String {
        format!(
            r#"
      UPDATE "{board}"
      SET deleted_on    = extract(epoch from now())::bigint,
        last_modified = extract(epoch from now())::bigint
      WHERE
      no = $1 AND deleted_on is NULL;
      "#,
            board = board
        )
    }

    fn query_update_deleteds(&self, board: YotsubaBoard) -> String {
        // This will find an already existing post due to the WHERE clause, meaning it's
        // ok to only select no
        format!(
            r#"
        INSERT INTO "{board}" (no, time, resto)
        SELECT x.* FROM
          (SELECT no, time, resto FROM "{board}" where no=$2 or resto=$2 order by no) x
          --(SELECT * FROM "{board}" where no=$2 or resto=$2 order by no) x
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
        "{board}".deleted_on is NULL; "#,
            board = board
        )
    }

    fn query_update_hash(
        &self, board: YotsubaBoard, hash_type: YotsubaHash, thumb: YotsubaStatement
    ) -> String {
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

    fn query_update_metadata(&self, column: YotsubaEndpoint) -> String {
        format!(
            r#"
    INSERT INTO metadata(board, {endpoint})
      VALUES ($1, $2::jsonb)
      ON CONFLICT (board)
      DO UPDATE
        SET {endpoint} = $2::jsonb;
    "#,
            endpoint = column
        )
    }

    fn query_medias(&self, board: YotsubaBoard, media_mode: YotsubaStatement) -> String {
        format!(
            r#"
        SELECT * FROM "{0}"
        WHERE (md5 is not null) {1} AND filedeleted IS NULL AND (no=$1 or resto=$1)
        ORDER BY no desc;"#,
            board,
            match media_mode {
                YotsubaStatement::UpdateHashThumbs => "AND (sha256t IS NULL)",
                YotsubaStatement::UpdateHashMedia => "AND (sha256 IS NULL)",
                _ => "AND (sha256 IS NULL OR sha256t IS NULL)"
            }
        )
    }

    fn query_threads_modified(&self, endpoint: YotsubaEndpoint) -> String {
        format!(
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
        )
    }

    fn query_threads(&self) -> String {
        "SELECT jsonb_agg(newv->'no')
      FROM
      (SELECT jsonb_array_elements(jsonb_array_elements($1::jsonb)->'threads') as newv)z"
            .to_string()
    }

    fn query_metadata(&self, column: YotsubaEndpoint) -> String {
        format!(
            r#"select CASE WHEN {endpoint} is not null THEN true ELSE false END from metadata where board = $1"#,
            endpoint = column
        )
    }

    fn query_threads_combined(&self, board: YotsubaBoard, endpoint: YotsubaEndpoint) -> String {
        format!(
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

    fn query_init_board(&self, board: YotsubaBoard) -> String {
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
          PRIMARY KEY (no),
          CONSTRAINT "unique_no_{board}" UNIQUE (no));
        
        CREATE INDEX IF NOT EXISTS "idx_{board}_no_resto" on "{board}"(no, resto);
        
        -- Needs to be superuser
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        
        CREATE INDEX IF NOT EXISTS "trgm_idx_{board}_com" ON "{board}" USING gin (com gin_trgm_ops);
        -- SET enable_seqscan TO OFF;"#,
            board = board
        )
    }

    fn query_init_type(&self) -> String {
        r#"
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
            .into()
    }

    fn query_init_views(&self, board: YotsubaBoard) -> String {
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
      (CASE WHEN sticky IS NULL THEN false ELSE sticky END) AS sticky,
      (CASE WHEN closed IS NULL THEN false ELSE closed END) AS locked
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

    fn query_update_thread(&self, board: YotsubaBoard) -> String {
        format!(
            r#"
        INSERT INTO "{board}" (
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
            board = board
        )
    }
}

/// PostgreSQL version is using tokio_postgres
#[async_trait]
impl QueriesExecutor<Statement, Row> for tokio_postgres::Client {
    async fn init_type(&self) {
        self.execute(self.query_init_type().as_str(), &[])
            .await
            .expect("Err initializing 4chan schema as a type");
    }

    async fn init_schema(&self, schema: &str) {
        self.batch_execute(&self.query_init_schema(schema))
            .await
            .expect(&format!("Err creating schema: {}", schema));
    }

    async fn init_metadata(&self) {
        self.batch_execute(&self.query_init_metadata()).await.expect("Err creating metadata");
    }

    async fn init_board(&self, board: YotsubaBoard) {
        self.batch_execute(&self.query_init_board(board))
            .await
            .expect(&format!("Err creating board: {}", board));
    }

    async fn init_views(&self, board: YotsubaBoard) {
        self.batch_execute(&self.query_init_views(board)).await.expect("Err create views");
    }

    async fn update_metadata(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<u64>
    {
        log::debug!("update_metadata");
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateMetadata };
        let statement = statements.get(&id).unwrap();
        Ok(self
            .execute(statement, &[
                &board.to_string(),
                &serde_json::from_slice::<serde_json::Value>(item)?
            ])
            .await?)
    }

    async fn medias(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u32
    ) -> Result<Vec<Row>>
    {
        log::debug!("medias");
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Medias };
        let statement = statements.get(&id).unwrap();
        Ok(self.query(statement, &[&(no as i64)]).await?)
    }

    async fn update_hash(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    )
    {
        // info!("Creating Identifier");
        let id = YotsubaIdentifier { endpoint, board, statement: hash_type };
        let statement = statements.get(&id).unwrap();
        log::debug!("update_hash {:?} hash: {:?}", id, &hashsum);
        self.execute(statement, &[&(no as i64), &hashsum])
            .await
            .expect("Err executing sql: update_hash");
    }

    /// Mark a single post as deleted.
    async fn delete(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u32
    )
    {
        log::debug!("delete");
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Delete };
        let statement = statements.get(&id).unwrap();
        self.execute(statement, &[&i64::try_from(no).unwrap()])
            .await
            .expect("Err executing sql: delete");
    }

    /// Mark posts from a thread where it's deleted.
    async fn update_deleteds(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, thread: u32, item: &[u8]
    ) -> Result<u64>
    {
        log::debug!("update_deleteds");
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateDeleteds };
        let statement = statements.get(&id).unwrap();
        Ok(self
            .execute(statement, &[
                &serde_json::from_slice::<serde_json::Value>(item)?,
                &i64::try_from(thread).unwrap()
            ])
            .await?)
    }

    async fn update_thread(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<u64>
    {
        log::debug!("update_thread");
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateThread };
        let statement = statements.get(&id).unwrap();
        Ok(self.execute(statement, &[&serde_json::from_slice::<serde_json::Value>(item)?]).await?)
    }

    async fn metadata(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard
    ) -> bool
    {
        log::debug!("metadata");
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

    async fn threads(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        log::debug!("threads");
        let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Threads };
        let statement = statements.get(&id).unwrap();
        let i = serde_json::from_slice::<serde_json::Value>(item)?;
        Ok(self
            .query(statement, &[&i])
            .await
            .ok()
            .filter(|re| !re.is_empty())
            .map(|re| re[0].try_get(0).ok())
            .flatten()
            .map(|re| serde_json::from_value::<VecDeque<u32>>(re).ok())
            .flatten()
            .ok_or_else(|| anyhow!("Error in executing getting threads"))?)
    }

    /// This query is only run ONCE at every startup
    /// Running a JOIN to compare against the entire DB on every INSERT/UPDATE
    /// would not be that great. That is not done here.
    /// This gets all the threads from cache, compares it to the new json to get
    /// new + modified threads Then compares that result to the database
    /// where a thread is deleted or archived, and takes only the threads
    /// where's it's not deleted or archived
    async fn threads_combined(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, new_threads: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        log::debug!("threads_combined");
        let id =
            YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::ThreadsCombined };
        let statement = statements.get(&id).unwrap();
        let i = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        Ok(self
            .query(statement, &[&board.to_string(), &i])
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
    async fn threads_modified(
        &self, endpoint: YotsubaEndpoint, board: YotsubaBoard, new_threads: &[u8],
        statements: &StatementStore<Statement>
    ) -> Result<VecDeque<u32>>
    {
        log::debug!("threads_combined");
        let id =
            YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::ThreadsModified };
        let statement = statements.get(&id).unwrap();
        let i = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        Ok(self
            .query(statement, &[&board.to_string(), &i])
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
