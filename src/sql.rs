
pub fn init_schema(schema: &str) -> String {
    format!(r#"CREATE SCHEMA IF NOT EXISTS "{}";"#, schema)
}

pub fn init_metadata(schema: &str) -> String {
    format!(r#"
        CREATE TABLE IF NOT EXISTS "{0}".metadata (
                board text NOT NULL,
                threads jsonb,
                archive jsonb,
                PRIMARY KEY (board),
                CONSTRAINT board_unique UNIQUE (board));

        CREATE INDEX IF NOT EXISTS metadata_board_idx on "{0}".metadata(board);
        "#, schema)
}

pub fn init_board(schema: &str, board: &str) -> String {
    format!(r#"
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
        
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        
        CREATE INDEX IF NOT EXISTS "trgm_idx_{1}_com" ON "{0}"."{1}" USING gin (com gin_trgm_ops);
        -- SET enable_seqscan TO OFF;
        "#, schema, board)
}

pub fn init_type<'a>() -> &'a str {
    r#"
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'schema_4chan') THEN
                CREATE TYPE "schema_4chan" AS (
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
    "#
}

pub fn init_views(schema: &str, board: &str) -> String {
    let safe_create_view = |n, stmt| format!(r#"
        DO $$
        BEGIN
        CREATE VIEW "{0}"."{1}{3}" AS
            {2}
        EXCEPTION
        WHEN SQLSTATE '42P07' THEN
            NULL;
        END;
        $$;
        "#, schema, board, stmt, n);
    
    let main_view = |is_main| safe_create_view(
        if is_main { "_asagi" } else { "_deleted" },
        format!(r#"
            SELECT
                no AS doc_id,
                (CASE WHEN md5 IS NOT NULL THEN no ELSE NULL END) AS media_id,
                0::smallint AS poster_ip, --not used
                no AS num,
                subnum, --for ghost posts
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
                NULL AS email, --deprecated
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
                    , regexp_replace(r22, E'<a.*?>(.*?)</a>', '\1', 'g') r23 -- changed for postgres regex
                    , regexp_replace(r23, E'<span class=\"spoiler\"[^>]*>(.*?)</span>', '[spoiler]\1[/spoiler]', 'g') r24
                    , regexp_replace(r24, E'<span class=\"sjis\">(.*?)</span>', '[shiftjis]\1[/shiftjis]', 'g') r25 
                    , regexp_replace(r25, E'<s>', '[spoiler]', 'g') r26
                    , regexp_replace(r26, E'</s>', '[/spoiler]', 'g') r27
                    , regexp_replace(r27, E'<br\\s*/?>', E'\n', 'g') r28
                    , regexp_replace(r28, E'<wbr>', '', 'g') r29
                    ) AS comment,
                NULL AS delpass, --not used
                (CASE WHEN sticky IS NULL THEN false ELSE sticky END) AS sticky,
                (CASE WHEN closed IS NULL THEN false ELSE closed END) AS locked,
                (CASE WHEN id='Developer' THEN 'Dev' ELSE id END) AS poster_hash, --not the same AS media_hash
                country AS poster_country,
                country_name AS poster_country_name,
                NULL AS exif, --TODO not used
                (CASE WHEN archived_on IS NULL THEN false ELSE true END) AS archived,
                archived_on
                FROM "{0}"."{1}"
                {2};
        "#, schema, board, if is_main { "" } else { "WHERE deleted_on is not null" }));
    
    let board_threads = safe_create_view("_threads", format!(r#"
        SELECT
            no as thread_num,
            time as time_op,
            time as time_last,
            time as time_bump,
            null as time_ghost,
            null as time_ghost_bump,
            time as time_last_modified,
            replies as nreplies,
            1 as nimages,
            (CASE WHEN sticky IS NULL THEN false ELSE sticky END) AS sticky,
            (CASE WHEN closed IS NULL THEN false ELSE closed END) AS locked
        from "{0}"."{1}" where resto=0;
        "#, schema, board));

    let board_users = safe_create_view("_users", format!(r#"
        SELECT
            ROW_NUMBER() OVER (ORDER by min(t.time)) AS id,
            t.n AS name, t.tr AS trip, min(t.time) AS firstseen, count(*) AS postcount
            FROM (SELECT *, COALESCE(name,'') AS n, COALESCE(trip,'') AS tr
        FROM "{0}"."{1}") t GROUP BY t.n,t.tr;
        "#, schema, board));

    let board_images = safe_create_view("_images", format!(r#"
        select ROW_NUMBER() OVER(ORDER by x.media) AS media_id, * from (
            select
                encode(md5, 'base64') as media_hash,
                max(tim)::text || max(ext) as media,
                (case when max(resto) = 0 then max(tim)::text || 's.jpg' else null END)  as preview_op ,
                (case when max(resto) != 0 then max(tim)::text || 's.jpg' else null END)  as preview_reply ,
                count(md5)::int as total,
                0::smallint as banned
            from "{0}"."{1}" where md5 is not null group by md5)x;
        "#, schema, board));

    let board_daily = safe_create_view("_daily", format!(r#"
        SELECT
            MIN(t.no) AS firstpost,
            t.day AS day,
            COUNT(*) AS posts, COUNT(md5) AS images, COUNT(CASE WHEN name ~* '.*sage.*' THEN name ELSE NULL END) AS sage,
            COUNT(CASE WHEN name = 'Anonymous' AND trip IS NULL THEN name ELSE NULL END) AS anons, COUNT(trip) AS trips,
            COUNT(CASE WHEN COALESCE(name <> 'Anonymous' AND trip IS NULL, TRUE) THEN name ELSE NULL END) AS names
        FROM (SELECT *, (FLOOR(time/86400)*86400)::bigint AS day FROM "{0}"."{1}")t GROUP BY t.day ORDER BY day;
        "#, schema, board));
    
    format!(r#"
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
        "#, schema, board, main_view(true), main_view(false), board_threads, board_users, board_images, board_daily)
}

#[allow(dead_code)]
pub fn create_asagi_tables(schema: &str, board: &str) -> String {
    format!(r#"
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
        "#, schema, board)
}

#[allow(dead_code)]
pub fn create_asagi_triggers(schema: &str, board: &str, board_name_main: &str) -> String  {
    format!(r#"
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
        "#, schema, board, board_name_main)
}

pub fn upsert_deleted(schema: &str, board: &str, no: u32) -> String {
    // This will find an already existing post due to the WHERE clause, meaning it's ok to only select no
    format!(r#"
        INSERT INTO "{0}"."{1}" (no, time, resto)
            SELECT no, time, resto FROM "{0}"."{1}" WHERE no = {2}
            --SELECT * FROM "{0}"."{1}" WHERE no = {2}
        ON CONFLICT (no)
        DO
            UPDATE
            SET last_modified = extract(epoch from now())::bigint,
                deleted_on = extract(epoch from now())::bigint;
        "#, schema, board, no)
}

pub fn upsert_deleteds(schema: &str, board: &str, thread: u32) -> String {
    // This will find an already existing post due to the WHERE clause, meaning it's ok to only select no
    format!(r#"
        INSERT INTO "{0}"."{1}" (no, time, resto)
            SELECT x.* FROM
                (SELECT no, time, resto FROM "{0}"."{1}" where no={2} or resto={2} order by no) x
                --(SELECT * FROM "{0}"."{1}" where no={2} or resto={2} order by no) x
            FULL JOIN
                (SELECT no, time, resto FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts')) z
                --(SELECT * FROM jsonb_populate_recordset(null::"schema_4chan", $1::jsonb->'posts')) z
            ON x.no = z.no
            WHERE z.no is null
        ON CONFLICT (no) 
        DO
            UPDATE
            SET last_modified = extract(epoch from now())::bigint,
                deleted_on = extract(epoch from now())::bigint;
        "#, schema, board, thread)
}

pub fn upsert_hash(schema: &str, board: &str, no: u64, hash_type: &str) -> String {
    // This will find an already existing post due to the WHERE clause, meaning it's ok to only select no
    format!(r#"
        INSERT INTO "{0}"."{1}" (no, time, resto)
            SELECT no, time, resto FROM "{0}"."{1}" WHERE no = {2}
            --SELECT * FROM "{0}"."{1}" WHERE no = {2}
        ON CONFLICT (no) DO UPDATE
            SET last_modified = extract(epoch from now())::bigint,
                "{3}" = $1;
        "#, schema, board, no, hash_type)
}

pub fn upsert_thread(schema: &str, board: &str) -> String {
    format!(r#"
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
    )"#, schema, board)
}

pub fn upsert_metadata(schema: &str, column: &str) -> String {
    format!(r#"
        INSERT INTO "{0}".metadata(board, {1})
            VALUES ($1, $2::jsonb)
            ON CONFLICT (board)
            DO UPDATE
                SET {1} = $2::jsonb;
        "#, schema, column)
}

pub fn media_posts(schema: &str, board: &str, thread: u32) -> String {
    format!(r#"
        SELECT * FROM "{0}"."{1}"
        WHERE (no={2} OR resto={2}) AND (md5 is not null) AND (sha256 IS NULL OR sha256t IS NULL)
        ORDER BY no;
        "#, schema, board, thread)
}

pub fn deleted_and_modified_threads(schema: &str, is_threads: bool) -> String {
    format!(r#"
        SELECT {1} from
        (select jsonb_array_elements({2}) as prev from "{0}".metadata where board = $1)x
        full JOIN
        (select jsonb_array_elements({3}) as newv)z
        ON {4}
        where newv is null or {5};
        "#, schema,
            if is_threads { r#"jsonb_agg(COALESCE(newv->'no',prev->'no'))"# } else { "coalesce(newv,prev)" },
            if is_threads { r#"jsonb_array_elements(threads)->'threads'"# } else { "archive" },
            if is_threads { r#"jsonb_array_elements($2::jsonb)->'threads'"# } else { "$2::jsonb" },
            if is_threads { r#"prev->'no' = (newv -> 'no')"# } else { "prev = newv" },
            if is_threads { r#"not prev->'last_modified' <@ (newv -> 'last_modified')"# } else { "prev is null" }
        )
}

pub fn threads_list<'a>() -> &'a str {
    "SELECT jsonb_agg(newv->'no') from
                    (select jsonb_array_elements(jsonb_array_elements($1::jsonb)->'threads') as newv)z"
}

pub fn check_metadata_col(schema: &str, column: &str) -> String {
    format!(r#"select CASE WHEN {1} is not null THEN true ELSE false END from "{0}".metadata where board = $1"#,
                                    schema, column)
}

pub fn combined_threads(schema: &str, board: &str, is_threads: bool) -> String {
    format!(r#"
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
        "#, schema, board,
            if is_threads { r#"(prev->'no', newv->'no')::bigint"# } else { "(newv, prev)::bigint" },
            if is_threads { r#"jsonb_array_elements(threads)->'threads'"# } else { "archive" },
            if is_threads { r#"jsonb_array_elements($2::jsonb)->'threads'"# } else { "$2::jsonb" },
            if is_threads { r#"prev->'no' = (newv -> 'no')"# } else { "prev = newv" }
        )
}