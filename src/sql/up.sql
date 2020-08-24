    -- To load this file:
    -- PGPASSWORD env $ENV:PGPASSWORD='zxc'
    -- createdb -h localhost -p 5432 -U postgres ena4
    -- echo "SELECT 'CREATE DATABASE ena4' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ena4')\gexec" | psql -h localhost -p 5432 -U postgres
    -- echo "SELECT true WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'ena4')" | psql -h localhost -p 5432 -U postgres
    -- psql -h localhost -p 5432 -U postgres -d ena4 -f up.sql
    -- cat src/sql/up.sql | psql -h localhost -p 5432 -U postgres -d ena4
    
    -- SHOW search_path;
    CREATE SCHEMA IF NOT EXISTS %%SCHEMA%%;
    CREATE SCHEMA IF NOT EXISTS extensions;
    SET search_path = "%%SCHEMA%%", "$user", public, extensions;
    ALTER DATABASE %%DB_NAME%% SET search_path = "%%SCHEMA%%", "$user", public, extensions;
    ALTER DATABASE %%DB_NAME%% SET timezone TO 'America/New_York';

    -- make sure everybody can use everything in the extensions schema
    grant usage on schema extensions to public;
    grant execute on all functions in schema extensions to public;
    
    -- include future extensions
    alter default privileges in schema extensions
       grant execute on functions to public;
    
    alter default privileges in schema extensions
       grant usage on types to public;
    
    /*
    CREATE OR REPLACE FUNCTION unix_timestamp()  
    RETURNS INT8 AS $$  
    declare  
        result INT8;  
    BEGIN  
       SELECT extract(epoch from now())::INT8 into result;  
       RETURN result;  
    END;  
    $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
    */
    
    CREATE TABLE IF NOT EXISTS "media" (
        "banned"        BOOL,
        "id"            TEXT UNIQUE,
        "md5"           BYTEA UNIQUE,
        -- "md5t"          BYTEA UNIQUE GENERATED ALWAYS AS (decode(md5(content_thumb)::text, 'hex')) STORED,
        -- "sha256"        BYTEA UNIQUE GENERATED ALWAYS AS (sha256(content)) STORED,
        -- "sha256t"       BYTEA UNIQUE GENERATED ALWAYS AS (sha256(content_thumb)) STORED,
        "sha256"        BYTEA UNIQUE,
        -- Thumbnails aren't unique. 2 full medias can have the same thumbnail
        "sha256t"       BYTEA,
        "content"       BYTEA,
        "content_thumb" BYTEA
    );
    
    CREATE UNIQUE   INDEX IF NOT EXISTS unq_idx_media_md5          ON "media" ("md5");
    CREATE          INDEX IF NOT EXISTS idx_media_sha256t          ON "media" ("sha256t");
    
    COMMENT ON COLUMN media.id is 'SeaweedFS fid';
    
    CREATE TABLE IF NOT EXISTS "boards" (
        "last_modified_threads"     INT8,
        "last_modified_archive"     INT8,
        "id"                        SMALLSERIAL NOT NULL UNIQUE PRIMARY KEY,
        "board"                     TEXT        NOT NULL UNIQUE,
        "title"                     TEXT,
        "threads"                   JSONB,
        "archive"                   JSONB
    );
    
    CREATE TABLE IF NOT EXISTS "posts" (
        "no"                INT8 NOT NULL,
        "subnum"            INT8,
        "tim"               INT8,
        "resto"             INT8 NOT NULL,
        "time"              INT8 NOT NULL,
        "last_modified"     INT8,
        "archived_on"       INT8,
        "deleted_on"        INT8,
        "fsize"             INT4,
        "w"                 INT4,
        "h"                 INT4,
        "replies"           INT4,
        "images"            INT4,
        "unique_ips"        INT4,
        "board"             INT2 NOT NULL REFERENCES "boards" ("id"),
        "tn_w"              INT2,
        "tn_h"              INT2,
        "custom_spoiler"    INT2,
        "since4pass"        INT2,
        "sticky"            BOOL,
        "closed"            BOOL,
        "filedeleted"       BOOL,
        "spoiler"           BOOL,
        "m_img"             BOOL,
        "bumplimit"         BOOL,
        "imagelimit"        BOOL,
        "md5"               BYTEA REFERENCES "media" ("md5"),
        "name"              TEXT,
        "sub"               TEXT,
        "com"               TEXT,
        "filename"          TEXT,
        "ext"               TEXT,
        "trip"              TEXT,
        "id"                TEXT,
        "capcode"           TEXT,
        "country"           TEXT,
        "troll_country"     TEXT,
        "country_name"      TEXT,
        "semantic_url"      TEXT,
        "tag"               TEXT,
        "ip"                INET,
        "extra"             JSONB
    );
    
    COMMENT ON COLUMN posts.last_modified is 'Last modified time according to ''Last-Modified'' header in server response for OP, or modified/deleted/updated for posts';
    COMMENT ON COLUMN posts.deleted_on is 'Whenever a post is deleted. This is only accurate if you are actively archiving. Therefore it should not be relied upon entirely.';
    COMMENT ON COLUMN posts.ip is 'Gotta have a way to ban spammers ya''know';
    
    -- Optional TimescaleDB. It is recommended to partition on a huge table such as posts.
    CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA extensions CASCADE;
    SELECT create_hypertable('posts', 'time', if_not_exists => true, chunk_time_interval => INTERVAL '2 weeks');
    -- (the default value is 1 week)
    -- other functions:
    -- SELECT create_hypertable('posts', 'time', if_not_exists => true, chunk_time_interval => 1296000); -- 15 days
    -- SELECT set_chunk_time_interval('posts', chunk_time_interval => INTERVAL '1 week');
    
    
    CREATE UNIQUE   INDEX IF NOT EXISTS unq_idx_boards_board          ON "boards" ("board");
    CREATE UNIQUE   INDEX IF NOT EXISTS unq_idx_no_subnum_board_time  ON "posts" ("no", COALESCE("subnum", 0), board, "time");
    CREATE          INDEX IF NOT EXISTS ix_no                         ON "posts" USING brin ("no") WITH (pages_per_range = 32, autosummarize = on);
    CREATE          INDEX IF NOT EXISTS ix_board                      ON "posts" USING brin ("board") WITH (pages_per_range = 32, autosummarize = on);
    CREATE          INDEX IF NOT EXISTS ix_time                       ON "posts" ("time") ;
    CREATE          INDEX IF NOT EXISTS ix_md5                        ON "posts" USING brin ("md5") WITH (pages_per_range = 32, autosummarize = on) WHERE "md5" is not null;
    CREATE INDEX IF NOT EXISTS ix_no_board   ON "posts" USING brin ("no", "board" ) WITH (pages_per_range = 32, autosummarize = on);
    CREATE INDEX IF NOT EXISTS ix_no_resto_board   ON "posts" USING brin ("no", "resto", "board" ) WITH (pages_per_range = 32, autosummarize = on);
    
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT typname FROM pg_type WHERE typname = 'schema_4chan_with_extra') THEN
            CREATE TYPE "schema_4chan_with_extra" AS (
                "no"            INT8,
                sticky          INT2,
                closed          INT2,
                "now"           TEXT,
                "name"          TEXT,
                sub             TEXT,
                com             TEXT,
                filedeleted     INT2,
                spoiler         INT2,
                custom_spoiler  INT2,
                filename        TEXT,
                ext             TEXT,
                w               INT4,
                h               INT4,
                tn_w            INT2,
                tn_h            INT2,
                tim             INT8,
                "time"          INT8,
                "md5"           TEXT,
                fsize           INT4,
                m_img           INT2,
                resto           INT4,
                trip            TEXT,
                id              TEXT,
                capcode         TEXT,
                country         TEXT,
                troll_country   TEXT,
                country_name    TEXT,
                archived        INT2,
                bumplimit       INT2,
                archived_on     INT8,
                imagelimit      INT2,
                semantic_url    TEXT,
                replies         INT4,
                images          INT4,
                unique_ips      INT4,
                tag             TEXT,
                since4pass      INT2,
                extra           JSONB
            );
            END IF;
        END
        $$;
        
        CREATE OR REPLACE FUNCTION thread_upsert(_board INT, json_posts JSONB)  
        RETURNS setof posts AS $$  
        declare    
        BEGIN
        RETURN query
            -- My schema: Same as original schema but removed archived, added extra, last_modified, board + type changes + column tetris
            -- Not including `ip` in this UPSERT since that's for frontends with an imageboard
            -- Manually select which columns to insert to
            INSERT INTO posts (
            	board,
                no,
                sticky,
                closed,
                -- now,
                name,
                sub,
                com,
                filedeleted,
                spoiler,
                custom_spoiler,
                filename,
                ext,
                w,
                h,
                tn_w,
                tn_h,
                tim,
                time,
                md5,
                fsize,
                m_img,
                resto,
                trip,
                id,
                capcode,
                country,
                troll_country,
                country_name,
                bumplimit,
                archived_on,
                imagelimit,
                semantic_url,
                replies,
                images,
                unique_ips,
                tag,
                since4pass,
                extra)
                    -- Manually select columns for type conversions
                    SELECT
            			_board,
                        no,
                        sticky::int::bool,
                        closed::int::bool,
                        -- now,
                        name,
                        sub,
                        com,
                        filedeleted::int::bool,
                        spoiler::int::bool,
                        custom_spoiler,
                        filename,
                        ext,
                        w,
                        h,
                        tn_w,
                        tn_h,
                        tim,
                        time,
                        (CASE WHEN post.md5 IS NOT NULL AND length(post.md5) > 20 THEN decode(REPLACE (post.md5, E'\\', '')::text, 'base64'::text) ELSE NULL END) AS md5,
                        fsize,
                        m_img::int::bool,
                        resto,
                        trip,
                        post.id,
                        capcode,
                        country,
                        troll_country,
                        country_name,
                        bumplimit::int::bool,
                        archived_on,
                        imagelimit::int::bool,
                        semantic_url,
                        replies,
                        images,
                        -- Default unique_ips to 1 for OP if it's NULL
                        (CASE WHEN post.resto=0 AND post.unique_ips IS NULL THEN 1 ELSE post.unique_ips END) as unique_ips,
                        tag,
                        since4pass,
                        extra
                    FROM jsonb_populate_recordset(NULL::"schema_4chan_with_extra", json_posts->'posts') post
                    -- This where clause is for accomodating 4chan's side when a thread lingers (is still live) with no `no` or replies after deletion,
                    -- and also for tail json's OP
                    WHERE post.no IS NOT NULL AND post.resto IS NOT NULL AND post.time IS NOT NULL
                    ON CONFLICT (no, COALESCE(subnum, 0), board, "time")
                    DO UPDATE SET
                        -- Set previous to new values
                        no              = excluded.no,
                        sticky          = excluded.sticky,
                        closed          = excluded.closed,
                        -- now             = excluded.now,
                        name            = excluded.name,
                        sub             = excluded.sub,
                        com             = excluded.com,
                        filedeleted     = excluded.filedeleted,
                        -- Don't update media when filedeleted since media info will be wiped
                        -- spoiler         = excluded.spoiler,
                        -- custom_spoiler  = excluded.custom_spoiler,
                        -- filename        = excluded.filename,
                        -- ext             = excluded.ext,
                        -- w               = excluded.w,
                        -- h               = excluded.h,
                        -- tn_w            = excluded.tn_w,
                        -- tn_h            = excluded.tn_h,
                        -- tim             = excluded.tim,
                        time            = excluded.time,
                        last_modified   = extract(epoch from now())::INT8,
                        -- md5             = excluded.md5,
                        -- fsize           = excluded.fsize,
                        -- m_img           = excluded.m_img,
                        resto           = excluded.resto,
                        trip            = excluded.trip,
                        id              = excluded.id,
                        capcode         = excluded.capcode,
                        country         = excluded.country,
                        troll_country   = excluded.troll_country,
                        country_name    = excluded.country_name,
                        bumplimit       = excluded.bumplimit,
                        archived_on     = excluded.archived_on,
                        imagelimit      = excluded.imagelimit,
                        semantic_url    = excluded.semantic_url,
                        -- replies         = excluded.replies,
                        -- images          = excluded.images,
                        unique_ips      = COALESCE(posts.unique_ips, excluded.unique_ips),
                        tag             = excluded.tag,
                        since4pass      = excluded.since4pass,
                        extra           = posts.extra || excluded.extra
                        -- Only update if the following columns are different.
            			-- Also, either OP or reply post, OP will always have unique_ips, replies will not. A whole row will not update if one or the other is null..!
            			-- This won't happen since before inserting a new post, unique_ips is always default to 1 if null.
            			-- The reason we do all this for unique_ips is because it becomes null on archived and doing all this retains it after it's archived.
                        WHERE ((posts.unique_ips IS NULL AND excluded.unique_ips IS NULL) OR (posts.unique_ips IS NOT NULL AND excluded.unique_ips IS NOT NULL)) AND EXISTS (
                        SELECT
                            posts.no,
                            posts.sticky,
                            posts.closed,
                            -- posts.now,
                            posts.name,
                            posts.sub,
                            posts.com,
                            posts.filedeleted,
                            -- posts.spoiler,
                            -- posts.custom_spoiler,
                            -- posts.filename,
                            -- posts.ext,
                            -- posts.w,
                            -- posts.h,
                            -- posts.tn_w,
                            -- posts.tn_h,
                            -- posts.tim,
                            posts.time,
                            posts.last_modified,
                            -- posts.md5,
                            -- posts.fsize,
                            -- posts.m_img,
                            posts.resto,
                            posts.trip,
                            posts.id,
                            posts.capcode,
                            posts.country,
                            posts.troll_country,
                            posts.country_name,
                            posts.bumplimit,
                            posts.archived_on,
                            posts.imagelimit,
                            posts.semantic_url,
                            -- posts.replies,
                            -- posts.images,
            				posts.unique_ips,
                            posts.tag,
                            posts.since4pass,
                            posts.extra
                        -- The EXCEPT operator returns distinct rows from the first (left) query that are not in the output of the second (right) query.
                        -- (Use the PostgreSQL EXCEPT operator to get the rows from the first query that do not appear in the result set of the second query.)
                        -- Then the EXISTS operator (above) takes the columns that exists (the ones returned from EXCEPT), which means these are the ones that are different
                        EXCEPT
                        SELECT 
                            excluded.no,
                            excluded.sticky,
                            excluded.closed,
                            -- excluded.now,
                            excluded.name,
                            excluded.sub,
                            excluded.com,
                            -- excluded.filedeleted,
                            -- excluded.spoiler,
                            -- excluded.custom_spoiler,
                            -- excluded.filename,
                            -- excluded.ext,
                            -- excluded.w,
                            -- excluded.h,
                            -- excluded.tn_w,
                            -- excluded.tn_h,
                            -- excluded.tim,
                            excluded.time,
                            excluded.last_modified,
                            -- excluded.md5,
                            -- excluded.fsize,
                            -- excluded.m_img,
                            excluded.resto,
                            excluded.trip,
                            excluded.id,
                            excluded.capcode,
                            excluded.country,
                            excluded.troll_country,
                            excluded.country_name,
                            excluded.bumplimit,
                            excluded.archived_on,
                            excluded.imagelimit,
                            excluded.semantic_url,
                            -- excluded.replies,
                            -- excluded.images,
                            excluded.unique_ips,
                            excluded.tag,
                            excluded.since4pass,
                            excluded.extra
                        WHERE excluded.no = posts.no AND excluded.board = posts.board)
                        RETURNING *;
        END;  
        $$ LANGUAGE plpgsql;
    	-- Should not mark this a PARALLEL SAFE since this program could run multiple processess of itself, in which they're all mutating fields in parallel.
    	-- This is probably not the case due to MVCC but I'll play it safe since I'd rather not lose data or have them incorrect.
    
        CREATE OR REPLACE FUNCTION function_trigger_update_replies_images()
        RETURNS trigger AS $$
        BEGIN
            UPDATE
        	    posts
            SET
            	replies    = new_vals.replies,
            	images     = new_vals.images
            FROM
            	(
            	   SELECT COUNT(no) as "replies", COUNT(md5) as "images", MIN(board) as "board", MAX(resto) as "resto" FROM posts WHERE board=(SELECT MIN(board) FROM NEW) and resto=(SELECT MAX(resto) FROM NEW)
                ) AS new_vals
                
            -- Only update if changed.. The `WHERE EXISTS` doesn't seem to work
            -- WHERE EXISTS (
            --     SELECT posts.replies, posts.images
            --     EXCEPT
            --     SELECT new_vals.replies, new_vals.images
            -- );
            WHERE posts.board=new_vals.board AND posts.no = new_vals.resto AND (posts.replies != new_vals.replies OR posts.images != new_vals.images);
            RETURN NULL;
        END$$ LANGUAGE 'plpgsql';
        
        DROP TRIGGER IF EXISTS trigger_after_insert_update_replies_images ON "posts";
        CREATE TRIGGER trigger_after_insert_update_replies_images
        AFTER INSERT ON "posts"
        REFERENCING NEW TABLE AS NEW
        FOR EACH STATEMENT
        EXECUTE PROCEDURE function_trigger_update_replies_images();
        
        DROP TRIGGER IF EXISTS trigger_after_delete_update_replies_images ON "posts";
        CREATE TRIGGER trigger_after_delete_update_replies_images
        AFTER DELETE ON "posts"
        REFERENCING OLD TABLE AS NEW
        FOR EACH STATEMENT
        EXECUTE PROCEDURE function_trigger_update_replies_images();
        
        
        CREATE OR REPLACE FUNCTION function_trigger_insert_media_md5()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.md5 IS NOT NULL THEN
                INSERT INTO media("md5") VALUES(NEW.md5)
                ON CONFLICT ("md5") DO NOTHING;
            END IF;
            RETURN NEW;
        END$$ LANGUAGE 'plpgsql';
        
        DROP TRIGGER IF EXISTS trigger_media_before_insert ON "posts";
        CREATE TRIGGER trigger_media_before_insert
        BEFORE INSERT ON posts
        FOR EACH ROW
        EXECUTE PROCEDURE function_trigger_insert_media_md5();
