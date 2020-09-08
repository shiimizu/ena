CREATE SCHEMA IF NOT EXISTS %%SCHEMA%%;
CREATE SCHEMA IF NOT EXISTS extensions;
SET search_path = "%%SCHEMA%%", "$user", public, extensions;
ALTER DATABASE %%DB_NAME%% SET search_path = "%%SCHEMA%%", "$user", public, extensions;
ALTER DATABASE %%DB_NAME%% SET timezone TO 'UTC';
-- ALTER DATABASE %%DB_NAME%% SET timezone TO 'America/New_York';

-- make sure everybody can use everything in the extensions schema
grant usage on schema extensions to public;
grant execute on all functions in schema extensions to public;

-- include future extensions
alter default privileges in schema extensions
grant execute on functions to public;

alter default privileges in schema extensions
grant usage on types to public;

-- Unique hashes of all media
CREATE TABLE IF NOT EXISTS "media" (
    "w"         INT4,
    "h"         INT4,
    "tn_w"      INT2,
    "tn_h"      INT2,
    "fsize"     INT4,
    "banned"    BOOL,
    "ext"       TEXT,
    "fid"       TEXT,
    "md5"       BYTEA,
    "sha256"    BYTEA UNIQUE,
    "content"   BYTEA,
    "extra"     JSONB
);

COMMENT ON COLUMN media.w       is 'Image width dimension';
COMMENT ON COLUMN media.h       is 'Image height dimension';
COMMENT ON COLUMN media.tn_w    is 'Thumbnail image width dimension';
COMMENT ON COLUMN media.tn_h    is 'Thumbnail image height dimension';
COMMENT ON COLUMN media.fsize   is 'Size of uploaded file in bytes';
COMMENT ON COLUMN media.banned  is 'If the file is banned';
COMMENT ON COLUMN media.ext     is 'Filetype';
COMMENT ON COLUMN media.md5     is 'MD5 hash of file';
COMMENT ON COLUMN media.sha256  is 'SHA256 hash of file';
COMMENT ON COLUMN media.content is 'Optionally stored file';
COMMENT ON COLUMN media.extra   is 'Storage for any other columns';

-- The media entry that a post has
CREATE TABLE IF NOT EXISTS "posts_media" (
    "id"            BIGSERIAL,
    "post_num"      INT8,
    "thread_num"    INT8,
    "created_on"    TIMESTAMPTZ,
    "site_deleted"  BOOL,
    "spoiler"       BOOL,
    "board"         TEXT,
    "filename"      TEXT,
    "md5"           BYTEA,
    "sha256"        BYTEA,
    "sha256t_op"    BYTEA,
    "sha256t_reply" BYTEA,
    "content_thumb" BYTEA,
    "fid"           TEXT,
    "extra"         JSONB,
    PRIMARY KEY(board, post_num, sha256, sha256t_op, sha256t_reply),
    UNIQUE (board, post_num)
);

COMMENT ON COLUMN posts_media.id                is 'Monotonically increasing id';
COMMENT ON COLUMN posts_media.post_num          is 'The numeric post ID';
COMMENT ON COLUMN posts.media.thread_num        is 'The numeric thread ID if reply or NULL if OP';
COMMENT ON COLUMN posts_media.created_on        is 'UTC timestamp + microtime that an image was uploaded';
COMMENT ON COLUMN posts_media.site_deleted      is 'If the file was deleted at that website';
COMMENT ON COLUMN posts_media.spoiler           is 'If the media was marked as spoiler';
COMMENT ON COLUMN posts_media.board             is 'Board name';
COMMENT ON COLUMN posts_media.filename          is 'Filename as it appeared on the poster''s device';
COMMENT ON COLUMN posts_media.md5               is 'MD5 hash of file';
COMMENT ON COLUMN posts_media.sha256            is 'SHA256 hash of file';
COMMENT ON COLUMN posts_media.sha256t_op        is 'SHA256 hash of thumbnail when the post is OP';
COMMENT ON COLUMN posts_media.sha256t_reply     is 'SHA256 hash of thumbnail when the post is a reply';
COMMENT ON COLUMN posts_media.content_thumb     is 'Optionally stored thumbnail';
COMMENT ON COLUMN posts_media.fid               is 'SeaweedFS fid for thumbnails';
COMMENT ON COLUMN posts_media.extra             is 'Storage for any other columns';

-- List of all boards
CREATE TABLE IF NOT EXISTS "boards" (
    "last_modified_threads"     INT8,
    "last_modified_archive"     INT8,
    "id"                        SERIAL  NOT NULL UNIQUE PRIMARY KEY,
    "name"                      TEXT    NOT NULL UNIQUE,
    "title"                     TEXT,
    "threads"                   JSONB,
    "archive"                   JSONB
);

-- List of posts
CREATE TABLE IF NOT EXISTS "posts" (
    "num"            INT8        NOT NULL,
    -- "tim"               INT8,
    "thread_num"     INT8        ,
    "created_on"    TIMESTAMPTZ NOT NULL,
    "updated_on"    TIMESTAMPTZ,
    "archived_on"   TIMESTAMPTZ,
    "deleted_on"    TIMESTAMPTZ,
    -- "fsize"             INT4,
    -- "w"                 INT4,
    -- "h"                 INT4,
    "replies"       INT4,
    "images"        INT4,
    "unique_ips"    INT4,
    -- "tn_w"              INT2,
    -- "tn_h"              INT2,
    -- "custom_spoiler"    INT2,
    -- "since4pass"        INT2,
    "sticky"        BOOL,
    "closed"        BOOL,
    -- "filedeleted"       BOOL,
    -- "spoiler"           BOOL,
    -- "m_img"             BOOL,
    "bumplimit"     BOOL,
    "imagelimit"    BOOL,
    "md5"           BYTEA,
    "sha256"        BYTEA REFERENCES "media"("sha256"),
    "board"         TEXT NOT NULL REFERENCES "boards"("name") ON UPDATE CASCADE ON DELETE CASCADE,
    "name"          TEXT,
    "subject"       TEXT,
    "html"          TEXT,
    -- "filename"          TEXT,
    -- "ext"               TEXT,
    "tripcode"      TEXT,
    "ip_hash"       TEXT,
    -- "capcode"           TEXT,
    "country"       TEXT,
    -- "troll_country"     TEXT,
    -- "country_name"      TEXT,
    -- "semantic_url"      TEXT,
    -- "tag"               TEXT,
    "ip"            INET,
    "extra"         JSONB,
    PRIMARY KEY("board", "num", "created_on")
);

COMMENT ON COLUMN posts.num         is 'The numeric post ID';
COMMENT ON COLUMN posts.thread_num  is 'The numeric thread ID if reply or NULL if OP';
COMMENT ON COLUMN posts.created_on  is 'UTC timestamp the post was created';
COMMENT ON COLUMN posts.updated_on  is 'UTC timestamp from ''Last-Modified'' header in server response for OP, or when a post is modified/deleted/updated for replies';
COMMENT ON COLUMN posts.archived_on is 'UTC timestamp the post was archived';
COMMENT ON COLUMN posts.deleted_on  is 'UTC timestamp of system time of when the post was deleted, not serverside. Therefore it should not be relied upon entirely.';
COMMENT ON COLUMN posts.replies     is 'Total number of replies to a thread';
COMMENT ON COLUMN posts.images      is 'Total number of image replies to a thread';
COMMENT ON COLUMN posts.unique_ips  is 'Number of unique posters in a thread';
COMMENT ON COLUMN posts.sticky      is 'If the thread is being pinned to the top of the page';
COMMENT ON COLUMN posts.closed      is 'If the thread is closed to replies';
COMMENT ON COLUMN posts.bumplimit   is 'If a thread has reached bumplimit, it will no longer bump';
COMMENT ON COLUMN posts.imagelimit  is 'If an image has reached image limit, no more image replies can be made';
COMMENT ON COLUMN posts.md5         is 'MD5 hash of file in binary';
COMMENT ON COLUMN posts.board       is 'Board';
COMMENT ON COLUMN posts.name        is 'Name user posted with';
COMMENT ON COLUMN posts.subject     is 'OP Subject text';
COMMENT ON COLUMN posts.html        is 'Comment (HTML escaped)';
COMMENT ON COLUMN posts.tripcode    is 'The user''s tripcode, in format: !tripcode or !!securetripcode';
COMMENT ON COLUMN posts.ip_hash     is 'The poster''s ID';
COMMENT ON COLUMN posts.country     is 'Poster''s ISO 3166-1 alpha-2 country code';
COMMENT ON COLUMN posts.ip          is 'Gotta have a way to ban spammers ya''know';
COMMENT ON COLUMN posts.extra       is 'Storage for any other columns';


CREATE          INDEX IF NOT EXISTS ix_no                         ON "posts" USING brin ("no") WITH (pages_per_range = 32, autosummarize = on);
CREATE          INDEX IF NOT EXISTS ix_board                      ON "posts" USING brin ("board") WITH (pages_per_range = 32, autosummarize = on);
CREATE          INDEX IF NOT EXISTS ix_time                       ON "posts" ("time") ;
CREATE          INDEX IF NOT EXISTS ix_md5                        ON "posts" USING brin ("md5") WITH (pages_per_range = 32, autosummarize = on) WHERE "md5" is not null;
CREATE INDEX IF NOT EXISTS ix_no_board   ON "posts" USING brin ("no", "board" ) WITH (pages_per_range = 32, autosummarize = on);
CREATE INDEX IF NOT EXISTS ix_no_resto_board   ON "posts" USING brin ("no", "resto", "board" ) WITH (pages_per_range = 32, autosummarize = on);


-- Optional TimescaleDB. It is recommended to partition on a huge table such as posts.
-- CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA extensions CASCADE;
-- SELECT create_hypertable('posts', 'created_on', if_not_exists => true, chunk_time_interval => INTERVAL '2 weeks');

-- Create the 4chan schema as a type
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

-- Upsert a thread function
CREATE OR REPLACE FUNCTION thread_upsert(_board INT, json_posts JSONB)  
RETURNS setof posts AS $$  
declare    
BEGIN
RETURN query
    -- Manually select which columns to insert to
    -- Not including `ip` in this UPSERT since that's used by imageboard frontends
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
                unique_ips      = GREATEST(excluded.unique_ips, posts.unique_ips),
                tag             = excluded.tag,
                since4pass      = excluded.since4pass,
                extra           = posts.extra || excluded.extra
                -- Only update if the following columns are different.
                WHERE EXISTS (
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
                    -- posts.last_modified,
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
                    excluded.filedeleted,
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
                    -- excluded.last_modified,
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
                    GREATEST(excluded.unique_ips, posts.unique_ips) as unique_ips,
                    excluded.tag,
                    excluded.since4pass,
                    excluded.extra
                WHERE excluded.no = posts.no AND excluded.board = posts.board)
                RETURNING *;
END;  
$$ LANGUAGE plpgsql;


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
