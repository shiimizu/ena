// use fomat_macros::fomat;
// use futures::stream::StreamExt;
use super::{Query, QueryExecutor};
use crate::{
    config::{Board, Opt},
    ThreadType,
};
use async_rwlock::RwLock;
use async_trait::async_trait;
use color_eyre::eyre::{eyre, Result};
use futures::{future::Either, stream::Iter};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};
use strum::IntoEnumIterator;
use tokio_postgres::types::{FromSql, ToSql};

/// List of prepared statments to query
pub type StatementStore = HashMap<Query, tokio_postgres::Statement>;

/// List of computged prepared statments
static STATEMENTS: Lazy<RwLock<StatementStore>> = Lazy::new(|| RwLock::new(HashMap::new()));

#[async_trait]
impl QueryExecutor for tokio_postgres::Client {
    async fn boards_index_get_last_modified(&self) -> Option<String> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardsIndexGetLastModified).unwrap();
        self.query_one(statement, &[]).await.ok().map(|row| row.get("last_modified")).flatten()
    }

    async fn boards_index_upsert(&self, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardsIndexUpsert).unwrap();
        self.execute(statement, &[&json, &last_modified]).await.map_err(|e| eyre!(e))
    }

    async fn board_is_valid(&self, board: &str) -> bool {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardIsValid).unwrap();
        self.query_one(statement, &[&board]).await.unwrap().get(0)
    }

    async fn board_upsert(&self, board: &str) -> Result<u64> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardUpsert).unwrap();
        self.execute(statement, &[&board]).await.map_err(|e| eyre!(e))
    }

    async fn board_table_exists(&self, board: &str, opt: &Opt) -> Option<String> {
        unreachable!()
    }

    async fn board_get(&self, board: &str) -> Result<u16> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardGet).unwrap();
        let res = self.query_one(statement, &[&board]).await.map_err(|e| eyre!(e));
        res.map(|row| row.get("id")).map(|res: Option<i16>| res.map(|v| v as u16).unwrap())
    }

    async fn board_get_last_modified(&self, thread_type: ThreadType, board_id: u16) -> Option<String> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardGetLastModified).unwrap();
        self.query_one(statement, &[&thread_type.as_str(), &(board_id as i16)]).await.ok().map(|row| row.get("last_modified")).flatten()
    }

    async fn board_upsert_threads(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardUpsertThreads).unwrap();
        self.execute(statement, &[&(board_id as i16), &board, &json, &last_modified]).await.map_err(|e| eyre!(e))
    }

    async fn board_upsert_archive(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::BoardUpsertArchive).unwrap();
        self.execute(statement, &[&(board_id as i16), &board, &json, &last_modified]).await.map_err(|e| eyre!(e))
    }

    async fn thread_get(&self, board_info: &Board, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadGet).unwrap();
        let b = &(board_info.id as i16);
        let t = &(thread as i64);
        let params = vec![b as &(dyn ToSql + Sync), t as &(dyn ToSql + Sync)];
        let res = self.query_raw(statement, params.into_iter().map(|p| p as &dyn ToSql)).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn thread_get_media(&self, board_info: &Board, thread: u64, start: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadGetMedia).unwrap();
        let b = &(board_info.id as i16);
        let t = &(thread as i64);
        let s = &(start as i64);
        let params = vec![b as &(dyn ToSql + Sync), t as &(dyn ToSql + Sync), s as &(dyn ToSql + Sync)];
        let res = self.query_raw(statement, params.into_iter().map(|p| p as &dyn ToSql)).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn thread_get_last_modified(&self, board_id: u16, thread: u64) -> Option<String> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadGetLastModified).unwrap();
        self.query_one(statement, &[&(board_id as i16), &(thread as i64)]).await.ok().map(|row| row.get("last_modified")).flatten()
    }

    // FIXME: Follow this method's example to use PG functions
    // TODO return an Option|Result
    async fn thread_upsert(&self, board_info: &Board, thread_json: &serde_json::Value) -> u64 {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadUpsert).unwrap();
        let res = self.query_one(statement, &[&(board_info.id as i32), &thread_json]).await.map(|row| row.get::<usize, Option<i64>>(0));
        // .map(|c: Option<i64>| c.map(|count| count as u64));
        res.unwrap().unwrap() as u64
        // unreachable!()
    }

    async fn thread_update_last_modified(&self, last_modified: &str, board_id: u16, thread: u64) -> Result<u64> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadUpdateLastModified).unwrap();
        self.execute(statement, &[&last_modified, &(board_id as i16), &(thread as i64)]).await.map_err(|e| eyre!(e))
    }

    async fn thread_update_deleted(&self, board_id: u16, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadUpdateDeleted).unwrap();
        let b = &(board_id as i16);
        let t = &(thread as i64);
        let params = vec![b as &(dyn ToSql + Sync), t as &(dyn ToSql + Sync)];
        let res = self.query_raw(statement, params.into_iter().map(|p| p as &dyn ToSql)).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn thread_update_deleteds(&self, board_info: &Board, thread: u64, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadUpdateDeleteds).unwrap();
        let b = &(board_info.id as i16);
        let t = &(thread as i64);
        let params = vec![b as &(dyn ToSql + Sync), t as &(dyn ToSql + Sync), json as &(dyn ToSql + Sync)];
        let res = self.query_raw(statement, params.into_iter().map(|p| p as &dyn ToSql)).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn threads_get_combined(&self, thread_type: ThreadType, board_id: u16, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadsGetCombined).unwrap();
        let b = &(board_id as i16);
        let s = &thread_type.as_str();
        let params = vec![s as &(dyn ToSql + Sync), b as &(dyn ToSql + Sync), json as &(dyn ToSql + Sync)];
        let res = self.query_raw(statement, params.into_iter().map(|p| p as &dyn ToSql)).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn threads_get_modified(&self, board_id: u16, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::ThreadsGetModified).unwrap();
        // let params = vec![&(board_id as i16  as (dyn ToSql+Sync)), &(thread as i64 as (dyn ToSql+Sync))];
        let b = &(board_id as i16);
        let params = vec![b as &(dyn ToSql + Sync), json as &(dyn ToSql + Sync)];
        let res = self.query_raw(statement, params.into_iter().map(|p| p as &dyn ToSql)).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn post_get_single(&self, board_id: u16, thread: u64, no: u64) -> bool {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::PostGetSingle).unwrap();
        let res = self.query_opt(statement, &[&(board_id as i16), &(thread as i64), &(no as i64)]).await.map_err(|e| eyre!(e));
        res.map(|r| r.is_some()).unwrap()
    }

    async fn post_get_media(&self, board_info: &Board, md5: &str, hash_thumb: Option<&[u8]>) -> Either<Result<Option<tokio_postgres::Row>>, Option<mysql_async::Row>> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::PostGetMedia).unwrap();
        let res = self.query_opt(statement, &[&hash_thumb]).await.map_err(|e| eyre!(e));
        Either::Left(res)
    }

    async fn post_upsert_media(&self, md5: &[u8], hash_full: Option<&[u8]>, hash_thumb: Option<&[u8]>) -> Result<u64> {
        let store = STATEMENTS.read().await;
        let statement = (*store).get(&Query::PostUpsertMedia).unwrap();
        let res = self.execute(statement, &[&md5, &Some(hash_full), &Some(hash_thumb)]).await.map_err(|e| eyre!(e));
        res
    }

    async fn init_statements(&self, board_id: u16, board: &str) {
        let mut map = STATEMENTS.write().await;
        for query in super::Query::iter() {
            let statement = match query {
                Query::BoardsIndexGetLastModified => self.prepare(boards_index_get_last_modified()),
                Query::BoardsIndexUpsert => self.prepare(boards_index_upsert()),
                Query::BoardIsValid => self.prepare(board_is_valid()),
                Query::BoardUpsert => self.prepare(board_upsert()),
                Query::BoardTableExists => self.prepare("SELECT 1;"),
                Query::BoardGet => self.prepare(board_get()),
                Query::BoardGetLastModified => self.prepare(board_get_last_modified()),
                Query::BoardUpsertThreads => self.prepare(board_upsert_threads()),
                Query::BoardUpsertArchive => self.prepare(board_upsert_archive()),
                Query::ThreadGet => self.prepare(thread_get()),
                Query::ThreadGetMedia => self.prepare(thread_get_media()),
                Query::ThreadGetLastModified => self.prepare(thread_get_last_modified()),
                Query::ThreadUpsert => self.prepare(thread_upsert()),
                Query::ThreadUpdateLastModified => self.prepare(thread_update_last_modified()),
                Query::ThreadUpdateDeleted => self.prepare(thread_update_deleted()),
                Query::ThreadUpdateDeleteds => self.prepare(thread_update_deleteds()),
                Query::ThreadsGetCombined => self.prepare(threads_get_combined()),
                Query::ThreadsGetModified => self.prepare(threads_get_modified()),
                Query::PostGetSingle => self.prepare(post_get_single()),
                Query::PostGetMedia => self.prepare(post_get_media()),
                Query::PostUpsertMedia => self.prepare(post_upsert_media()),
            }
            .await
            .unwrap();
            map.insert(query, statement);
        }
        map.remove(&Query::BoardTableExists);
    }
}

pub fn boards_index_get_last_modified<'a>() -> &'a str {
    r#"
        SELECT to_char(to_timestamp("last_modified_threads") AT TIME ZONE 'UTC', 'Dy, DD Mon YYYY HH24:MI:SS GMT') AS "last_modified"
        FROM boards WHERE id=0;
    "#
}

pub fn boards_index_upsert<'a>() -> &'a str {
    r#"
        INSERT INTO boards(id, board, title, threads, last_modified_threads) VALUES(0, 'boards', 'boards.json', $1, extract(epoch from $2::TEXT::TIMESTAMP))
        ON CONFLICT (board)
        DO UPDATE SET
            id                      = excluded.id,
            board                   = excluded.board,
            title                   = excluded.title,
            threads                 = excluded.threads,
            last_modified_threads   = excluded.last_modified_threads;
    "#
}

pub fn board_is_valid<'a>() -> &'a str {
    r#"
    select jsonb_path_exists(threads, '$.boards[*].board ? (@ == $_board)', ('{"_board":"' || $1 || '"}')::TEXT::JSONB) from boards where id=0;
    "#
}

pub fn board_upsert<'a>() -> &'a str {
    r#"
        INSERT INTO boards(board) VALUES($1)
        ON CONFLICT (board) DO NOTHING;
    "#
}
pub fn board_get<'a>() -> &'a str {
    r#"SELECT * from boards where board = $1;"#
}

pub fn board_get_last_modified<'a>() -> &'a str {
    // The CASE is a workaround so we can make a Statement for tokio_postgres
    r#"
    SELECT to_char(to_timestamp((CASE WHEN $1='threads' THEN "last_modified_threads" ELSE "last_modified_archive" END)) AT TIME ZONE 'UTC', 'Dy, DD Mon YYYY HH24:MI:SS GMT') AS "last_modified" FROM boards WHERE id=$2;
    "#
}

pub fn board_upsert_threads<'a>() -> &'a str {
    r#"
    INSERT INTO boards(id, board, "threads", last_modified_threads) VALUES($1, $2, $3, extract(epoch from $4::TEXT::TIMESTAMP))
    ON CONFLICT (id)
    DO UPDATE SET
        id                          = excluded.id,
        board                       = excluded.board,
        threads                     = excluded.threads,
        last_modified_threads       = excluded.last_modified_threads;
    "#
}

pub fn board_upsert_archive<'a>() -> &'a str {
    r#"
    INSERT INTO boards(id, board, "archive", last_modified_archive) VALUES($1, $2, $3, extract(epoch from $4::TEXT::TIMESTAMP))
    ON CONFLICT (id)
    DO UPDATE SET
        id                          = excluded.id,
        board                       = excluded.board,
        archive                     = excluded.archive,
        last_modified_archive       = excluded.last_modified_archive;
    "#
}

pub fn thread_get<'a>() -> &'a str {
    r#"
    SELECT * FROM posts WHERE board=$1 AND (no=$2 or resto=$2) ORDER BY no;
    "#
}
pub fn thread_get_media<'a>() -> &'a str {
    // SELECT * FROM posts WHERE board=$1 AND (no=$2 or resto=$2) AND md5 IS NOT NULL ORDER BY no;
    r#"
    SELECT posts.resto, posts.no, posts.tim, posts.ext, posts.filename, media.* FROM posts
    INNER JOIN media ON posts.md5 = media.md5
    WHERE posts.board = $1  AND (posts.resto=$2 or posts.no=$2)
                            AND posts.no >= $3
                            AND posts.md5 IS NOT null
                            AND NOT (CASE WHEN posts.filedeleted is not null THEN posts.filedeleted = true ELSE false END);
    "#
}

pub fn thread_get_last_modified<'a>() -> &'a str {
    r#"
        SELECT COALESCE(op.last_modified, latest_post.last_modified) as "last_modified" FROM
        (
            SELECT to_char(to_timestamp(max("time")) AT TIME ZONE 'UTC', 'Dy, DD Mon YYYY HH24:MI:SS GMT') AS "last_modified"
            FROM posts WHERE board=$1 AND (no=$2 OR resto=$2)
        ) latest_post
        LEFT JOIN
        (
            SELECT to_char(to_timestamp("last_modified") AT TIME ZONE 'UTC', 'Dy, DD Mon YYYY HH24:MI:SS GMT') AS "last_modified"
            FROM posts WHERE board=$1 AND no=$2 AND resto=0
        ) op
        ON true; 
    "#
}
pub fn thread_upsert<'a>() -> &'a str {
    r#"
        SELECT COUNT(*) from thread_upsert($1, $2::JSONB);
    "#
}

/// Update a thread's `last_modified`.
// The caller of this function only calls this when it's actually modified,
// so we don't have to set a clause to update if things changed.
pub fn thread_update_last_modified<'a>() -> &'a str {
    r#"
        UPDATE posts
            SET last_modified = extract(epoch from $1::TEXT::TIMESTAMP)
        WHERE board=$2 and resto=0 and no=$3;
    "#
}

/// Mark a thread as deleted
pub fn thread_update_deleted<'a>() -> &'a str {
    r#"
        UPDATE posts
        SET deleted_on      = extract(epoch from now())::INT8,
            last_modified   = extract(epoch from now())::INT8,
            replies         = COALESCE(new_vals.replies, posts.replies),
            images          = COALESCE(new_vals.images, posts.images)
            
        FROM 
            (
                SELECT MIN(board) AS board, MIN(resto) AS resto, MAX(deleted_on) AS deleted_on, COUNT(no) AS replies, COUNT(md5) AS "images" FROM posts WHERE board = $1 AND resto = $2
            ) as new_vals
        WHERE
            posts.board = $1 AND posts.no = $2 AND
            (posts.deleted_on is NULL or posts.deleted_on != new_vals.deleted_on or posts.replies != new_vals.replies or posts.images != new_vals.images)
        RETURNING *;
    "#
}

/// Mark down any post found in the thread that was deleted
pub fn thread_update_deleteds<'a>() -> &'a str {
    r#"
    UPDATE
	    posts
    SET
    	deleted_on		= extract(epoch from now())::INT8,
    	last_modified	= extract(epoch from now())::INT8
    FROM
    	(
    	-- Get the deleted posts (posts not in latest)
    	-- Bumped off posts should not be marked deleted 
    	SELECT post.* FROM
    	(SELECT * FROM jsonb_populate_recordset(null::"schema_4chan_with_extra", $3::JSONB->'posts')) latest
    	FULL JOIN
    	(SELECT * FROM "posts" WHERE "board"=$1 AND ("no"=$2 or resto=$2) AND "no" >= 
    		-- Get the first reply no or default to OP no
    		-- We do this so we don't fetch a huge 10,000+ post thread in the db,
    		-- and only get what's needed to diff with the latest 
    			(SELECT 
    					(CASE WHEN ("thread"->'posts'->1->'no')::INT8 IS NOT NULL
    					THEN ("thread"->'posts'->1->'no')::INT8
    					ELSE ("thread"->'posts'->0->'no')::INT8 END)
    			FROM (SELECT $3::JSONB as "thread")o1
    			)
    		ORDER BY "no") post
    	ON post.no = latest.no
    	WHERE latest.no is null
    	) AS deleted_posts
    WHERE
    	-- Only where the entry exists and if there can be made a change
    	posts.board = deleted_posts.board AND
    	posts.no = deleted_posts.no AND
    	EXISTS (
    		SELECT posts.deleted_on
    		EXCEPT
    		SELECT COALESCE(posts.deleted_on, extract(epoch from now())::INT8) as deleted_on
    	)
    RETURNING *;
    "#
}
/// combined, only run once at startup
// this only looks complex cause I tried to get threads and archive in a single query
// json must be in proper form or not there will be: ERROR:  cannot extract elements from an object
// SQL state: 22023
pub fn threads_get_combined<'a>() -> &'a str {
    r#"
    SELECT jsonb_array_elements(threads_json)::INT8 as "no" FROM (
    	SELECT (CASE WHEN $1='threads' THEN _threads ELSE archive END) as threads_json FROM
    	(
    		SELECT jsonb_agg(o0.no) _threads FROM 
    		(SELECT (jsonb_array_elements(jsonb_array_elements(threads::JSONB)->'threads')->'no')::INT8 as "no" FROM "boards" WHERE id = $2)o0
    	)o1, "boards" where id=$2
    ) o2
    UNION
    SELECT jsonb_array_elements(threads_json)::INT8 as "no" FROM (
    	SELECT (CASE WHEN $1='threads' THEN _threads ELSE $3::JSONB END) as threads_json FROM
    	(
    		SELECT jsonb_agg(o3.no) _threads FROM 
    		(SELECT (jsonb_array_elements(jsonb_array_elements($3::JSONB)->'threads')->'no')::INT8 as "no")o3
    	)o4
    ) o5;
    "#
}

/// modified (on subsequent fetch. --watch-thread/--watch-board)
/// on threads modified, deleted, archived?
/// Only applies to threads. Archives call get_combined.
pub fn threads_get_modified<'a>() -> &'a str {
    r#"
        SELECT COALESCE((latest->'no')::INT8,(previous->'no')::INT8) AS "no" FROM
        (SELECT jsonb_array_elements(jsonb_array_elements(threads)->'threads') AS previous FROM boards where id = $1)o0
        FULL JOIN
        (SELECT jsonb_array_elements(jsonb_array_elements($2::JSONB)->'threads') AS latest)o1
        ON previous -> 'no' = (latest -> 'no')
        WHERE latest IS NULL OR previous IS NULL OR NOT previous->'last_modified' <@ (latest -> 'last_modified')
    "#
}

pub fn post_get_single<'a>() -> &'a str {
    "SELECT * from posts where board=$1 and resto=$2 and no=$3 LIMIT 1;"
}

pub fn post_get_media<'a>() -> &'a str {
    r#"
    SELECT * FROM media WHERE sha256t=$1::bytea LIMIT 1;
    "#
}

pub fn post_upsert_media<'a>() -> &'a str {
    r#"
    INSERT INTO media("md5", "sha256", "sha256t") VALUES($1, $2, $3)
    ON CONFLICT ("md5")
    DO UPDATE SET
        "sha256"    = excluded."sha256", 
        "sha256t"   = excluded."sha256t"
    WHERE EXISTS (
    		SELECT media."sha256", media."sha256t"
    		EXCEPT
    		SELECT excluded."sha256", excluded."sha256t"
    	);
    "#
}

pub fn get_thread_index<'a>() -> &'a str {
    r#"
    select json_strip_nulls(json_agg(jj.*)) from (
    select thread.*, last_replies from (
    select o1.no,o1.board, json_strip_nulls(json_agg(o2.* order by o2.no)) as last_replies from 
    (
        select op.*, coalesce(reply.last_no, op.no) as combined_last_no, coalesce(reply.last_time, op.time) as combined_last_time from
        (select * from (select board, resto, max(no) as last_no, max(time) as last_time, max(tim) as last_tim from posts group by resto,board having resto!=0)x)reply
        FULL JOIN
        (select board, resto, no, time,replies from posts where resto=0)op
        on reply.board = op.board and reply.resto=op.no
        -- faster here
        -- where op.no < 124205675 and op.board=1
        where op.board=1
        order by combined_last_no desc
        limit 15
    )o1
    LEFT JOIN LATERAL
    (select * from (select * from posts WHERE (no=o1.no or resto=o1.no) and resto!=0 and board=o1.board order by no desc limit 5 )oo order by no)o2
    on o1.no=o2.resto and o1.board=o2.board
    group by o1.no,o1.board
    ) thread_w_last_replies
    LEFT JOIN LATERAL
    (select * from posts where resto=0) thread
    on thread.no = thread_w_last_replies.no and thread.board = thread_w_last_replies.board
    -- where thread.board=1
    order by (CASE WHEN replies=0 THEN thread_w_last_replies.no else (last_replies->-1->>'no')::bigint end) desc
    -- limit 15;
    )jj;
    // \g output.json
    "#
}

/// The schema for each row (post).  
///
/// Optimized specifically for Postgres with [column tetris](https://www.2ndquadrant.com/en/blog/on-rocks-and-sand/) to save space.  
/// The original 4chan schema is used as a base with reasonable changes made.  
///
/// ## Added
/// - [`subnum`](struct.Post.html#structfield.subnum) For ghost posts
/// - [`deleted_on`](struct.Post.html#structfield.deleted_on) For context
/// - [`last_modified`](struct.Post.html#structfield.last_modified) For context and possible integration
///   alongside search engines
/// - [`sha256`](struct.Post.html#structfield.sha256) For file dedup and to prevent [MD5 collisions](https://github.com/4chan/4chan-API/issues/70)
/// - [`sha256t`](struct.Post.html#structfield.sha256t) For thumbnail dedup and to prevent [MD5 collisions](https://github.com/4chan/4chan-API/issues/70)
/// - [`extra`](struct.Post.html#structfield.extra) For any future schema changes
/// ## Removed
/// - [`archived`](struct.Post.html#structfield.archived) Redundant with
///   [`archived_on`](struct.Post.html#structfield.archived_on)  
/// ## Modified  
/// - [`md5`](struct.Post.html#structfield.md5) From base64 to binary, to save space
/// - To [`bool`], to save space
///     - [`sticky`](struct.Post.html#structfield.sticky)
///     - [`closed`](struct.Post.html#structfield.closed)
///     - [`filedeleted`](struct.Post.html#structfield.filedeleted)
///     - [`spoiler`](struct.Post.html#structfield.spoiler)
///     - [`m_img`](struct.Post.html#structfield.m_img)
///     - [`bumplimit`](struct.Post.html#structfield.bumplimit)
///     - [`imagelimit`](struct.Post.html#structfield.imagelimit)
/// ## Side notes  
/// Postgres does not have a numeric smaller than [`i16`], nor do they have unsigned integers.  
/// For example: [`custom_spoiler`](struct.Post.html#structfield.custom_spoiler) should ideally
/// be [`u8`].
///
/// ---
/// <details><summary>▶ <b>SQL table query</b></summary>
/// <p>
///
/// ```sql
///                     Table "public.posts"
///      Column     |   Type   | Collation | Nullable | Default
/// ----------------+----------+-----------+----------+---------
///  no             | bigint   |           | not null |
///  subnum         | bigint   |           |          |
///  tim            | bigint   |           |          |
///  resto          | bigint   |           | not null |
///  time           | bigint   |           | not null |
///  last_modified  | bigint   |           |          |
///  archived_on    | bigint   |           |          |
///  deleted_on     | bigint   |           |          |
///  fsize          | integer  |           |          |
///  w              | integer  |           |          |
///  h              | integer  |           |          |
///  replies        | integer  |           |          |
///  images         | integer  |           |          |
///  unique_ips     | integer  |           |          |
///  board          | smallint |           | not null |
///  tn_w           | smallint |           |          |
///  tn_h           | smallint |           |          |
///  custom_spoiler | smallint |           |          |
///  since4pass     | smallint |           |          |
///  sticky         | boolean  |           |          |
///  closed         | boolean  |           |          |
///  filedeleted    | boolean  |           |          |
///  spoiler        | boolean  |           |          |
///  m_img          | boolean  |           |          |
///  bumplimit      | boolean  |           |          |
///  imagelimit     | boolean  |           |          |
///  md5            | bytea    |           |          |
///  now            | text     |           |          |
///  name           | text     |           |          |
///  sub            | text     |           |          |
///  com            | text     |           |          |
///  filename       | text     |           |          |
///  ext            | text     |           |          |
///  trip           | text     |           |          |
///  id             | text     |           |          |
///  capcode        | text     |           |          |
///  country        | text     |           |          |
///  troll_country  | text     |           |          |
///  country_name   | text     |           |          |
///  semantic_url   | text     |           |          |
///  tag            | text     |           |          |
///  extra          | jsonb    |           |          |
/// Indexes:
///     "posts_no_key" UNIQUE CONSTRAINT, btree (no)
///     "ix_board" brin (board) WITH (pages_per_range='32', autosummarize='on')
///     "ix_board_post_last_modified" brin (board, resto, last_modified) WITH (pages_per_range='32', autosummarize='on')
///     "ix_md5" brin (md5) WITH (pages_per_range='32', autosummarize='on') WHERE md5 IS NOT NULL
///     "ix_no" brin (no) WITH (pages_per_range='32', autosummarize='on')
/// Foreign-key constraints:
///     "posts_board_fkey" FOREIGN KEY (board) REFERENCES boards(id)
///     "posts_md5_fkey" FOREIGN KEY (md5) REFERENCES media(md5)
/// ```
///
/// </p>
/// </details>
/// 
/// ---  
/// The following below is taken from the [official docs](https://github.com/4chan/4chan-API) where applicable.  
#[cold]
#[rustfmt::skip]
#[derive(ToSql, FromSql, Deserialize, Serialize, Debug, Clone)]
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
    pub fsize: Option<i32>,

    /// Appears: `always if post has attachment`  
    /// Possible values: `any positive integer`  
    /// <font style="color:#789922;">> Image width dimension</font>
    pub w: Option<i32>,

    /// Appears: `always if post has attachment`  
    /// Possible values: `any positive integer`  
    /// <font style="color:#789922;">> Image height dimension</font>
    pub h: Option<i32>,

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
    pub board: i16,

    /// Appears: `always if post has attachment`  
    /// Possible values: `any positive integer`  
    /// <font style="color:#789922;">> Thumbnail image width dimension</font>
    pub tn_w: Option<i16>,

    /// Appears: `always if post has attachment`  
    /// Possible values: `any positive integer`  
    /// <font style="color:#789922;">> Thumbnail image height dimension</font>
    pub tn_h: Option<i16>,

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

    /// Appears: `always if post has attachment`  
    /// Possible values:   
    /// <font style="color:#789922;">> binary representation of the `MD5` hash of the file (24 characters in hex)</font>
    pub md5: Option<Vec<u8>>,

    /// Appears: `always if post has attachment`  
    /// Possible values:   
    /// <font style="color:#789922;">> binary representation of the `SHA256` hash of the file (65 characters in hex)</font>
    pub sha256: Option<Vec<u8>>,

    /// Appears: `always if post has thumbnail attachment`, excludes `/f/`  
    /// Possible values:  
    /// <font style="color:#789922;">> binary representation of the `SHA256` hash of the thumbnail (65 characters in hex)</font>
    pub sha256t: Option<Vec<u8>>,

    /// Appears: `always`
    /// Possible values: `any string`  
    /// <font style="color:#789922;">> MM/DD/YY(Day)HH:MM (:SS on some boards), EST/EDT timezone</font>
    pub now: String,

    /// Appears: `always` <sup><b>Note:</b> Can be and empty string if user has a tripcode and opted out of a name</sup>  
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
    /// Possible values: `2 character string` or `XX` if unknown  
    /// <font style="color:#789922;">> Poster's [ISO 3166-1 alpha-2 country code](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)</font>
    pub country: Option<String>,

    /// Appears: `if troll country flags are enabled`  
    /// Possible values: `2 character string`  
    /// <font style="color:#789922;">> Poster's custom country</font>
    pub troll_country: Option<String>,

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

    /// Appears: `always if there are extra fields`   
    /// Possible values: a [`JSON`](https://docs.rs/serde_json/*/serde_json/enum.Value.html) containing the extra fields  
    /// <font style="color:#789922;">> Optional field for extra column additions</font>
    pub extra: Option<serde_json::Value>,
}

impl Default for Post {
    fn default() -> Self {
        let t = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
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
            replies:        None,
            images:         None,
            unique_ips:     None,
            board:          0,
            tn_w:           None,
            tn_h:           None,
            custom_spoiler: None,
            since4pass:     None,
            sticky:         None,
            closed:         None,
            filedeleted:    None,
            spoiler:        None,
            m_img:          None,
            bumplimit:      None,
            imagelimit:     None,
            md5:            None,
            sha256:         None,
            sha256t:        None,
            now:            "".into(),
            name:           None,
            sub:            None,
            com:            None,
            filename:       None,
            ext:            None,
            trip:           None,
            id:             None,
            capcode:        None,
            country:        None,
            troll_country:  None,
            country_name:   None,
            semantic_url:   None,
            tag:            None,
            extra:          None,
        }
    }
}
