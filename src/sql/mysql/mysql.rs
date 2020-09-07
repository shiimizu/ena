#![allow(unused_imports)]
use super::{DropExecutor, Query, QueryExecutor};
use crate::{
    config::{Board, Opt},
    yotsuba, ThreadType,
};
use anyhow::{anyhow, Result};
use async_rwlock::RwLock;
use async_trait::async_trait;
use fomat_macros::{epintln, fomat, pintln};
use format_sql_query::*;
use futures::{
    future::Either,
    stream::{Iter, StreamExt},
};
use html_escape::decode_html_entities as sanitize; // unescape
use itertools::Itertools;
use mysql_async::prelude::*;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    borrow::{Borrow, Cow},
    collections::{HashMap, HashSet},
};
use strum::IntoEnumIterator;
pub mod clean;
pub use clean::*;
mod asagi;
use crate::{get_ctrlc, sleep};
use asagi::*;
pub use queries::*;
use std::time::Duration;

/// List of prepared statments to query
pub type BoardId = u16;
pub type StatementStore = HashMap<BoardId, HashMap<Query, String>>;

/// List of computged prepared statments
static STATEMENTS: Lazy<RwLock<StatementStore>> = Lazy::new(|| RwLock::new(HashMap::new()));

fn get_sql_template(board: &str, engine: &str, charset: &str, collate: &str, query: &str) -> String {
    query.replace("%%ENGINE%%", engine).replace("%%BOARD%%", board).replace("%%CHARSET%%", charset).replace("%%COLLATE%%", collate)
}

use futures::prelude::*;
struct AsagiInner {
    t:              String,
    direct_db_pool: mysql_async::Pool,
}

impl AsagiInner {
    fn get_db_conn(&self) -> impl Future<Output = mysql_async::Result<mysql_async::Conn>> {
        let b = true;
        match b {
            true => Either::Left(self.direct_db_pool.get_conn().and_then(|mut conn| async {
                conn.query_drop("SET time_zone='+00:00';").await?;
                Ok(conn)
            })),
            false => Either::Right(self.direct_db_pool.get_conn()),
        }
    }
}
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Eq)]
struct Page {
    page:    u8,
    threads: HashSet<Threade>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default, Eq, Hash)]
struct Threade {
    no:            u64,
    last_modified: u64,
}

#[async_trait]
impl DropExecutor for mysql_async::Pool {
    async fn disconnect_pool(self) -> Result<()> {
        self.disconnect().await.map_err(|e| anyhow!(e))
    }
}

fn unix_timestamp() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

#[async_trait]
impl QueryExecutor for mysql_async::Pool {
    async fn boards_index_get_last_modified(&self) -> Result<Option<String>> {
        let fun_name = "boards_index_get_last_modified";
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&1).and_then(|queries| queries.get(&Query::BoardsIndexGetLastModified)).ok_or_else(|| anyhow!("{}: Empty query statement!", fun_name))?;
        let res: Result<Option<Option<String>>, mysql_async::Error> = conn.exec_first(statement.as_str(), ()).await;
        let res = res.map(|r| r.flatten()).map_err(|e| anyhow!(e));
        res
    }

    async fn boards_index_upsert(&self, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let fun_name = "boards_index_upsert";
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let res = store
            .get(&1)
            .and_then(|queries| queries.get(&Query::BoardsIndexUpsert))
            .zip(Some(serde_json::to_string(json)?))
            .map(|(statement, json)| conn.exec_drop(statement.as_str(), (json, last_modified)))
            .ok_or_else(|| anyhow!("{}: Empty query statement!", fun_name))?
            .await;
        res.map(|_| 0).map_err(|e| anyhow!(e))
    }

    async fn board_is_valid(&self, board: &str) -> Result<bool> {
        let fun_name = "board_is_valid";
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let res: Result<Option<Option<String>>, _> =
            store.get(&1).and_then(|queries| queries.get(&Query::BoardIsValid)).map(|stmt| conn.exec_first(stmt.as_str(), ())).ok_or_else(|| anyhow!("{}: Empty query statement!", fun_name))?.await;
        let res = res.map(|r| r.flatten())?;
        res.map_or_else(
            || Ok(false),
            |res| {
                let json = serde_json::from_str::<serde_json::Value>(&res)?;
                let _boards = json.get("boards").and_then(|j| j.as_array()).ok_or_else(|| anyhow!("{}: Error accessing `boards` from json", fun_name))?;
                let res = _boards.iter().map(|v| v.get("board").and_then(|v| v.as_str())).any(|_board| _board == Some(board));
                Ok(res)
            },
        )
    }

    async fn board_upsert(&self, board: &str) -> Result<u64> {
        let fun_name = "board_upsert";
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&1).and_then(|queries| queries.get(&Query::BoardUpsert)).ok_or_else(|| anyhow!("{}: Empty query statement", fun_name))?;
        let res = conn.exec_drop(statement.as_str(), params! { "board" => board }).await;
        res.map(|_| 0).map_err(|e| anyhow!(e))
    }

    async fn board_table_exists(&self, board: &str, opt: &Opt) -> Option<String> {
        let mut conn = self.get_conn().await.ok()?;

        // Init the `boards` table
        if board == "boards" {
            // pintln!("board_table_exists: Creating `boards` table");
            let boards_table = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/boards_table.sql"));
            let common = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/common.sql"));
            conn.query_drop(&boards_table).await.ok()?;
            conn.query_drop(&common).await.ok()?;
            // No one checks the result of this function anyways so it's okay to return anything
            return None;
        }

        let store = STATEMENTS.read().await;
        let map = store.get(&1)?;
        let statement = map.get(&Query::BoardTableExists)?;
        let res: Result<Option<Option<String>>, _> = conn.exec_first(statement.as_str(), (&opt.database.name, board)).await;
        let res = res.unwrap().flatten();
        if res.is_none() {
            let boards = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/boards.sql"));
            let triggers = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/triggers.sql"));
            let query = [boards, triggers].concat();
            pintln!("Creating tables for /"(board)"/");
            conn.query_drop(query).await.ok()?;
        }
        res
    }

    async fn board_get(&self, board: &str) -> Result<Option<u16>> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&1).and_then(|queries| queries.get(&Query::BoardGet)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::BoardGet))?;
        let res: Result<Option<mysql_async::Row>> = conn.exec_first(statement.as_str(), (board,)).await.map_err(|e| anyhow!(e));
        res.map(|opt| opt.and_then(|row| row.get::<Option<u16>, &str>("id")).flatten())
    }

    async fn board_get_last_modified(&self, thread_type: ThreadType, board_info: &Board) -> Option<String> {
        let mut conn = self.get_conn().await.ok()?;
        let store = STATEMENTS.read().await;
        let map = store.get(&1)?;
        let statement = map.get(&Query::BoardGetLastModified)?;
        let res: Result<Option<Option<String>>, _> = conn.exec_first(statement.as_str(), (thread_type.is_threads(), board_info.id)).await;
        res.unwrap().flatten()
    }

    async fn board_upsert_threads(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&1).and_then(|queries| queries.get(&Query::BoardUpsertThreads)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::BoardUpsertThreads))?;
        let json = serde_json::to_string(json)?;
        let res: Result<Option<mysql_async::Row>> = conn.exec_first(statement.as_str(), (board_id, board, json, last_modified)).await.map_err(|e| anyhow!(e));
        Ok(0)
    }

    async fn board_upsert_archive(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&1).and_then(|queries| queries.get(&Query::BoardUpsertArchive)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::BoardUpsertArchive))?;
        let json = serde_json::to_string(json)?;
        let res: Result<Option<mysql_async::Row>> = conn.exec_first(statement.as_str(), (board_id, board, json, last_modified)).await.map_err(|e| anyhow!(e));
        Ok(0)
    }

    async fn thread_get(&self, board_info: &Board, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>> {
        let mut conn = self.get_conn().await;
        match conn {
            Err(e) => Either::Right(Err(anyhow!(e))),
            Ok(mut conn) => {
                let store = STATEMENTS.read().await;
                let statement = store.get(&board_info.id).and_then(|queries| queries.get(&Query::ThreadGet)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::ThreadGet));
                match statement {
                    Err(e) => Either::Right(Err(e)),
                    Ok(statement) => {
                        let res: Result<Vec<mysql_async::Row>> = conn.exec(statement.as_str(), (thread,)).await.map_err(|e| anyhow!(e));
                        Either::Right(res)
                    }
                }
            }
        }
    }

    /// Unused
    async fn thread_get_media(&self, board_info: &Board, thread: u64, start: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>> {
        let mut conn = self.get_conn().await;

        match conn {
            Err(e) => Either::Right(Err(anyhow!(e))),
            Ok(mut conn) => {
                let store = STATEMENTS.read().await;
                let statement = store.get(&board_info.id).and_then(|queries| queries.get(&Query::ThreadGetMedia)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::ThreadGetMedia));
                match statement {
                    Err(e) => Either::Right(Err(e)),
                    Ok(statement) => {
                        let res: Result<Vec<mysql_async::Row>> = conn.exec(statement.as_str(), (thread,)).await.map_err(|e| anyhow!(e));
                        Either::Right(res)
                    }
                }
            }
        }
    }

    async fn thread_get_last_modified(&self, board_id: u16, thread: u64) -> Option<String> {
        let mut conn = self.get_conn().await.ok()?;
        let store = STATEMENTS.read().await;
        let map = store.get(&board_id)?;
        let statement = map.get(&Query::ThreadGetLastModified)?;
        let res: Result<Option<Option<String>>, mysql_async::Error> = conn.exec_first(statement.as_str(), params! {"thread" => thread}).await;
        loop {
            match res {
                Err(_) => {
                    if get_ctrlc() {
                        return None;
                        //break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }
                Ok(_res) => {
                    return _res.flatten();
                }
            }
        }
    }

    async fn thread_upsert(&self, board_info: &Board, thread_json: &serde_json::Value) -> Result<u64> {
        let mut posts: Vec<yotsuba::Post> = serde_json::from_value(thread_json["posts"].clone())?;
        // TODO: This should never happen
        if posts.len() == 0 {
            return Ok(0);
        }

        // Preserve `unique_ips` when a thread is archived
        if posts[0].unique_ips.is_none() {
            let mut conn = self.get_conn().await?;
            let store = STATEMENTS.read().await;
            let stmt_post_get_single = store.get(&board_info.id).and_then(|queries| queries.get(&Query::PostGetSingle)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::PostGetSingle))?;
            let thread = posts[0].no;
            let mut single_res: Option<mysql_async::Row> = conn.exec_first(stmt_post_get_single.as_str(), (thread, thread)).await?;
            let prev_unique_ips = single_res
                .and_then(|row| row.get::<Option<String>, &str>("exif").flatten())
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|j| j.as_object().cloned())
                .and_then(|obj| obj.get("uniqueIps").cloned())
                .and_then(|json| json.as_str().map(|s| s.parse::<u32>().ok()).flatten());
            posts[0].unique_ips = prev_unique_ips;
        }

        let mut conn = self.get_conn().await?;

        // Get the size of differences (upserted)
        let res_diff_len = {
            let start = posts[if posts.len() == 1 { 0 } else { 1 }].no;

            // Select all posts from DB starting from new json post's first reply `no`
            // (default to OP if noreplies).
            let thread = posts[0].no;
            let query = format!(
                "select * from `{board}` where ((thread_num = {thread} and num >= {start}) or (thread_num = {thread} and op=1) ) and subnum = 0  order by num;",
                board = &board_info.board,
                start = start,
                thread = thread
            );
            let res: Vec<mysql_async::Row> = conn.query(query.as_str()).await?;
            let rows: HashSet<Post> = res
                .into_iter()
                .map(|row| {
                    let mut p = Post::from(row);
                    p.doc_id = 0;
                    p.media_id = 0;
                    p.poster_ip = 0.0;
                    p
                })
                .collect();

            let converted_posts: HashSet<Post> = posts.iter().filter(|post| post.time != 0 && post.no != 0).map(|p| Post::from(p)).collect();
            let res = converted_posts.symmetric_difference(&rows).unique_by(|p| p.num).count();
            res
        };

        // This filter is for accomodating 4chan's side when a thread lingers (is still live) after deletion
        // without a `no` field and no replies, and also for tail json's OP
        let posts_iter = posts.iter().filter(|post| post.time != 0 && post.no != 0).enumerate();
        let posts_iter_len = posts_iter.clone().count();
        // Manually stitch the query together so we can have multiple values
        let mut q = fomat!(
        r#"
        INSERT INTO `"# (&board_info.board) r#"`
        (poster_ip, num, subnum, thread_num, op, `timestamp`, timestamp_expired, preview_orig, preview_w, preview_h,
        media_filename, media_w, media_h, media_size, media_hash, media_orig, spoiler, deleted,
        capcode, email, `name`, trip, title, comment, delpass, sticky, locked, poster_hash, poster_country, exif)
        VALUES
            "#
        for (i, post) in posts_iter {
            "\n"
            (asagi::Post::from(post).to_sql())
            if i < posts_iter_len-1 { "," } else { "" }
        }
        r#"
        ON DUPLICATE KEY UPDATE
        `poster_ip`=VALUES(`poster_ip`),
        `op`=VALUES(`op`),
        `timestamp`=VALUES(`timestamp`),
        `timestamp_expired`=VALUES(`timestamp_expired`),
        -- Don't update media when filedeleted since media info will be wiped
        -- preview_orig=VALUES(preview_orig),
        -- preview_w=VALUES(preview_w),
        -- preview_h=VALUES(preview_h),
        -- media_filename=COALESCE(VALUES(media_filename), media_filename),
        -- media_w=VALUES(media_w),
        -- media_h=VALUES(media_h),
        -- media_size=VALUES(media_size),
        -- media_hash=VALUES(media_hash),
        -- media_orig=VALUES(media_orig),
        -- spoiler=VALUES(spoiler),
        deleted=VALUES(deleted),
        capcode=VALUES(capcode),
        email=VALUES(email),
        `name`=VALUES(`name`),
        trip=VALUES(trip),
        title=VALUES(title),
        comment=VALUES(comment),
        delpass=VALUES(delpass),
        sticky = (VALUES(sticky) OR sticky ),
        locked = (VALUES(locked) OR locked ),
        poster_hash=VALUES(poster_hash),
        poster_country=VALUES(poster_country),
        exif=VALUES(exif);"#
        );
        loop {
            // TODO: Return rows affected
            if let Ok(_) = conn.query_drop(q.as_str()).await {
                break;
            }
            if get_ctrlc() {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
        Ok(res_diff_len as u64)
    }

    async fn thread_update_last_modified(&self, last_modified: &str, board_id: u16, thread: u64) -> Result<u64> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&board_id).and_then(|queries| queries.get(&Query::ThreadUpdateLastModified)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::ThreadUpdateLastModified))?;
        let res = conn.exec_drop(statement.as_str(), (last_modified, thread)).await;
        res.map(|_| 0).map_err(|e| anyhow!(e))
    }

    async fn thread_update_deleted(&self, board_id: u16, thread: u64) -> Result<Either<tokio_postgres::RowStream, Option<u64>>> {
        // TODO the return of this function should just be u64 or bool
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let map = store.get(&board_id).ok_or_else(|| anyhow!("thread_update_deleted: Empty value getting key `{}` in statement store", &board_id))?;

        let stmt_thread_update_deleted = map.get(&Query::ThreadUpdateDeleted).ok_or_else(|| anyhow!("{}: Empty query statement", Query::ThreadUpdateDeleted))?;
        let stmt_post_get_single = map.get(&Query::PostGetSingle).ok_or_else(|| anyhow!("{}: Empty query statement", Query::PostGetSingle))?;

        let res = conn.exec_drop(stmt_thread_update_deleted.as_str(), (Post::timestamp_nyc(unix_timestamp()), thread)).await?;

        // Get the post afterwards to see if it was deleted
        let mut single_res: Option<mysql_async::Row> = conn.exec_first(stmt_post_get_single.as_str(), (thread, thread)).await?;
        let status = single_res.and_then(|row| row.get::<Option<u8>, &str>("deleted").flatten()).map_or_else(|| None, |deleted| if deleted == 1 { Some(thread) } else { None });
        Ok(Either::Right(status))
    }

    async fn thread_update_deleteds(&self, board_info: &Board, thread: u64, json: &serde_json::Value) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>> {
        let posts = json.get("posts").and_then(|posts| posts.as_array()).ok_or_else(|| anyhow!("Empty value getting `posts` array from json"))?;

        // TODO this should never happen.
        if posts.len() == 0 {
            return Ok(Either::Right(None));
        }
        let start = posts[if posts.len() == 1 { 0 } else { 1 }]["no"].as_u64().unwrap_or_default();
        let mut conn = self.get_conn().await?;

        // Select all posts from DB starting from new json post's first reply `no`
        // (default to OP if noreplies).
        let query = format!("select * from `{board}` where thread_num = {thread} and num >= {start} and subnum = 0;", board = &board_info.board, start = start, thread = thread);
        let res: Vec<mysql_async::Row> = conn.query(query.as_str()).await?;
        let rows: Vec<Post> = res.into_iter().map(|row| Post::from(row)).collect();

        // latest posts (v_latest) can include OP post since we're taking the difference. should not filter
        // it out (diff = values in self, but not in other).
        let v_latest: HashSet<_> = posts.iter().map(|v| v.get("no").and_then(|val| val.as_u64()).unwrap()).collect();
        let v_previous: HashSet<_> = rows.iter().filter(|post| !post.deleted).map(|post| post.num).collect();
        let diff: Vec<_> = v_previous.difference(&v_latest).unique().copied().collect();
        if diff.is_empty() {
            return Ok(Either::Right(None));
        }
        let mut list_no_iter = diff.iter().enumerate();

        // Manually stitch the query together so we can have multiple values/batch upsert
        let query = fomat!(
        r#"
                INSERT INTO `"# (&board_info.board) r#"`
                (num, subnum, deleted, `timestamp`, `timestamp_expired`)
                VALUES
                    "#
        for (i, no) in list_no_iter {
            "\n(" (no) ", 0, 1, UNIX_TIMESTAMP(), " ((Post::timestamp_nyc(unix_timestamp()))) ")"
            if i < diff.len()-1 { "," } else { "" }
        }
        r#"
                ON DUPLICATE KEY UPDATE
                deleted=VALUES(`deleted`),
                timestamp_expired=VALUES(`timestamp_expired`);"#
        );
        while let Err(e) = conn.query_drop(query.as_str()).await {
            if get_ctrlc() {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }

        Ok(Either::Right(Some(diff)))
    }

    async fn threads_get_combined(&self, thread_type: ThreadType, board_id: u16, json: &serde_json::Value) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&board_id).and_then(|queries| queries.get(&Query::ThreadsGetCombined)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::ThreadsGetCombined))?;
        let res: Option<Option<String>> = conn.exec_first(statement.as_str(), (thread_type.is_threads(), board_id)).await?;
        let res = res.flatten();
        if let Some(res) = res {
            // If an entry exists in the database we can apply a diff
            if thread_type.is_threads() {
                let mut prev: Vec<Page> = serde_json::from_str(&res)?;
                let mut new: Vec<Page> = serde_json::from_value(json.clone())?;
                let a: HashSet<_> = prev.into_iter().flat_map(|page| page.threads).collect();
                let b: HashSet<_> = new.into_iter().flat_map(|page| page.threads).collect();
                let diff: Vec<_> = a.union(&b).map(|thread| thread.no).unique().collect();
                Ok(Either::Right(Some(diff)))
            } else {
                let mut prev: Vec<u64> = serde_json::from_str(&res)?;
                let mut new: Vec<u64> = serde_json::from_value(json.clone())?;
                let diff: Vec<u64> = prev.into_iter().chain(new.into_iter()).unique().collect();
                Ok(Either::Right(Some(diff)))
            }
        } else {
            // If an entry does not exist in the database just use the json received
            // This usually occurs on startup when there's no cache of threads/archive for the board
            if thread_type.is_threads() {
                let mut received: Vec<Page> = serde_json::from_value(json.clone())?;
                let res: Vec<u64> = received.into_iter().flat_map(|page| page.threads.into_iter().map(|thread| thread.no)).unique().collect();
                Ok(Either::Right(Some(res)))
            } else {
                let mut res: Vec<u64> = serde_json::from_value(json.clone())?;
                Ok(Either::Right(Some(res)))
            }
        }
    }

    async fn threads_get_modified(&self, board_id: u16, json: &serde_json::Value) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&board_id).and_then(|queries| queries.get(&Query::ThreadsGetModified)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::ThreadsGetModified))?;

        let res: Option<Option<String>> = conn.exec_first(statement.as_str(), (board_id,)).await?;
        let res = res.flatten();
        if let Some(res) = res {
            let mut prev: Vec<Page> = serde_json::from_str(&res)?;
            let mut new: Vec<Page> = serde_json::from_value(json.clone())?;
            let a: HashSet<_> = prev.into_iter().flat_map(|page| page.threads).collect();
            let b: HashSet<_> = new.into_iter().flat_map(|page| page.threads).collect();
            let diff: Vec<_> = a.symmetric_difference(&b).map(|thread| thread.no).unique().collect();
            Ok(Either::Right(Some(diff)))
        } else {
            Ok(Either::Right(None))
        }
    }

    async fn post_get_single(&self, board_id: u16, thread: u64, no: u64) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let store = STATEMENTS.read().await;
        let statement = store.get(&board_id).and_then(|queries| queries.get(&Query::PostGetSingle)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::PostGetSingle))?;
        let res: Result<Option<mysql_async::Row>, _> = conn.exec_first(statement.as_str(), (thread, no)).await;
        res.map(|opt| {
            opt.and_then(|row| {
                let _tmp: Option<u64> = row.get("num");
                _tmp
            })
            .map_or_else(|| Ok(false), |_| Ok(true))
        })
        .unwrap()
    }

    async fn post_get_media(&self, board_info: &Board, md5: &str, hash_thumb: Option<&[u8]>) -> Either<Result<Option<tokio_postgres::Row>>, Result<Option<mysql_async::Row>>> {
        match self.get_conn().await {
            Err(e) => Either::Right(Err(anyhow!(e))),
            Ok(mut conn) => {
                let store = STATEMENTS.read().await;
                let statement = store.get(&board_info.id).and_then(|queries| queries.get(&Query::PostGetMedia)).ok_or_else(|| anyhow!("{}: Empty query statement", Query::PostGetMedia));
                match statement {
                    Err(e) => Either::Right(Err(e)),
                    Ok(statement) => {
                        let mut res: Option<mysql_async::Row> = conn.exec_first(statement.as_str(), (md5,)).await.ok().flatten();
                        Either::Right(Ok(res))
                    }
                }
            }
        }
    }

    async fn post_upsert_media(&self, md5: &[u8], hash_full: Option<&[u8]>, hash_thumb: Option<&[u8]>) -> Result<u64> {
        unreachable!()
    }

    async fn init_statements(&self, board_id: u16, board: &str) -> Result<()> {
        if STATEMENTS.read().await.contains_key(&board_id) {
            return Ok(());
        }

        let mut conn = self.get_conn().await?;
        let mut map = HashMap::new();
        let actual = if board_id == 1 { super::Query::iter().filter(|q: &Query| q.to_string().starts_with("Board")).collect::<Vec<Query>>() } else { super::Query::iter().collect::<Vec<Query>>() };
        for query in actual {
            let statement = match query {
                Query::BoardsIndexGetLastModified => boards_index_get_last_modified().into(),
                Query::BoardsIndexUpsert => boards_index_upsert().into(),
                Query::BoardIsValid => board_is_valid().into(),
                Query::BoardUpsert => board_upsert().into(),
                Query::BoardTableExists => board_table_exists().into(),
                Query::BoardGet => board_get().into(),
                Query::BoardGetLastModified => board_get_last_modified().into(),
                Query::BoardUpsertThreads => board_upsert_threads().into(),
                Query::BoardUpsertArchive => board_upsert_archive().into(),
                Query::ThreadGet => thread_get(board).into(),
                Query::ThreadGetMedia => thread_get_media(board).into(),
                Query::ThreadGetLastModified => thread_get_last_modified(board).into(),
                Query::ThreadUpsert => thread_upsert(board).into(),
                Query::ThreadUpdateLastModified => thread_update_last_modified(board).into(),
                Query::ThreadUpdateDeleted => thread_update_deleted(board).into(),
                Query::ThreadUpdateDeleteds => thread_update_deleteds(board).into(),
                Query::ThreadsGetCombined => threads_get_combined().into(),
                Query::ThreadsGetModified => threads_get_modified().into(),
                Query::PostGetSingle => post_get_single(board).into(),
                Query::PostGetMedia => post_get_media(board).into(),
                Query::PostUpsertMedia => "SELECT 1;".into(),
            };
            map.insert(query, statement);
            map.remove(&Query::PostUpsertMedia);
        }
        STATEMENTS.write().await.insert(board_id, map);
        Ok(())
    }
}

/// List of available queries to make
pub mod queries {
    pub fn boards_index_get_last_modified<'a>() -> &'a str {
        r#"
        SELECT DATE_FORMAT(FROM_UNIXTIME(`last_modified_threads`), '%a, %d %b %Y %T GMT') AS `last_modified` FROM boards WHERE id=1;
    "#
    }

    pub fn boards_index_upsert<'a>() -> &'a str {
        r#"
        INSERT INTO boards(id, board, title, threads, last_modified_threads)
        VALUES(1, 'boards', 'boards.json', ?, UNIX_TIMESTAMP(STR_TO_DATE(?, '%a, %d %b %Y %T GMT')))
        ON DUPLICATE KEY UPDATE
            id                      = VALUES(id),
            board                   = VALUES(board),
            title                   = VALUES(title),
            threads                 = VALUES(threads),
            last_modified_threads   = VALUES(last_modified_threads);
    "#
    }

    pub fn board_is_valid<'a>() -> &'a str {
        // SELECT JSON_CONTAINS(JSON_EXTRACT(threads, "$.boards[*].board"), CONCAT('"', ?, '"'), "$") AS
        // board from boards where id=1;
        r#"
        SELECT threads from boards where id=1;
    "#
    }

    pub fn board_upsert<'a>() -> &'a str {
        r#"
        INSERT INTO boards(board)
        SELECT :board AS `board` FROM DUAL WHERE NOT EXISTS ( SELECT 1 FROM boards WHERE board = :board ) ;
    "#
    }

    pub fn board_table_exists<'a>() -> &'a str {
        r#"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name=?;"#
    }

    pub fn board_get<'a>() -> &'a str {
        r#"SELECT * from boards where board = ?;"#
    }

    pub fn board_get_last_modified<'a>() -> &'a str {
        // The CASE is a workaround so we can make a Statement for tokio_postgres
        r#"
    SELECT
    DATE_FORMAT(FROM_UNIXTIME(
        (CASE WHEN ? THEN last_modified_threads ELSE last_modified_archive END)
    ), '%a, %d %b %Y %T GMT')
    AS last_modified FROM boards WHERE id=?;
    "#
    }

    pub fn board_upsert_threads<'a>() -> &'a str {
        r#"
    INSERT INTO boards(id, board, threads, last_modified_threads) VALUES(?, ?, ?, UNIX_TIMESTAMP(STR_TO_DATE(?, '%a, %d %b %Y %T GMT')))
    ON DUPLICATE KEY UPDATE
        id                          = VALUES(id),
        board                       = VALUES(board),
        threads                     = VALUES(threads),
        last_modified_threads       = VALUES(last_modified_threads);
    "#
    }

    pub fn board_upsert_archive<'a>() -> &'a str {
        r#"
    INSERT INTO boards(id, board, archive, last_modified_archive) VALUES(?, ?, ?, UNIX_TIMESTAMP(STR_TO_DATE(?, '%a, %d %b %Y %T GMT')))
    ON DUPLICATE KEY UPDATE
        id                          = VALUES(id),
        board                       = VALUES(board),
        archive                     = VALUES(archive),
        last_modified_archive       = VALUES(last_modified_archive);
    "#
    }

    pub fn thread_get(board: &str) -> String {
        format!("SELECT * FROM `{board}` WHERE thread_num = ? AND subnum = 0 ORDER BY num;", board = board)
    }

    pub fn thread_get_media(board: &str) -> String {
        format!("SELECT *, FROM_BASE64(media_hash) AS md5 FROM `{board}` WHERE thread_num = ? AND subnum = 0 AND media_hash IS NOT NULL ORDER BY num;", board = board)
    }

    pub fn thread_get_last_modified(board: &str) -> String {
        // WHERE num=:thread
        // num is always unique for a single post in the `{board}` table so it always returns 1 row.
        format!(
            r#"
        SELECT COALESCE(op.last_modified, latest_post.last_modified) as last_modified FROM
        (
            SELECT
            DATE_FORMAT(FROM_UNIXTIME(`timestamp`), '%a, %d %b %Y %T GMT')
            AS last_modified
            FROM `{board}` WHERE num=:thread AND subnum=0
        ) latest_post
        LEFT JOIN
        (
            SELECT
            DATE_FORMAT(FROM_UNIXTIME(`time_last`), '%a, %d %b %Y %T GMT')
            AS last_modified
            FROM `{board}_threads` WHERE thread_num=:thread
        ) op
        ON true;
    "#,
            board = board
        )
    }

    // Unused
    pub fn thread_upsert<'a>(board: &str) -> String {
        // omit email, delpass
        format!(
            r#"
    INSERT INTO `{board}`
    (num, subnum, thread_num, op, `timestamp`, timestamp_expired, preview_orig, preview_w, preview_h,
    media_filename, media_w, media_h, media_size, media_hash, media_orig, spoiler,
    capcode, `name`, trip, title, comment, sticky, locked, poster_hash, poster_country, exif)
    VALUES
        (:num, :subnum, :thread_num, :op, :timestamp, :timestamp_expired, :preview_orig, :preview_w, :preview_h,
        :media_filename, :media_w, :media_h, :media_size, :media_hash, :media_orig, :spoiler,
        :capcode, :name, :trip, :title, :comment, :sticky, :locked, :poster_hash, :poster_country, :exif)
    ON DUPLICATE KEY UPDATE
        op=VALUES(op),
        `timestamp`=VALUES(`timestamp`),
        timestamp_expired=VALUES(timestamp_expired),
        preview_orig=VALUES(preview_orig),
        preview_w=VALUES(preview_w),
        preview_h=VALUES(preview_h),
        media_filename=COALESCE(VALUES(media_filename), media_filename),
        media_w=VALUES(media_w),
        media_h=VALUES(media_h),
        media_size=VALUES(media_size),
        media_hash=VALUES(media_hash),
        media_orig=VALUES(media_orig),
        spoiler=VALUES(spoiler),
        capcode=VALUES(capcode),
        `name`=VALUES(`name`),
        trip=VALUES(trip),
        title=VALUES(title),
        comment=VALUES(comment),
        sticky = (VALUES(sticky) OR sticky ),
        locked = (VALUES(locked) OR locked ),
        poster_hash=VALUES(poster_hash),
        poster_country=VALUES(poster_country),
        exif=VALUES(exif);
    "#,
            board = board
        )
    }

    /// Update a thread's `last_modified`.
    // The caller of this function only calls this when it's actually modified,
    // so we don't have to set a clause to update if things changed.
    pub fn thread_update_last_modified(board: &str) -> String {
        format!(
            r#"
        UPDATE `{board}_threads`
            SET time_last = UNIX_TIMESTAMP(STR_TO_DATE(?, '%a, %d %b %Y %T GMT'))
        WHERE thread_num=?;
    "#,
            board = board
        )
    }

    /// Mark a thread as deleted
    pub fn thread_update_deleted(board: &str) -> String {
        format!(
            r#"
        UPDATE `{board}`
        SET deleted             = 1,
            timestamp_expired   = ?
        WHERE
            op=1 AND thread_num = ? AND subnum = 0;
    "#,
            board = board
        )
    }
    // Unused
    pub fn thread_update_deleteds(board: &str) -> String {
        // unused
        format!(
            r#"
        UPDATE `{board}`
        SET deleted             = 1,
            timestamp_expired   = UNIX_TIMESTAMP()
        WHERE
            thread_num = ? AND num = ? AND subnum = 0
    "#,
            board = board
        )
    }

    /// combined, only run once at startup
    ///
    /// This is the MySQL variant.  
    /// It returns the cached `threads` or `archive` in the database for a specific board.
    ///
    /// ```sql
    ///     SELECT (CASE WHEN ? THEN threads ELSE archive END) as `threads`
    ///     FROM boards WHERE id=?;
    /// ```
    pub fn threads_get_combined<'a>() -> &'a str {
        r#"
        SELECT (CASE WHEN ? THEN threads ELSE archive END) as `threads` FROM boards WHERE id=?;
    "#
    }

    /// modified (on subsequent fetch. --watch-thread/--watch-board)
    /// on threads modified, deleted, archived?
    /// Only applies to threads. Archives call get_combined.
    pub fn threads_get_modified<'a>() -> &'a str {
        r#"
        SELECT threads FROM boards WHERE id=?;
    "#
    }

    pub fn post_get_single(board: &str) -> String {
        format!(
            r#"
        SELECT * from `{board}` WHERE thread_num=? AND num=? AND subnum = 0 LIMIT 1;
    "#,
            board = board
        )
    }

    pub fn post_get_media(board: &str) -> String {
        format!("SELECT * FROM `{board}_images` WHERE media_hash=?;", board = board)
    }
}
