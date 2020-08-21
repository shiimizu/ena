#![allow(unused_imports)]
use super::{Query, QueryExecutor};
use crate::{
    config::{Board, Opt},
    yotsuba, ThreadType,
};
use async_rwlock::RwLock;
use async_trait::async_trait;
use color_eyre::eyre::{eyre, Result};
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
use asagi::*;
pub use queries::*;

/// List of prepared statments to query
pub type BoardId = u16;
pub type StatementStore = HashMap<BoardId, HashMap<super::Query, mysql_async::Statement>>;

/// List of computged prepared statments
static STATEMENTS: Lazy<RwLock<StatementStore>> = Lazy::new(|| RwLock::new(HashMap::new()));

fn get_sql_template(board: &str, engine: &str, charset: &str, collate: &str, query: &str) -> String {
    // let s = std::fs::read_to_string(path).unwrap();
    query.replace("%%ENGINE%%", engine).replace("%%BOARD%%", board).replace("%%CHARSET%%", charset).replace("%%COLLATE%%", collate)
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

/*
#[async_trait]
trait StatementExt {
    async fn get_statement<'a>(&self, query: Query) -> &'a mysql_async::Statement;
}

#[async_trait]
impl StatementExt for RwLock<mysql_async::Conn> {

async fn get_statement<'a>(&self, query: Query) -> &'a mysql_async::Statement {
    let mut conn = self.write().await;
    let store = STATEMENTS.read().await;
    // BoardId of boards.json entry in db is always 1
    let map = (*store).get(&1).unwrap();
    let statement = map.get(&query).unwrap();
    statement
}
}
*/

#[async_trait]
impl QueryExecutor for RwLock<mysql_async::Conn> {
    async fn boards_index_get_last_modified(&self) -> Option<String> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        // BoardId of boards.json entry in db is always 1
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardsIndexGetLastModified).unwrap();
        let res: Result<Option<Option<String>>, mysql_async::Error> = conn.exec_first(statement, ()).await;
        res.ok().flatten().flatten()
    }

    async fn boards_index_upsert(&self, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        // BoardId of boards.json entry in db is always 1
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardsIndexUpsert).unwrap();
        let json = serde_json::to_string(json).unwrap();
        let res = conn.exec_drop(statement, (json, last_modified)).await;
        res.map(|_| 0).map_err(|e| eyre!(e))
    }

    async fn board_is_valid(&self, board: &str) -> bool {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        // BoardId of boards.json entry in db is always 1
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardIsValid).unwrap();
        let res: Result<Option<Option<String>>, mysql_async::Error> = conn.exec_first(statement, ()).await;
        let res = res.ok().flatten().flatten();
        if let Some(res) = res {
            let j: serde_json::Value = serde_json::from_str(&res).unwrap();
            let boards = j["boards"].as_array().unwrap();
            let b = boards.iter().any(|v| matches!(v["board"].as_str(), Some(board)));
            b
        } else {
            false
        }
    }

    async fn board_upsert(&self, board: &str) -> Result<u64> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardUpsert).unwrap();
        let res = conn.exec_drop(statement, params! { "board" => board }).await;
        res.map(|_| 0).map_err(|e| eyre!(e))
    }

    async fn board_table_exists(&self, board: &str, opt: &Opt) -> Option<String> {
        let mut conn = self.write().await;

        // Init the `boards` table
        if board == "boards" {
            // pintln!("board_table_exists: Creating `boards` table");
            let boards_table = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/boards_table.sql"));
            let common = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/common.sql"));
            conn.query_drop(&boards_table).await.unwrap();
            conn.query_drop(&common).await.unwrap();
            // No one checks the result of this function anyways so it's okay to return anything
            return None;
        }

        let store = STATEMENTS.read().await;
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardTableExists).unwrap();
        let res: Result<Option<Option<String>>, _> = conn.exec_first(statement, (&opt.database.name, board)).await;
        let res = res.ok().flatten().flatten();
        if res.is_none() {
            // let boards = get_sql_template(board, &opt.database.engine, &opt.database.charset,
            // &opt.database.collate, include_str!("templates/boards.sql"));
            let boards = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/boards.sql"));
            let triggers = get_sql_template(board, &opt.database.engine, &opt.database.charset, &opt.database.collate, include_str!("templates/triggers.sql"));
            let query = [boards, triggers].concat();
            pintln!("Creating tables for /"(board)"/");
            conn.query_drop(query).await.unwrap();
        }
        res
    }

    async fn board_get(&self, board: &str) -> Result<u16> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardGet).unwrap();
        let res: Result<Option<mysql_async::Row>> = conn.exec_first(statement, (board,)).await.map_err(|e| eyre!(e));
        res.map(|opt| opt.unwrap().get("id").unwrap())
    }

    async fn board_get_last_modified(&self, thread_type: ThreadType, board_id: u16) -> Option<String> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardGetLastModified).unwrap();
        let res: Result<Option<Option<String>>, _> = conn.exec_first(statement, (thread_type.as_str(), board_id)).await;
        res.ok().flatten().flatten()
    }

    async fn board_upsert_threads(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardUpsertThreads).unwrap();
        let json = serde_json::to_string(json).unwrap();
        let res: Result<Option<mysql_async::Row>> = conn.exec_first(statement, (board_id, board, json, last_modified)).await.map_err(|e| eyre!(e));
        Ok(0)
    }

    async fn board_upsert_archive(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&1).unwrap();
        let statement = map.get(&Query::BoardUpsertArchive).unwrap();
        let json = serde_json::to_string(json).unwrap();
        let res: Result<Option<mysql_async::Row>> = conn.exec_first(statement, (board_id, board, json, last_modified)).await.map_err(|e| eyre!(e));
        Ok(0)
    }

    async fn thread_get(&self, board_info: &Board, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_info.id).unwrap();
        let statement = map.get(&Query::ThreadGet).unwrap();
        let res: Result<Vec<mysql_async::Row>> = conn.exec(statement, (thread,)).await.map_err(|e| eyre!(e));
        Either::Right(res)
    }

    /// Unused
    async fn thread_get_media(&self, board_info: &Board, thread: u64, start: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_info.id).unwrap();
        let statement = map.get(&Query::ThreadGet).unwrap();
        let res: Result<Vec<mysql_async::Row>> = conn.exec(statement, (thread,)).await.map_err(|e| eyre!(e));
        Either::Right(res)
    }

    async fn thread_get_last_modified(&self, board_id: u16, thread: u64) -> Option<String> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_id).unwrap();
        let statement = map.get(&Query::ThreadGetLastModified).unwrap();
        let res: Result<Option<Option<String>>, mysql_async::Error> = conn.exec_first(statement, params! {"thread" => thread}).await;
        res.ok().flatten().flatten()
    }

    async fn thread_upsert(&self, board_info: &Board, thread_json: &serde_json::Value) -> u64 {
        let posts: Vec<yotsuba::Post> = serde_json::from_value(thread_json["posts"].clone()).unwrap();
        if posts.len() == 0 {
            return 0;
        }
        // TODO If you want UPSERTED count, you have to convert 4chan post into Asagi, then compare that
        // with the fetched from the db Mapping those Post structs might be inneficent?

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

        let mut conn = self.write().await;
        // TODO: Return rows affected
        conn.query_drop(q.as_str()).await.unwrap();
        0
    }

    async fn thread_update_last_modified(&self, last_modified: &str, board_id: u16, thread: u64) -> Result<u64> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_id).unwrap();
        let statement = map.get(&Query::ThreadUpdateLastModified).unwrap();
        let res = conn.exec_drop(statement, (last_modified, thread)).await;
        res.map(|_| 0).map_err(|e| eyre!(e))
    }

    async fn thread_update_deleted(&self, board_id: u16, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        // TODO the return of this function should just be u64 or bool
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_id).unwrap();
        let statement = map.get(&Query::ThreadUpdateDeleted).unwrap();

        let get_single_statement = map.get(&Query::PostGetSingle).unwrap();

        let res = conn.exec_drop(statement, (thread,)).await.unwrap();
        let mut single_res: Option<mysql_async::Row> = conn.exec_first(get_single_statement, (thread, thread)).await.ok().flatten();

        // Get the post afterwards to see if it was deleted
        if let Some(mut row) = single_res {
            let del = row.get::<Option<u8>, &str>("deleted").flatten();
            if let Some(_del) = del {
                if _del == 1 {
                    Either::Right(Some(futures::stream::iter(vec![thread])))
                } else {
                    Either::Right(None)
                }
            } else {
                Either::Right(None)
            }
        } else {
            Either::Right(None)
        }
    }

    async fn thread_update_deleteds(&self, board_info: &Board, thread: u64, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        // let posts: Vec<yotsuba::Post> = serde_json::from_value(json["posts"].clone()).unwrap();
        let posts = json["posts"].as_array().unwrap();

        // TODO this should never happen. Return an error
        if posts.len() == 0 {
            return Either::Right(None);
        }
        // let posts_iter = posts.iter().filter(|post| post.time != 0 && post.no != 0);
        // let posts = json["posts"].as_array().unwrap();
        // let no = posts[if posts.len() == 1 { 0 } else { 1 }].no;
        let start = posts[if posts.len() == 1 { 0 } else { 1 }]["no"].as_u64().unwrap_or_default();
        // let start = if let Some(post_json) = json["posts"].get(1) {
        //     post_json["no"].as_u64().unwrap()
        // } else {
        //     json["posts"][0]["no"].as_u64().unwrap()
        // };
        let mut conn = self.write().await;

        // Select all posts from DB starting from new json post's first reply `no` (default to OP if no
        // replies).
        let query = format!("select * from `{board}` where thread_num = {thread} and num >= {start} and subnum = 0;", board = &board_info.board, start = start, thread = thread);
        let res: Result<Vec<mysql_async::Row>, _> = conn.query(query.as_str()).await;

        match res {
            Ok(rows) => {
                // let posts: HashSet<yotsuba::Post> = serde_json::from_value(json["posts"].clone()).unwrap();

                // latest posts (v_latest) can include OP post since we're taking the difference. should not filter
                // it out (diff = values in self, but not in other).
                let v_latest = posts.iter().map(|v| v.get("no").unwrap().as_i64().unwrap() as u64).collect();
                // let v_latest :HashSet<u64>= posts.iter().filter(|post| post.time != 0 && post.no !=
                // 0).map(|post|post.no).collect();
                let v_previous: HashSet<u64> = rows.into_iter().map(|row| row.get("num")).map(|v: Option<u64>| v.unwrap()).collect();
                let diff: Vec<u64> = v_previous.difference(&v_latest).map(|&v| v).unique().collect();
                if diff.len() > 0 {
                    // let tmp = json["posts"].as_array().unwrap().iter().map(|v| v.get("no").unwrap().as_i64().unwrap()
                    // as u64).collect::<Vec<_>>(); pintln!("\ntmp: " [tmp]"\nv_previous: "
                    // [v_previous] "\ndiff: "[diff]);

                    let mut list_no_iter = diff.iter().enumerate();

                    // Manually stitch the query together so we can have multiple values
                    let query = fomat!(
                    r#"
                INSERT INTO `"# (&board_info.board) r#"`
                (num, subnum, deleted, `timestamp`, `timestamp_expired`)
                VALUES
                    "#
                    for (i, no) in list_no_iter {
                        "\n(" (no) ", 0, 1, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())"
                        if i < diff.len()-1 { "," } else { "" }
                    }
                    r#"
                ON DUPLICATE KEY UPDATE
                deleted=1,
                timestamp_expired=UNIX_TIMESTAMP();"#
                    );
                    conn.query_drop(query.as_str()).await.unwrap();

                    Either::Right(Some(futures::stream::iter(diff)))
                } else {
                    Either::Right(None)
                }
            }
            Err(e) => {
                epintln!("thread_update_deleteds:"(e));
                Either::Right(None)
            }
        }
    }

    async fn threads_get_combined(&self, thread_type: ThreadType, board_id: u16, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_id).unwrap();
        let statement = map.get(&Query::ThreadsGetCombined).unwrap();
        let res: Result<Option<Option<String>>, _> = conn.exec_first(statement, (thread_type.as_str(), board_id)).await;
        let res = res.ok().flatten().flatten();
        if let Some(res) = res {
            // If an entry exists in the database we can apply a diff
            if thread_type.is_threads() {
                let mut prev: Vec<Page> = serde_json::from_str(&res).unwrap();
                let mut new: Vec<Page> = serde_json::from_value(json.clone()).unwrap();
                let a: HashSet<_> = prev.into_iter().flat_map(|page| page.threads).collect();
                let b: HashSet<_> = new.into_iter().flat_map(|page| page.threads).collect();
                let diff: Vec<_> = a.union(&b).map(|thread| thread.no).unique().collect();
                // pintln!([a]"-----------\n"[diff]"-----------\n"[b]);
                Either::Right(Some(futures::stream::iter(diff)))
            } else {
                let mut prev: Vec<u64> = serde_json::from_str(&res).unwrap();
                let mut new: Vec<u64> = serde_json::from_value(json.clone()).unwrap();
                let diff: Vec<u64> = prev.into_iter().chain(new.into_iter()).unique().collect();
                Either::Right(Some(futures::stream::iter(diff)))
            }
        } else {
            // If an entry does not exist in the database just use the json received
            // This usually occurs on startup when there's no cache of threads/archive for the board
            if thread_type.is_threads() {
                let mut received: Vec<Page> = serde_json::from_value(json.clone()).unwrap();
                let res: Vec<u64> = received.into_iter().flat_map(|page| page.threads.into_iter().map(|thread| thread.no)).unique().collect();
                Either::Right(Some(futures::stream::iter(res)))
            } else {
                let mut res: Vec<u64> = serde_json::from_value(json.clone()).unwrap();
                Either::Right(Some(futures::stream::iter(res)))
            }
        }

        /*let diff = previous_json
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|page| page["threads"].as_array().unwrap().iter().map(|thread| thread["no"].as_u64().unwrap()))
            .chain(json.as_array().unwrap().iter().flat_map(|page| page["threads"].as_array().unwrap().iter().map(|thread| thread["no"].as_u64().unwrap()))).unique().collect::<Vec<_>>();
        Either::Right(futures::stream::iter(diff))*/
    }

    async fn threads_get_modified(&self, board_id: u16, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        let map = (*store).get(&board_id).unwrap();
        let statement = map.get(&Query::ThreadsGetModified).unwrap();
        let res: Result<Option<Option<String>>, _> = conn.exec_first(statement, (board_id,)).await;
        let res = res.ok().flatten().flatten();
        if let Some(res) = res {
            let mut prev: Vec<Page> = serde_json::from_str(&res).unwrap();
            let mut new: Vec<Page> = serde_json::from_value(json.clone()).unwrap();
            let a: HashSet<_> = prev.into_iter().flat_map(|page| page.threads).collect();
            let b: HashSet<_> = new.into_iter().flat_map(|page| page.threads).collect();
            let diff: Vec<_> = a.symmetric_difference(&b).map(|thread| thread.no).unique().collect();
            Either::Right(Some(futures::stream::iter(diff)))
        } else {
            Either::Right(None)
        }
    }

    async fn post_get_single(&self, board_id: u16, thread: u64, no: u64) -> bool {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        // BoardId of boards.json entry in db is always 1
        let map = (*store).get(&board_id).unwrap();
        let statement = map.get(&Query::PostGetSingle).unwrap();
        let res: Result<Option<mysql_async::Row>, _> = conn.exec_first(statement, (thread, no)).await;
        res.ok()
            .flatten()
            .map(|row| {
                let _tmp: Option<u64> = row.get("num");
                _tmp
            })
            .flatten()
            .map_or_else(|| false, |_| true)
    }

    async fn post_get_media(&self, board_info: &Board, md5: &str, hash_thumb: Option<&[u8]>) -> Either<Result<Option<tokio_postgres::Row>>, Option<mysql_async::Row>> {
        let mut conn = self.write().await;
        let store = STATEMENTS.read().await;
        // BoardId of boards.json entry in db is always 1
        let map = (*store).get(&board_info.id).unwrap();
        let statement = map.get(&Query::PostGetMedia).unwrap();
        let mut res: Option<mysql_async::Row> = conn.exec_first(statement, (md5,)).await.ok().flatten();
        Either::Right(res)
    }

    async fn post_upsert_media(&self, md5: &[u8], hash_full: Option<&[u8]>, hash_thumb: Option<&[u8]>) -> Result<u64> {
        unreachable!()
    }

    async fn init_statements(&self, board_id: u16, board: &str) {
        if STATEMENTS.read().await.contains_key(&board_id) {
            return;
        }

        let mut conn = self.write().await;
        let mut map = HashMap::new();
        let actual = if board_id == 1 { super::Query::iter().filter(|q: &Query| q.to_string().starts_with("Board")).collect::<Vec<Query>>() } else { super::Query::iter().collect::<Vec<Query>>() };
        for query in actual {
            let statement = match query {
                Query::BoardsIndexGetLastModified => conn.prep(boards_index_get_last_modified()),
                Query::BoardsIndexUpsert => conn.prep(boards_index_upsert()),
                Query::BoardIsValid => conn.prep(board_is_valid()),
                Query::BoardUpsert => conn.prep(board_upsert()),
                Query::BoardTableExists => conn.prep(board_table_exists()),
                Query::BoardGet => conn.prep(board_get()),
                Query::BoardGetLastModified => conn.prep(board_get_last_modified()),
                Query::BoardUpsertThreads => conn.prep(board_upsert_threads()),
                Query::BoardUpsertArchive => conn.prep(board_upsert_archive()),
                Query::ThreadGet => conn.prep(thread_get(board)),
                Query::ThreadGetMedia => conn.prep(thread_get_media(board)),
                Query::ThreadGetLastModified => conn.prep(thread_get_last_modified(board)),
                Query::ThreadUpsert => conn.prep(thread_upsert(board)),
                Query::ThreadUpdateLastModified => conn.prep(thread_update_last_modified(board)),
                Query::ThreadUpdateDeleted => conn.prep(thread_update_deleted(board)),
                Query::ThreadUpdateDeleteds => conn.prep(thread_update_deleteds(board)),
                Query::ThreadsGetCombined => conn.prep(threads_get_combined()),
                Query::ThreadsGetModified => conn.prep(threads_get_modified()),
                Query::PostGetSingle => conn.prep(post_get_single(board)),
                Query::PostGetMedia => conn.prep(post_get_media(board)),
                Query::PostUpsertMedia => conn.prep("SELECT 1;"),
            }
            .await
            .unwrap();
            map.insert(query, statement);
            map.remove(&Query::PostUpsertMedia);
        }
        STATEMENTS.write().await.insert(board_id, map);
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
        (CASE WHEN ?='threads' THEN last_modified_threads ELSE last_modified_archive END)
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
            timestamp_expired   = UNIX_TIMESTAMP()
        WHERE
            op=1 AND thread_num = ? AND subnum = 0;
    "#,
            board = board
        )
    }

    /// Mark down any post found in the thread that was deleted
    ///
    /// ```sql
    ///     UPDATE `{board}`
    ///     SET deleted             = 1,
    ///         timestamp_expired   = UNIX_TIMESTAMP()
    ///     WHERE
    ///         thread_num = ? AND num = ? AND subnum = 0
    /// ```
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
    ///     SELECT (CASE WHEN ?='threads' THEN threads ELSE archive END) as `threads`
    ///     FROM boards WHERE id=?;
    /// ```
    pub fn threads_get_combined<'a>() -> &'a str {
        r#"
        SELECT (CASE WHEN ?='threads' THEN threads ELSE archive END) as `threads` FROM boards WHERE id=?;
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
