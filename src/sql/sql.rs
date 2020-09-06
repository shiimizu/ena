// use fomat_macros::{epintln, fomat, pintln};
use anyhow::Result;
use serde::{Deserialize, Serialize};

// use futures::io::AsyncReadExt;
// use futures::stream::StreamExt;
// use futures::io::{AsyncReadExt, AsyncWriteExt};
use crate::config::{Board, Opt};
use async_trait::async_trait;
use futures::future::Either;
// use std::{collections::HashSet, fmt::Debug};
use crate::ThreadType;
use std::fmt::Debug;
use strum_macros::EnumIter;

pub mod postgres;

#[path = "mysql/mysql.rs"]
pub mod mysql;
pub use crate::sql::{mysql::*, postgres::*};

/*
-- Query normally with added board name

select board.board, post.resto, post.no, post.deleted_on, post.archived_on, post.com from
(select * from "posts" where archived_on is not null order by COALESCE(last_modified, time))post
left join lateral
(select * from boards) board
on post.board = board.id
where board.board='po'
order by post.no;
*/

/*
SET TIMEZONE='America/New_York';
use time;
let ts = time::OffsetDateTime::now_utc().timestamp();
let ts_repr = time::OffsetDateTime::from_unix_timestamp(ts).lazy_format("%a, %d %b %Y %H:%M:%S GMT").to_string();

SELECT board,no,time,last_modified, to_char(to_timestamp("time") AT TIME ZONE 'UTC', 'Dy, DD Mon YYYY HH24:MI:SS GMT') as "now_rfc2822" from posts where no=128031655 and resto=0 limit 10;

SELECT to_char(to_timestamp(max("time")) AT TIME ZONE 'UTC', 'Dy, DD Mon YYYY HH24:MI:SS GMT') as "now_rfc2822" from posts where board=1 and no=2702373574 or resto=2707237354;

select * from
(select board, resto, no, replies, "time" from posts where board=1 and resto=0 and replies=0 order by "time" desc limit 10)op
LEFT JOIN LATERAL
(select max("time") as max_time from posts where no=op.no or resto=op.no )o1
on true;

*/

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Hash, EnumIter, strum_macros::Display)]
pub enum Query {
    BoardsIndexGetLastModified,
    BoardsIndexUpsert,
    BoardIsValid,
    BoardUpsert,
    BoardTableExists,
    BoardGet,
    BoardGetLastModified,
    BoardUpsertThreads,
    BoardUpsertArchive,
    ThreadGet,
    ThreadGetMedia,
    ThreadGetLastModified,
    ThreadUpsert,
    ThreadUpdateLastModified,
    ThreadUpdateDeleted,
    ThreadUpdateDeleteds,
    ThreadsGetCombined,
    ThreadsGetModified,
    PostGetSingle,
    PostGetMedia,
    PostUpsertMedia,
}

#[async_trait]
pub trait DropExecutor {
    async fn disconnect_pool(self) -> Result<()>;
}
#[async_trait]
pub trait QueryExecutor {
    async fn boards_index_get_last_modified(&self) -> Result<Option<String>>;
    async fn boards_index_upsert(&self, json: &serde_json::Value, last_modified: &str) -> Result<u64>;
    async fn board_is_valid(&self, board: &str) -> Result<bool>;
    async fn board_upsert(&self, board: &str) -> Result<u64>;
    async fn board_table_exists(&self, board: &str, opt: &Opt) -> Option<String>;
    async fn board_get(&self, board: &str) -> Result<Option<u16>>;
    async fn board_get_last_modified(&self, thread_type: ThreadType, board_info: &Board) -> Option<String>;
    async fn board_upsert_threads(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64>;
    async fn board_upsert_archive(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64>;

    async fn thread_get(&self, board_info: &Board, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>>;
    async fn thread_get_media(&self, board_info: &Board, thread: u64, start: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>>;
    async fn thread_get_last_modified(&self, board_id: u16, thread: u64) -> Option<String>;
    async fn thread_upsert(&self, board_info: &Board, thread_json: &serde_json::Value) -> Result<u64>;
    async fn thread_update_last_modified(&self, last_modified: &str, board_id: u16, thread: u64) -> Result<u64>;
    async fn thread_update_deleted(&self, board_id: u16, thread: u64) -> Result<Either<tokio_postgres::RowStream, Option<u64>>>;
    async fn thread_update_deleteds(&self, board_info: &Board, thread: u64, json: &serde_json::Value) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>>;
    async fn threads_get_combined(&self, thread_type: ThreadType, board_id: u16, json: &serde_json::Value) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>>;
    async fn threads_get_modified(&self, board_id: u16, json: &serde_json::Value) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>>;

    async fn post_get_single(&self, board_id: u16, thread: u64, no: u64) -> Result<bool>;
    async fn post_get_media(&self, board_info: &Board, md5: &str, hash_thumb: Option<&[u8]>) -> Either<Result<Option<tokio_postgres::Row>>, Result<Option<mysql_async::Row>>>;
    async fn post_upsert_media(&self, md5: &[u8], hash_full: Option<&[u8]>, hash_thumb: Option<&[u8]>) -> Result<u64>;

    async fn init_statements(&self, board_id: u16, board: &str) -> Result<()>;
}
