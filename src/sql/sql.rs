// use fomat_macros::{epintln, fomat, pintln};
use serde::{Deserialize, Serialize};

// use futures::io::AsyncReadExt;
// use futures::stream::StreamExt;
// use futures::io::{AsyncReadExt, AsyncWriteExt};
use crate::config::{Board, Opt};
use async_trait::async_trait;
use futures::{future::Either, stream::Iter};
// use std::{collections::HashSet, fmt::Debug};
use color_eyre::eyre::{eyre, Result};
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
pub trait QueryExecutor {
    async fn boards_index_get_last_modified(&self) -> Option<String>;
    async fn boards_index_upsert(&self, json: &serde_json::Value, last_modified: &str) -> Result<u64>;
    async fn board_is_valid(&self, board: &str) -> bool;
    async fn board_upsert(&self, board: &str) -> Result<u64>;
    async fn board_table_exists(&self, board: &str, opt: &Opt) -> Option<String>;
    async fn board_get(&self, board: &str) -> Result<u16>;
    async fn board_get_last_modified(&self, thread_type: &str, board_id: u16) -> Option<String>;
    async fn board_upsert_threads(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64>;
    async fn board_upsert_archive(&self, board_id: u16, board: &str, json: &serde_json::Value, last_modified: &str) -> Result<u64>;

    async fn thread_get(&self, board_info: &Board, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>>;
    async fn thread_get_media(&self, board_info: &Board, thread: u64, start: u64) -> Either<Result<tokio_postgres::RowStream>, Result<Vec<mysql_async::Row>>>;
    async fn thread_get_last_modified(&self, board_id: u16, thread: u64) -> Option<String>;
    async fn thread_upsert(&self, board_info: &Board, thread_json: &serde_json::Value) -> u64;
    async fn thread_update_last_modified(&self, last_modified: &str, board_id: u16, thread: u64) -> Result<u64>;
    async fn thread_update_deleted(&self, board_id: u16, thread: u64) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>>;
    async fn thread_update_deleteds(&self, board_info: &Board, thread: u64, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>>;
    async fn threads_get_combined(&self, thread_type: &str, board_id: u16, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>>;
    async fn threads_get_modified(&self, board_id: u16, json: &serde_json::Value) -> Either<Result<tokio_postgres::RowStream>, Option<Iter<std::vec::IntoIter<u64>>>>;

    async fn post_get_single(&self, board_id: u16, thread: u64, no: u64) -> bool;
    async fn post_get_media(&self, board_info: &Board, md5: &str, hash_thumb: Option<&[u8]>) -> Either<Result<Option<tokio_postgres::Row>>, Option<mysql_async::Row>>;
    async fn post_upsert_media(&self, md5: &[u8], hash_full: Option<&[u8]>, hash_thumb: Option<&[u8]>) -> Result<u64>;

    async fn init_statements(&self, board_id: u16, board: &str);
}

// pub fn iter<I>(i: I) -> Row<impl RowTrait>
//  {
//         Ro
//     }
pub trait RowTrait {
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex,
        T: RowFrom<'a>;
}
pub trait RowIndex: tokio_postgres::row::RowIndex + mysql_common::row::ColumnIndex + std::fmt::Display {}
pub trait RowFrom<'a>: tokio_postgres::types::FromSql<'a> + mysql_async::prelude::FromValue {}

impl<'a> RowFrom<'a> for String {}
impl<'a> RowFrom<'a> for Option<Vec<u8>> {}
impl<'a> RowFrom<'a> for Option<String> {}
impl<'a> RowFrom<'a> for i64 {}

impl<'a> RowIndex for &'a str {}

impl RowTrait for tokio_postgres::Row {
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex,
        T: RowFrom<'a>, {
        Ok(self.try_get::<'a>(&idx)?)
    }
}

impl RowTrait for mysql_async::Row {
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex,
        T: RowFrom<'a>, {
        self.get(idx).ok_or_else(|| eyre!("Was an empty value"))
    }
}

use std::ops::Deref;

/// Row wrapper
pub struct Row<T: RowTrait>(T);

impl<T> Deref for Row<T>
where T: RowTrait
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

use std::iter::{Chain, Repeat, StepBy, Take};
/// Create an iterator that mimics the thread refresh system  
///
/// It repeats indefintely so `take` is required to limit how many `step_by`.  
/// If the stream has reached or passed its last value, it will keep repeating that last value.  
/// If an initial `refreshDelay` was set to `20`, `5` is added to each and subsequent requests
/// that return `NOT_MODIFIED`. If the next request is `OK`, the stream can be reset back to it's
/// initial value by calling `clone()` on it.
///
/// # Arguments
///
/// * `initial` - Initial value in seconds
/// * `step_by` - Add this much every time `next()` is called
/// * `take`    - Limit this to how many additions to make from `step_by`
///
/// # Example
///
/// ```
/// use ena::config;
/// let orig = config::refresh_rate(20, 5, 10);
/// let mut rate = orig.clone();
/// rate.next(); // 20
/// rate.next(); // 25
/// rate.next(); // 30
///
/// /* continued calls to rate.next(); */
/// rate.next(); // 75
/// rate.next(); // 75 .. repeating
///
/// rate = orig.clone();
/// rate.next(); // 20
/// ```
pub fn refresh_rate(initial: u32, step_by: usize, take: usize) -> Chain<Take<StepBy<std::ops::RangeFrom<u32>>>, Repeat<u32>> {
    let base = (initial..).step_by(step_by).take(take);
    let repeat = std::iter::repeat(base.clone().last().unwrap());
    let ratelimit = base.chain(repeat);
    ratelimit
}
