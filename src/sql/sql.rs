use anyhow::Result;
use async_trait::async_trait;
use futures::future::Either;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use strum_macros::EnumIter;

pub mod postgres;

#[path = "mysql/mysql.rs"]
pub mod mysql;
pub use crate::sql::{mysql::*, postgres::*};
use crate::{
    config::{Board, Opt},
    ThreadType,
};

#[derive(
    Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Hash, EnumIter, strum_macros::Display,
)]
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
    async fn boards_index_upsert(
        &self,
        json: &serde_json::Value,
        last_modified: &str,
    ) -> Result<u64>;
    async fn board_is_valid(&self, board: &str) -> Result<bool>;
    async fn board_upsert(&self, board: &str) -> Result<u64>;
    async fn board_table_exists(&self, board: &Board, opt: &Opt, db_name: &str) -> Option<String>;
    async fn board_get(&self, board: &str) -> Result<Option<u16>>;
    async fn board_get_last_modified(
        &self,
        thread_type: ThreadType,
        board: &Board,
    ) -> Option<String>;
    async fn board_upsert_threads(
        &self,
        board_id: u16,
        board: &str,
        json: &serde_json::Value,
        last_modified: &str,
    ) -> Result<u64>;
    async fn board_upsert_archive(
        &self,
        board_id: u16,
        board: &str,
        json: &serde_json::Value,
        last_modified: &str,
    ) -> Result<u64>;

    async fn thread_get(
        &self,
        board: &Board,
        thread: u64,
    ) -> Result<Either<tokio_postgres::RowStream, Vec<mysql_async::Row>>>;
    async fn thread_get_media(
        &self,
        board: &Board,
        thread: u64,
        start: u64,
    ) -> Result<Either<tokio_postgres::RowStream, Vec<mysql_async::Row>>>;
    async fn thread_get_last_modified(&self, board_id: u16, thread: u64) -> Option<String>;
    async fn thread_upsert(&self, board: &Board, thread_json: &serde_json::Value) -> Result<u64>;
    async fn thread_update_last_modified(
        &self,
        last_modified: &str,
        board_id: u16,
        thread: u64,
    ) -> Result<u64>;
    async fn thread_update_deleted(
        &self,
        board: &Board,
        thread: u64,
    ) -> Result<Either<tokio_postgres::RowStream, Option<u64>>>;
    async fn thread_update_deleteds(
        &self,
        board: &Board,
        thread: u64,
        json: &serde_json::Value,
    ) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>>;
    async fn threads_get_combined(
        &self,
        thread_type: ThreadType,
        board_id: u16,
        json: &serde_json::Value,
    ) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>>;
    async fn threads_get_modified(
        &self,
        board_id: u16,
        json: &serde_json::Value,
    ) -> Result<Either<tokio_postgres::RowStream, Option<Vec<u64>>>>;

    async fn post_get_single(&self, board_id: u16, thread: u64, no: u64) -> Result<bool>;
    async fn post_get_media(
        &self,
        board: &Board,
        md5: &str,
        hash_thumb: Option<&[u8]>,
    ) -> Result<Either<Option<tokio_postgres::Row>, Option<mysql_async::Row>>>;
    async fn post_upsert_media(
        &self,
        md5: &[u8],
        hash_full: Option<&[u8]>,
        hash_thumb: Option<&[u8]>,
    ) -> Result<u64>;

    async fn init_statements(&self, board_id: u16, board: &str) -> Result<()>;
}

/*
use sqlx::Row as SqlxRow;
pub trait RowIndex<'a>: tokio_postgres::row::RowIndex + std::fmt::Display + sqlx::row::ColumnIndex<'a, sqlx::mysql::MySqlRow<'a>> {}
pub trait RowFrom<'a>: tokio_postgres::types::FromSql<'a> + sqlx::Type<sqlx::MySql> + sqlx::decode::Decode<'a, sqlx::MySql>  {}

pub trait Row {
    /// Gets a value from the row
    fn get<'a, A, B>(&'a self, idx: A) -> Result<B>
    where
        A: RowIndex<'a>,
        B: RowFrom<'a>;
}
impl Row for tokio_postgres::Row {
    /// Deserializes a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// Like [`RowGet`], but returns a `Result` rather than panicking.
    ///
    /// [`RowGet`]: tokio_postgres::Row::get
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex<'a>,
        T: RowFrom<'a> {
        Ok(self.try_get::<'a>(idx)?)
    }
}

impl<'c> Row for sqlx::mysql::MySqlRow<'c> {
    /// Index into the database row and decode a single value.
    ///
    /// A string index can be used to access a column by name and a `usize` index
    /// can be used to access a column by position.
    ///
    /// ```rust,ignore
    /// # let mut cursor = sqlx::query("SELECT id, name FROM users")
    /// #     .fetch(&mut conn);
    /// #
    /// # let row = cursor.next().await?.unwrap();
    /// #
    /// let id: i32 = row.get("id")?; // a column named "id"
    /// let name: &str = row.get(1)?; // the second column in the result
    /// ```
    ///
    /// # Errors
    ///  * [`ColumnNotFound`] if the column by the given name was not found.
    ///  * [`ColumnIndexOutOfBounds`] if the `usize` index was greater than the number of columns in the row.
    ///  * [`Decode`] if the value could not be decoded into the requested type.
    ///
    /// [`Decode`]: sqlx::Error::Decode
    /// [`ColumnNotFound`]: sqlx::Error::ColumnNotFound
    /// [`ColumnIndexOutOfBounds`]: sqlx::Error::ColumnIndexOutOfBounds
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex<'a>,
        T: RowFrom<'a> {

        Ok(self.try_get::<T,I>(idx)?)
    }
}
*/
