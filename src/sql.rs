//! SQL commons.
// #![cold]

use crate::{
    enums::{YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier},
    mysql
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use mysql_async::{prelude::*, Pool};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    ops::Add
};
/// This trait is used to define the implementation details archiving.  
/// The logic/algorithm in how you want to approach downloading everything.  
/// With that said, it should have more methods and stuff but I just use it as  
/// a way to pass generic implementations of `YotsubaArchiver` for different databases.
#[async_trait]
pub trait Archiver: Sync + Send {
    async fn run_inner(&self) -> Result<()>;
}

/// A struct to hold a generic implementation of `Archiver`  
/// See [this](https://is.gd/t3AHTt) chapter of the Rust Book on traits.
pub struct MuhArchiver(Box<dyn Archiver>);

impl MuhArchiver {
    pub fn new(x: Box<dyn Archiver>) -> Self {
        Self(x)
    }

    pub async fn run(&self) {
        if let Err(e) = self.0.run_inner().await {
            log::error!("{}", e);
        }
    }
}

pub type StatementStore<S> = HashMap<YotsubaIdentifier, S>;
#[async_trait]
pub trait StatementTrait<T>: Send + Sync {
    async fn prepare(&self, stmt: &str) -> T;
}

#[async_trait]
impl StatementTrait<tokio_postgres::Statement> for tokio_postgres::Client {
    async fn prepare(&self, stmt: &str) -> tokio_postgres::Statement {
        self.prepare(stmt).await.unwrap()
    }
}

#[async_trait]
impl StatementTrait<mysql::Statement> for Pool {
    async fn prepare(&self, stmt: &str) -> mysql::Statement {
        let conn = self.get_conn().await.unwrap();
        conn.prepare(stmt).await.unwrap()
    }
}

/// List of acceptable databases
#[derive(
    Debug,
    Copy,
    Clone,
    std::hash::Hash,
    PartialEq,
    std::cmp::Eq,
    enum_iterator::IntoEnumIterator,
    Deserialize,
    Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Database {
    PostgreSQL,
    TimescaleDB,
    MySQL,
    InnoDB,
    TokuDB
}

impl Database {
    pub fn base(&self) -> Database {
        match self {
            Database::PostgreSQL | Database::TimescaleDB => Database::PostgreSQL,
            _ => Database::MySQL
        }
    }

    pub fn mysql_engine(&self) -> Database {
        match self {
            Database::MySQL => Database::InnoDB,
            _ => *self
        }
    }
}

pub trait RowTrait {
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex,
        T: RowFrom<'a>;
}
pub trait RowIndex:
    tokio_postgres::row::RowIndex + mysql_common::row::ColumnIndex + std::fmt::Display {
}
pub trait RowFrom<'a>: tokio_postgres::types::FromSql<'a> + FromValue {}

impl<'a> RowFrom<'a> for String {}
impl<'a> RowFrom<'a> for Option<Vec<u8>> {}
impl<'a> RowFrom<'a> for Option<String> {}
impl<'a> RowFrom<'a> for i64 {}

impl<'a> RowIndex for &'a str {}

impl RowTrait for tokio_postgres::Row {
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex,
        T: RowFrom<'a> {
        Ok(self.try_get::<'a>(&idx)?)
    }
}
impl RowTrait for mysql_async::Row {
    fn get<'a, I, T>(&'a self, idx: I) -> Result<T>
    where
        I: RowIndex,
        T: RowFrom<'a> {
        Ok(self.get_opt(idx).ok_or_else(|| anyhow!("Was an empty value"))??)
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // Self::PostgreSQL | Self::TimescaleDB => write!(f, "postgresql"),
            d => write!(f, "{:?}", d)
        }
    }
}

/// Implement [`Into`].
/// Help taken from this [blog](https://is.gd/94QtP0)
/// [archive](http://archive.is/vIW5Y)
impl Into<Database> for String {
    fn into(self) -> Database {
        println!("Inside INTO {}", self);
        // Key point to note here is the `to_lowercase()`
        if let Some(found) = Database::into_enum_iter()
            .find(|db| db.to_string().to_lowercase() == self.to_lowercase())
        {
            found
        } else {
            let list = Database::into_enum_iter()
                .map(|zz| zz.to_string())
                .collect::<Vec<String>>()
                .join("`, `");
            panic!(format!("unknown variant `{}`, expected one of `{}`", self, list));
        }
    }
}

/// A list of actions that can be done.  
/// Basically an enum of `QueriesExecutor`.
#[derive(
    Debug, Copy, Clone, std::hash::Hash, PartialEq, std::cmp::Eq, enum_iterator::IntoEnumIterator,
)]
pub enum YotsubaStatement {
    UpdateMetadata = 1,
    UpdateThread,
    Delete,
    UpdateDeleteds,
    UpdateHashMedia,
    UpdateHashThumbs,
    Medias,
    Threads,
    ThreadsModified,
    ThreadsCombined,
    Metadata
}

impl YotsubaStatement {
    /// Return whether if thumbs or not
    pub fn is_thumbs(&self) -> bool {
        matches!(self, Self::UpdateHashThumbs)
    }

    /// Return the enum if thumb
    pub fn is_thumbs_val(&self) -> YotsubaStatement {
        match self {
            Self::UpdateHashThumbs => Self::UpdateHashThumbs,
            _ => Self::UpdateHashMedia
        }
    }

    pub fn is_media(&self) -> bool {
        matches!(self, Self::UpdateHashMedia)
    }
}

impl fmt::Display for YotsubaStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            s => write!(f, "{:?}", s)
        }
    }
}

impl Add for YotsubaStatement {
    type Output = u8;

    fn add(self, other: Self) -> u8 {
        (self as u8) + (other as u8)
    }
}

/// Executors for all SQL queries
#[async_trait]
pub trait QueriesExecutor<S, R> {
    async fn init_type(&self) -> Result<u64>;

    async fn init_schema(&self, schema: &str, engine: Database, charset: &str) -> Result<u64>;

    async fn init_metadata(&self, engine: Database, charset: &str) -> Result<u64>;

    async fn init_board(&self, board: YotsubaBoard, engine: Database, charset: &str)
    -> Result<u64>;

    async fn init_views(&self, board: YotsubaBoard) -> Result<u64>;

    async fn update_metadata(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<u64>;

    async fn update_thread(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<u64>;

    async fn delete(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u32
    ) -> Result<u64>;

    async fn update_deleteds(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        thread: u32, item: &[u8]
    ) -> Result<u64>;

    async fn update_hash(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    ) -> Result<u64>;

    async fn medias(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u32
    ) -> Result<Vec<R>>;

    async fn threads(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<VecDeque<u32>>;

    async fn threads_modified(
        &self, endpoint: YotsubaEndpoint, board: YotsubaBoard, new_threads: &[u8],
        statements: &StatementStore<S>
    ) -> Result<VecDeque<u32>>;

    async fn threads_combined(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        new_threads: &[u8]
    ) -> Result<VecDeque<u32>>;

    async fn metadata(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> Result<bool>;
}

/// List of all SQL queries to use
pub trait Queries {
    /// Create the schema if nonexistent and uses it as the search_path
    fn query_init_schema(&self, schema: &str, engine: Database, charset: &str) -> String;

    /// Create the metadata if nonexistent to store the api endpoints' data
    fn query_init_metadata(&self, engine: Database, charset: &str) -> String;

    /// Create a table for the specified board
    fn query_init_board(&self, board: YotsubaBoard, engine: Database, charset: &str) -> String;

    /// Create the 4chan schema as a type to be easily referenced
    fn query_init_type(&self) -> String;

    /// Create views for asagi
    fn query_init_views(&self, board: YotsubaBoard) -> String;

    /// Mark a post as deleted
    fn query_delete(&self, board: YotsubaBoard) -> String;

    /// Compare between the thread in db and the one fetched and marks any
    /// posts missing in the fetched thread as deleted
    fn query_update_deleteds(&self, board: YotsubaBoard) -> String;

    /// Upsert a media hash to a post
    fn query_update_hash(
        &self, board: YotsubaBoard, hash_type: YotsubaHash, media_mode: YotsubaStatement
    ) -> String;

    /// Upsert an endpoint to the metadata  
    ///
    /// Converts bytes to json object and feeds that into the query
    fn query_update_metadata(&self, column: YotsubaEndpoint) -> String;

    /// Check for the existence of an endpoint in the metadata
    fn query_metadata(&self, column: YotsubaEndpoint) -> String;

    /// Get a list of posts in a thread that have media
    fn query_medias(&self, board: YotsubaBoard, media_mode: YotsubaStatement) -> String;

    /// Get a list of only the deleted and modified threads when comparing the metadata
    /// and the fetched endpoint threads
    fn query_threads_modified(&self, endpoint: YotsubaEndpoint) -> String;

    /// Get a list of threads from the corresponding endpoint
    fn query_threads(&self) -> String;

    /// Get a list of threads from the one in the metadata + the fetched one
    fn query_threads_combined(&self, board: YotsubaBoard, endpoint: YotsubaEndpoint) -> String;

    /// Upsert a thread  
    ///
    /// This method updates an existing post or inserts a new one
    /// <sup>[1](https://stackoverflow.com/a/36406023) [2](https://dba.stackexchange.com/a/39821)</sup>.  
    /// Posts are only updated where there's a field change.  
    /// A majority of posts in a thread don't change, this minimizes I/O writes.  
    /// (sha256, sha256t, and deleted are handled seperately as they are special cases)  
    ///
    /// 4chan inserts a [backslash in their md5](https://stackoverflow.com/a/11449627).
    fn query_update_thread(&self, board: YotsubaBoard) -> String;
}

#[async_trait]
pub trait DatabaseTrait<T, R>:
    Queries + QueriesExecutor<T, R> + StatementTrait<T> + Send + Sync {
}

impl DatabaseTrait<tokio_postgres::Statement, tokio_postgres::Row> for tokio_postgres::Client {}
impl DatabaseTrait<mysql::Statement, mysql_async::Row> for Pool {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_thumbs_invalid() {
        let mut mode = YotsubaStatement::Medias;
        assert!(!mode.is_thumbs());
        mode = YotsubaStatement::Threads;
        assert!(!mode.is_thumbs());
        mode = YotsubaStatement::UpdateHashMedia;
        assert!(!mode.is_thumbs());
    }

    #[test]
    fn is_thumbs_valid() {
        let mode = YotsubaStatement::UpdateHashThumbs;
        assert!(mode.is_thumbs());
    }

    #[test]
    #[should_panic]
    fn test_panic() {
        panic!("aHHH");
    }
    #[test]
    #[should_panic(expected = "Divide result is zero")]
    fn test_panic2() {
        panic!("Divide result is zero");
    }

    #[test]
    fn update_metadata_unknown_json() {}

    #[test]
    fn update_metadata_deprecated_fields_json() {}

    #[test]
    fn update_metadata_added_fields_json() {}

    #[test]
    fn update_metadata_valid_json() {}

    #[test]
    fn update_thread_unknown_json() {}

    #[test]
    fn update_thread_deprecated_fields_json() {}

    #[test]
    fn update_thread_added_fields_json() {}

    #[test]
    fn update_thread_valid_json() {}

    #[test]
    fn delete_nonexistant_no() {}

    #[test]
    fn update_hash_unknown_hash() {}

    #[test]
    fn update_hash_improper_hash() {}

    #[test]
    fn update_hash_valid_json() {}

    #[test]
    fn get_metadata_missing() {}

    #[test]
    fn get_metadata_existing() {}

    #[test]
    fn get_medias_send_invalid_no() {}

    #[test]
    fn get_medias_send_valid_no() {}

    #[test]
    fn get_threads_send_unknown_json() {}

    #[test]
    fn get_threads_send_deprecated_fields_json() {}

    #[test]
    fn get_threads_send_added_fields_json() {}

    #[test]
    fn get_threads_send_valid_json() {}

    #[test]
    fn get_threads_modified_unknown_json() {}

    #[test]
    fn get_threads_modified_deprecated_fields_json() {}

    #[test]
    fn get_threads_modified_added_fields_json() {}

    #[test]
    fn get_threads_modified_valid_json() {}

    #[test]
    fn get_threads_combined_unknown_json() {}

    #[test]
    fn get_threads_combined_deprecated_fields_json() {}

    #[test]
    fn get_threads_combined_added_fields_json() {}

    #[test]
    fn get_threads_combined_valid_json() {}
}
