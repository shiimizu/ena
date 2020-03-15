// #![cold]

use crate::{YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier};
use ::mysql::{prelude::*, *};
use anyhow::Result;
use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    ops::Add
};

pub type StatementStore2<S> = HashMap<YotsubaIdentifier, S>;
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
impl StatementTrait<::mysql::Statement> for ::mysql::Pool {
    async fn prepare(&self, stmt: &str) -> ::mysql::Statement {
        let mut conn = self.get_conn().unwrap();
        conn.prep(stmt).unwrap()
    }
}

/*#[derive(Debug, Copy, Clone)]
pub struct MuhStatement<T: StatementTrait>(pub T);

impl<T> MuhStatement<T>
where T: StatementTrait
{
    pub fn new(x: T) -> Self {
        Self(x)
    }

    pub fn get(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::Deref for MuhStatement<T>
where T: StatementTrait
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for MuhStatement<T>
where T: StatementTrait
{
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> AsRef<T> for MuhStatement<T>
where T: StatementTrait
{
    fn as_ref(&self) -> &T {
        &self.0
    }
}*/

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
}
pub enum Rows {
    PostgreSQL(Vec<tokio_postgres::row::Row>),
    MySQL(Vec<mysql::Row>)
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // Self::PostgreSQL | Self::TimescaleDB => write!(f, "postgresql"),
            d => write!(f, "{:?}", d)
        }
    }
}

/// Implement `into`.
/// Help taken from this [blog](https://is.gd/94QtP0)
/// [archive](http://archive.is/vIW5Y)
/*impl Into<String> for Database {
    fn into(self) -> String {
        self.to_string()
    }
}*/

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

// impl From<Database> for String {
//     fn from(d: Database) -> String {
//     println!("INSIDE  FROM TRAIT");
//         d.to_string().to_lowercase()
//         // if let Some(found) =
//         //     Database::into_enum_iter().find(|db| db.to_string() == d.to_lowercase())
//         // {
//         //     found
//         // } else {
//         //     Database::PostgreSQL
//         // }
//     }
// }

/// A list of actions that can be done.  
/// Basically an enum of `QueriesExecutor2`.
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
pub trait QueriesExecutor2<S> {
    /// Creates the schema if nonexistent and uses it as the search_path
    async fn init_schema_new(&self, schema: &str);

    /// Creates the 4chan schema as a type to be easily referenced
    async fn init_type_new(&self);

    /// Creates the metadata if nonexistent to store the api endpoints' data
    async fn init_metadata_new(&self);

    /// Creates a table for the specified board
    async fn init_board_new(&self, board: YotsubaBoard);

    /// Creates views for asagi
    async fn init_views_new(&self, board: YotsubaBoard);

    /// Upserts an endpoint to the metadata
    ///
    /// Converts bytes to json object and feeds that into the query
    async fn update_metadata_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<u64>;

    /// Upserts a thread
    ///
    /// This method updates an existing post or inserts a new one.
    /// Posts are only updated where there's a field change.
    /// A majority of posts in a thread don't change, this minimizes I/O writes.
    /// <T>(sha256, sha25t, and deleted are handled seperately as they are special
    /// cases) https://stackoverflow.com/a/36406023
    /// https://dba.stackexchange.com/a/39821
    ///
    /// 4chan inserts a backslash in their md5.
    /// https://stackoverflow.com/a/11449627
    async fn update_thread_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<u64>;

    /// Marks a post as deleted
    async fn delete_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u32
    );

    // deleted before updating. PgSQL needs to do this >_>..
    /// Compares between the thread in db and the one fetched and marks any
    /// posts missing in the fetched thread as deleted
    async fn update_deleteds_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        thread: u32, item: &[u8]
    ) -> Result<u64>;

    /// Upserts a media hash to a post
    async fn update_hash_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    );

    /// Gets the list of posts in a thread that have media
    async fn medias_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u32
    ) -> Result<Rows>;

    /// Gets a list of threads from the corresponding endpoint
    async fn threads_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<VecDeque<u32>>;

    /// Gets only the deleted and modified threads when comparing the metadata
    /// and the fetched endpoint
    async fn threads_modified_new(
        &self, board: YotsubaBoard, new_threads: &[u8], statement: &S
    ) -> Result<VecDeque<u32>>;

    /// Gets the list of threads from the one in the metadata + the fetched one
    async fn threads_combined_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        new_threads: &[u8]
    ) -> Result<VecDeque<u32>>;

    /// Checks for the existence of an endpoint in the metadata
    async fn metadata_new(
        &self, statements: &StatementStore2<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> bool;
}

/// List of all SQL queries to use
pub trait Queries2 {
    fn query_init_schema(&self, schema: &str) -> String;
    fn query_init_metadata(&self) -> String;
    fn query_init_type(&self) -> String;
    fn query_init_views(&self, board: YotsubaBoard) -> String;
    fn query_delete(&self, board: YotsubaBoard) -> String;
    fn query_update_deleteds(&self, board: YotsubaBoard) -> String;
    fn query_update_hash(
        &self, board: YotsubaBoard, hash_type: YotsubaHash, media_mode: YotsubaStatement
    ) -> String;
    fn query_update_metadata(&self, column: YotsubaEndpoint) -> String;
    fn query_medias(&self, board: YotsubaBoard, media_mode: YotsubaStatement) -> String;
    fn query_threads_modified(&self, endpoint: YotsubaEndpoint) -> String;
    fn query_threads(&self) -> String;
    fn query_metadata(&self, column: YotsubaEndpoint) -> String;
    fn query_threads_combined(&self, board: YotsubaBoard, endpoint: YotsubaEndpoint) -> String;
    fn query_init_board(&self, board: YotsubaBoard) -> String;
    fn query_update_thread(&self, board: YotsubaBoard) -> String;
}

#[async_trait]
pub trait DatabaseTrait<T>:
    Queries2 + QueriesExecutor2<T> + StatementTrait<T> + Send + Sync {
    // async fn prepare(&self, stmt: dyn AsRef<str>) -> MuhStatement<T>  ;
}
impl DatabaseTrait<tokio_postgres::Statement> for tokio_postgres::Client {}
impl DatabaseTrait<::mysql::Statement> for ::mysql::Pool {}

/*#[derive(Debug, Copy, Clone)]
pub struct MuhConnection<T: DatabaseTrait>(pub T);

impl<T> MuhConnection<T>
where T: DatabaseTrait
{
    pub fn new(x: T) -> Self {
        Self(x)
    }

    pub fn get(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::Deref for MuhConnection<T>
where T: DatabaseTrait
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for MuhConnection<T>
where T: DatabaseTrait
{
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> AsRef<T> for MuhConnection<T>
where T: DatabaseTrait
{
    fn as_ref(&self) -> &T {
        &self.0
    }
}
*/
