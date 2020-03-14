// #![cold]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::{YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    ops::Add
};
use tokio_postgres::Statement;
// use mysql_async::prelude::*;
use mysql_async::Stmt;

pub type StatementStore = HashMap<YotsubaIdentifier, Statement>;
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
    MySQL
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::PostgreSQL => write!(f, "postgresql"),
            Self::MySQL => write!(f, "mysql")
        }
    }
}

/// Implement `into`.
/// Help taken from this [blog](https://is.gd/94QtP0)
/// [archive](http://archive.is/vIW5Y)
impl Into<String> for Database {
    fn into(self) -> String {
        self.to_string()
    }
}

impl Into<Database> for String {
    fn into(self) -> Database {
        if let Some(found) =
            Database::into_enum_iter().find(|db| db.to_string() == self.to_lowercase())
        {
            found
        } else {
            Database::PostgreSQL
        }
    }
}

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
pub trait SqlQueries {
    // async fn prepare(&self, query: &str) -> Result<Statement, tokio_postgres::error::Error>;

    /// Creates the schema if nonexistent and uses it as the search_path
    async fn init_schema(&self, schema: &str);

    /// Creates the 4chan schema as a type to be easily referenced
    async fn init_type(&self);

    /// Creates the metadata if nonexistent to store the api endpoints' data
    async fn init_metadata(&self);

    /// Creates a table for the specified board
    async fn init_board(&self, board: YotsubaBoard);

    /// Creates views for asagi
    async fn init_views(&self, board: YotsubaBoard);

    /// Upserts an endpoint to the metadata
    ///
    /// Converts bytes to json object and feeds that into the query
    async fn update_metadata(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<u64>;

    /// Upserts a thread
    ///
    /// This method updates an existing post or inserts a new one.
    /// Posts are only updated where there's a field change.
    /// A majority of posts in a thread don't change, this minimizes I/O writes.
    /// (sha256, sha25t, and deleted are handled seperately as they are special
    /// cases) https://stackoverflow.com/a/36406023
    /// https://dba.stackexchange.com/a/39821
    ///
    /// 4chan inserts a backslash in their md5.
    /// https://stackoverflow.com/a/11449627
    async fn update_thread(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<u64>;

    /// Marks a post as deleted
    async fn delete(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard, no: u32
    );

    // deleted before updating. PgSQL needs to do this >_>..
    /// Compares between the thread in db and the one fetched and marks any
    /// posts missing in the fetched thread as deleted
    async fn update_deleteds(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        thread: u32, item: &[u8]
    ) -> Result<u64>;

    /// Upserts a media hash to a post
    async fn update_hash(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    );

    /// Gets the list of posts in a thread that have media
    async fn medias(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard, no: u32
    ) -> Result<Vec<tokio_postgres::row::Row>, tokio_postgres::error::Error>;

    /// Gets a list of threads from the corresponding endpoint
    async fn threads(
        &self, statement: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<VecDeque<u32>>;

    /// Gets only the deleted and modified threads when comparing the metadata
    /// and the fetched endpoint
    async fn threads_modified(
        &self, board: YotsubaBoard, new_threads: &[u8], statement: &Statement
    ) -> Result<VecDeque<u32>>;

    /// Gets the list of threads from the one in the metadata + the fetched one
    async fn threads_combined(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        new_threads: &[u8]
    ) -> Result<VecDeque<u32>>;

    /// Checks for the existence of an endpoint in the metadata
    async fn metadata(
        &self, statements: &StatementStore, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> bool;
}

/// List of all SQL queries to use
pub trait SchemaTrait: Sync + Send {
    fn init_schema(&self, schema: &str) -> String;
    fn init_metadata(&self) -> String;
    fn init_type(&self) -> String;
    fn init_views(&self, board: YotsubaBoard) -> String;
    fn delete(&self, board: YotsubaBoard) -> String;
    fn update_deleteds(&self, board: YotsubaBoard) -> String;
    fn update_hash(
        &self, board: YotsubaBoard, hash_type: YotsubaHash, media_mode: YotsubaStatement
    ) -> String;
    fn update_metadata(&self, column: YotsubaEndpoint) -> String;
    fn medias(&self, board: YotsubaBoard, media_mode: YotsubaStatement) -> String;
    fn threads_modified(&self, endpoint: YotsubaEndpoint) -> String;
    fn threads(&self) -> String;
    fn metadata(&self, column: YotsubaEndpoint) -> String;
    fn threads_combined(&self, board: YotsubaBoard, endpoint: YotsubaEndpoint) -> String;
    fn init_board(&self, board: YotsubaBoard) -> String;
    fn update_thread(&self, board: YotsubaBoard) -> String;
}

/// A wrapper to contain all SQL queries. Modularity with schema and databases.
#[derive(Debug, Copy, Clone)]
pub struct YotsubaSchema<T: SchemaTrait>(T);

impl<T> YotsubaSchema<T>
where T: SchemaTrait
{
    pub fn new(x: T) -> Self {
        Self(x)
    }
}

/// Defining Our Own Smart Pointer by Implementing the Deref Trait
/// https://is.gd/62jW5Z
impl<T> std::ops::Deref for YotsubaSchema<T>
where T: SchemaTrait
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

pub enum Stuff<T> {
    Client { db: Database, inner_client: T }
}

#[async_trait]
pub trait DatabaseTrait {}
impl DatabaseTrait for tokio_postgres::Client {}
impl DatabaseTrait for mysql_async::Pool {}
// pub enum Yb{
//     PostgreSQL(tokio_postgres::Client),
//     MySQL(mysql_async::Pool)
// }

// pub struct YotsubaDatabase(Yb)

// impl YotsubaDatabase {
//     pub fn new(db: T) -> Self {
//         Self(db)
//     }
//     pub fn query() {

//     }
// }

#[derive(Debug, Copy, Clone)]
pub struct YotsubaDatabase<T: DatabaseTrait>(pub T);

impl<T> YotsubaDatabase<T>
where T: DatabaseTrait
{
    pub fn new(x: T) -> Self {
        Self(x)
    }
}

impl<T> std::ops::Deref for YotsubaDatabase<T>
where T: DatabaseTrait
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}
