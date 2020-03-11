// #![cold]

use crate::{YotsubaBoard, YotsubaEndpoint, YotsubaIdentifier};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};
use std::{collections::{HashMap, VecDeque},
          fmt,
          ops::Add};
use tokio_postgres::Statement;

pub type StatementStore = HashMap<YotsubaIdentifier, Statement>;
#[derive(Debug,
           Copy,
           Clone,
           std::hash::Hash,
           PartialEq,
           std::cmp::Eq,
           enum_iterator::IntoEnumIterator,
           Deserialize,
           Serialize)]
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

#[derive(Debug,
           Copy,
           Clone,
           std::hash::Hash,
           PartialEq,
           std::cmp::Eq,
           enum_iterator::IntoEnumIterator)]
pub enum YotsubaStatement {
    UpdateMetadata = 1,
    UpdateThread,
    Delete,
    UpdateDeleteds,
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

#[async_trait]
pub trait SqlQueries {
    /// Creates the 4chan schema as a type to be easily referenced
    async fn init_type(&self, schema: &str) -> Result<u64, tokio_postgres::error::Error>;

    /// Creates the schema if nonexistent
    async fn init_schema(&self, schema: &str);

    /// Creates the metadata if nonexistent to store the api endpoints' data
    async fn init_metadata(&self);

    /// Creates a table for the specified board
    async fn init_board(&self, board: YotsubaBoard, schema: &str);

    /// Creates views for asagi
    async fn init_views(&self, board: YotsubaBoard, schema: &str);

    /// Upserts an endpoint to the metadata
    ///
    /// Converts bytes to json object and feeds that into the query
    async fn update_metadata(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                             board: YotsubaBoard, item: &[u8])
                             -> Result<u64>;

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
    async fn update_thread(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                           board: YotsubaBoard, item: &[u8])
                           -> Result<u64>;

    /// Marks a post as deleted
    async fn delete(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                    board: YotsubaBoard, no: u32);

    // TODO: Check if new-new threads re-update the deleteds -> Add a check for
    // deleted before updating. PgSQL needs to do this >_>..
    /// Compares between the thread in db and the one fetched and marks any
    /// posts missing in the fetched thread as deleted
    async fn update_deleteds(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                             board: YotsubaBoard, thread: u32, item: &[u8])
                             -> Result<u64>;

    /// Upserts a media hash to a post
    async fn update_hash(&self, board: YotsubaBoard, no: u64, hash_type: &str, hashsum: Vec<u8>);

    /// Gets the list of posts in a thread that have media
    async fn medias(&self, thread: u32, statement: &Statement)
                    -> Result<Vec<tokio_postgres::row::Row>, tokio_postgres::error::Error>;

    /// Gets a list of threads from the corresponding endpoint
    async fn threads(&self, statement: &StatementStore, endpoint: YotsubaEndpoint,
                     board: YotsubaBoard, item: &[u8])
                     -> Result<VecDeque<u32>>;

    /// Gets only the deleted and modified threads when comparing the metadata
    /// and the fetched endpoint
    async fn threads_modified(&self, board: YotsubaBoard, new_threads: &[u8],
                              statement: &Statement)
                              -> Result<VecDeque<u32>>;

    /// Gets the list of threads from the one in the metadata + the fetched one
    async fn threads_combined(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                              board: YotsubaBoard, new_threads: &[u8])
                              -> Result<VecDeque<u32>>;

    /// Checks for the existence of an endpoint in the metadata
    async fn metadata(&self, statements: &StatementStore, endpoint: YotsubaEndpoint,
                      board: YotsubaBoard)
                      -> bool;
}

pub trait SchemaTrait {
    fn init_metadata(&self) -> String;
    fn delete(&self, schema: &str, board: YotsubaBoard) -> String;
    fn update_deleteds(&self, schema: &str, board: YotsubaBoard) -> String;
    fn update_hash(&self, board: YotsubaBoard, no: u64, hash_type: &str) -> String;
    fn update_metadata(&self, schema: &str, column: YotsubaEndpoint) -> String;
    fn medias(&self, schema: &str, board: YotsubaBoard) -> String;
    fn threads_modified(&self, schema: &str, endpoint: YotsubaEndpoint) -> String;
    fn threads<'a>(&self) -> &'a str;
    fn metadata(&self, schema: &str, column: YotsubaEndpoint) -> String;
    fn threads_combined(&self, schema: &str, board: YotsubaBoard, endpoint: YotsubaEndpoint)
                        -> String;
    fn init_board(&self, board: YotsubaBoard, schema: &str) -> String;
    fn init_type(&self, schema: &str) -> String;
    fn init_views(&self, schema: &str, board: YotsubaBoard) -> String;
    fn update_thread(&self, schema: &str, board: YotsubaBoard) -> String;
}

#[derive(Debug, Copy, Clone)]
pub struct YotsubaSchema<T: SchemaTrait> {
    impl_schema: T
}

#[allow(dead_code)]
impl<T> YotsubaSchema<T> where T: SchemaTrait {
    pub fn new(input: T) -> Self {
        Self { impl_schema: input }
    }

    pub fn init_metadata(&self) -> String {
        self.impl_schema.init_metadata()
    }

    pub fn delete(&self, schema: &str, board: YotsubaBoard) -> String {
        self.impl_schema.delete(schema, board)
    }

    pub fn update_deleteds(&self, schema: &str, board: YotsubaBoard) -> String {
        self.impl_schema.update_deleteds(schema, board)
    }

    pub fn update_hash(&self, board: YotsubaBoard, no: u64, hash_type: &str) -> String {
        self.impl_schema.update_hash(board, no, hash_type)
    }

    pub fn update_metadata(&self, schema: &str, column: YotsubaEndpoint) -> String {
        self.impl_schema.update_metadata(schema, column)
    }

    pub fn medias(&self, schema: &str, board: YotsubaBoard) -> String {
        self.impl_schema.medias(schema, board)
    }

    pub fn threads_modified(&self, schema: &str, endpoint: YotsubaEndpoint) -> String {
        self.impl_schema.threads_modified(schema, endpoint)
    }

    pub fn threads<'a>(&self) -> &'a str {
        self.impl_schema.threads()
    }

    pub fn metadata(&self, schema: &str, column: YotsubaEndpoint) -> String {
        self.impl_schema.metadata(schema, column)
    }

    pub fn threads_combined(&self, schema: &str, board: YotsubaBoard, endpoint: YotsubaEndpoint)
                            -> String {
        self.impl_schema.threads_combined(schema, board, endpoint)
    }

    pub fn init_board(&self, board: YotsubaBoard, schema: &str) -> String {
        self.impl_schema.init_board(board, schema)
    }

    pub fn init_type(&self, schema: &str) -> String {
        self.impl_schema.init_type(schema)
    }

    pub fn init_views(&self, schema: &str, board: YotsubaBoard) -> String {
        self.impl_schema.init_views(schema, board)
    }

    pub fn update_thread(&self, schema: &str, board: YotsubaBoard) -> String {
        self.impl_schema.update_thread(schema, board)
    }
}
