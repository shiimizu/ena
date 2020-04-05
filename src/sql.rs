//! SQL commons.
// #![cold]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::{
    enums::{YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier},
    mysql
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use mysql_async::{prelude::*, Pool};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, ops::Add};
/// Implementation details of archiving  
///
/// The logic/algorithm in how you want to approach downloading everything.  
/// With that said, it should have more methods and stuff but I just use it as  
/// a way to pass generic implementations of `YotsubaArchiver` for different databases.
#[async_trait]
pub trait Archiver: Sync + Send {
    async fn run_inner(&self) -> Result<()>;
}

/// Struct to hold a generic implementation of [`Archiver`]  
///
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

/// Hashmap to store statements for databases to use
pub type StatementStore<S> = HashMap<QueryIdentifier, S>;
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

/// List of actions that can be done in relation to a database.  
#[derive(
    Debug, Copy, Clone, std::hash::Hash, PartialEq, std::cmp::Eq, enum_iterator::IntoEnumIterator,
)]
pub enum YotsubaStatement {
    InitSchema = 1,
    InitType,
    InitMetadata,
    InitBoard,
    InitViews,
    UpdateMetadata,
    UpdateThread,
    Delete,
    UpdateDeleteds,
    UpdateHashMedia,
    UpdateHashThumbs,
    Medias,
    Threads,
    ThreadsModified,
    ThreadsCombined,

    /// Get the threads/archive for a board from the metadata cache
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
        no: u64
    ) -> Result<u64>;

    async fn update_deleteds(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        thread: u64, item: &[u8]
    ) -> Result<u64>;

    async fn update_hash(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    ) -> Result<u64>;

    async fn medias(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        no: u64
    ) -> Result<Vec<R>>;

    async fn threads(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        item: &[u8]
    ) -> Result<Queue>;

    async fn threads_modified(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        new_threads: &[u8]
    ) -> Result<Queue>;

    async fn threads_combined(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        new_threads: &[u8]
    ) -> Result<Queue>;

    async fn metadata(
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> Result<bool>;
}

/// New SQL queries to use
pub trait QueryRaw {
    fn inquiry(&self, statement: YotsubaStatement, id: QueryIdentifier) -> String;
}

pub trait Ret {}
impl Ret for u64 {}
// impl Ret<u64> for u64 {}
// impl Ret<Queue> for Queue {}
// impl Ret<bool> for bool {}
// impl Ret<mysql_async::Row> for mysql_async::Row {}
// impl Ret<tokio_postgres::Row> for tokio_postgres::Row {}
// impl Ret<mysql_async::Row> for Vec<mysql_async::Row> {}
// impl Ret<tokio_postgres::Row> for Vec<tokio_postgres::Row> {}

/// New SQL executors  
///
/// See [`YotsubaStatement`]
#[async_trait]
pub trait Query<S, R> {
    /// For the rest of [`YotsubaStatement`]
    async fn first(
        &self, statement: YotsubaStatement, id: &QueryIdentifier, statements: &StatementStore<S>,
        item: Option<&[u8]>, no: Option<u64>
    ) -> Result<u64>;

    /// For [`YotsubaStatement::Threads`], [`YotsubaStatement::ThreadsModified`], and
    /// [`YotsubaStatement::ThreadsCombined`]
    async fn get_list(
        &self, statement: YotsubaStatement, id: &QueryIdentifier, statements: &StatementStore<S>,
        item: Option<&[u8]>, no: Option<u64>
    ) -> Result<Queue>;

    /// For [`YotsubaStatement::Medias`]
    async fn get_rows(
        &self, statement: YotsubaStatement, id: &QueryIdentifier, statements: &StatementStore<S>,
        item: Option<&[u8]>, no: Option<u64>
    ) -> Result<Vec<R>>;

    async fn create_statements(
        &self, engine: Database, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> StatementStore<S>;
}

/// New QueryIdentifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryIdentifier {
    pub engine:     Database,
    pub endpoint:   YotsubaEndpoint,
    pub board:      YotsubaBoard,
    pub schema:     Option<String>,
    pub charset:    Option<String>,
    pub hash_type:  YotsubaHash,
    pub media_mode: YotsubaStatement
}

impl QueryIdentifier {
    pub fn new(
        engine: Database, endpoint: YotsubaEndpoint, board: YotsubaBoard, schema: Option<String>,
        charset: Option<String>, hash_type: YotsubaHash, media_mode: YotsubaStatement
    ) -> Self
    {
        Self { engine, endpoint, board, schema, charset, hash_type, media_mode }
    }

    pub fn simple(engine: Database, endpoint: YotsubaEndpoint, board: YotsubaBoard) -> Self {
        Self {
            engine,
            endpoint,
            board,
            schema: None,
            charset: None,
            hash_type: YotsubaHash::Sha256,
            media_mode: YotsubaStatement::Medias
        }
    }
}

/// SQL queries to use
pub trait Queries {
    /// Create the schema if nonexistent and uses it as the `search_path`
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
    fn query_update_deleteds(&self, engine: Database, board: YotsubaBoard) -> String;

    /// Upsert a media hash to a post
    fn query_update_hash(
        &self, board: YotsubaBoard, hash_type: YotsubaHash, media_mode: YotsubaStatement
    ) -> String;

    /// Upsert an endpoint to the metadata  
    ///
    /// JSON validity checks before upserting to metadata  
    /// Converts bytes to json object and feeds that into the query
    fn query_update_metadata(&self, column: YotsubaEndpoint) -> String;

    /// Check for the existence of an endpoint in the metadata
    fn query_metadata(&self, column: YotsubaEndpoint) -> String;

    /// Get a list of posts in a thread that have media
    fn query_medias(&self, board: YotsubaBoard, media_mode: YotsubaStatement) -> String;

    /// Get ONLY the new/modified/deleted threads  
    /// Compare time modified and get the new threads  
    /// Get a list of only the deleted and modified threads when comparing the metadata
    /// and the fetched endpoint threads
    fn query_threads_modified(&self, endpoint: YotsubaEndpoint) -> String;

    /// Get a list of threads from the corresponding endpoint
    fn query_threads(&self) -> String;

    /// Get a combination of ALL threads from cache + new threads
    /// getting a total of 150+ threads  
    /// (excluding archived, deleted, and duplicate threads)  
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
    fn query_update_thread(&self, engine: Database, board: YotsubaBoard) -> String;
}

#[async_trait]
pub trait DatabaseTrait<S, R>: QueryRaw + Query<S, R> + StatementTrait<S> + Send + Sync {}

impl DatabaseTrait<tokio_postgres::Statement, tokio_postgres::Row> for tokio_postgres::Client {}
impl DatabaseTrait<mysql::Statement, mysql_async::Row> for Pool {}

/// 4chan single thread
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Thread {
    /// List of posts
    pub posts: HashSet<Post>
}

impl Default for Thread {
    fn default() -> Self {
        Self { posts: HashSet::new() }
    }
}

/// 4chan post
#[allow(dead_code)]
#[cold]
#[rustfmt::skip]
#[derive(Deserialize, Serialize, Debug, Clone,  Eq, Hash)]
#[serde(default)]
pub struct Post {
    pub no:             u64,
    pub sticky:         Option<u8>,
    pub closed:         Option<u8>,
    pub now:            Option<String>,
    pub name:           Option<String>,
    pub sub:            Option<String>,
    pub com:            Option<String>,
    pub filedeleted:    Option<u8>,
    pub spoiler:        Option<u8>,
    pub custom_spoiler: Option<u16>,
    pub filename:       Option<String>,
    pub ext:            Option<String>,
    pub w:              Option<u32>,
    pub h:              Option<u32>,
    pub tn_w:           Option<u32>,
    pub tn_h:           Option<u32>,
    pub tim:            Option<u64>,
    pub time:           u64,
    pub md5:            Option<String>,
    pub fsize:          Option<u64>,
    pub m_img:          Option<u8>,
    pub resto:          u64,
    pub trip:           Option<String>,
    pub id:             Option<String>,
    pub capcode:        Option<String>,
    pub country:        Option<String>,
    pub troll_country:  Option<String>,
    pub country_name:   Option<String>,
    pub archived:       Option<u8>,
    pub bumplimit:      Option<u8>,
    pub archived_on:    Option<u64>,
    pub imagelimit:     Option<u16>,
    pub semantic_url:   Option<String>,
    pub replies:        Option<u32>,
    pub images:         Option<u32>,
    pub unique_ips:     Option<u32>,
    pub tag:            Option<String>,
    pub since4pass:     Option<u16>
}

impl PartialEq for Post {
    fn eq(&self, other: &Self) -> bool {
        self.no == other.no
    }
}

impl Default for Post {
    fn default() -> Self {
        let t =
            std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
        Self {
            no:             t.as_secs(),
            sticky:         None,
            closed:         None,
            now:            None,
            name:           None,
            sub:            None,
            com:            None,
            filedeleted:    None,
            spoiler:        None,
            custom_spoiler: None,
            filename:       None,
            ext:            None,
            w:              None,
            h:              None,
            tn_w:           None,
            tn_h:           None,
            tim:            None,
            time:           t.as_secs(),
            md5:            None,
            fsize:          None,
            m_img:          None,
            resto:          t.as_secs(),
            trip:           None,
            id:             None,
            capcode:        None,
            country:        None,
            troll_country:  None,
            country_name:   None,
            archived:       None,
            bumplimit:      None,
            archived_on:    None,
            imagelimit:     None,
            semantic_url:   None,
            replies:        None,
            images:         None,
            unique_ips:     None,
            tag:            None,
            since4pass:     None
        }
    }
}

use std::{
    collections::HashSet,
    hash::{Hash, Hasher}
};

/// 4chan threads.json format
pub type ThreadsList = Vec<Threads>;

pub trait ThreadsTrait {
    fn to_queue(&self) -> Queue;
}

impl ThreadsTrait for ThreadsList {
    fn to_queue(&self) -> Queue {
        self.iter()
            .map(|x: &Threads| x.threads.iter())
            .flat_map(|it| it.map(|post| post.no))
            .collect()
    }
}

/// List of threads used as a queue internally
pub type Queue = HashSet<u64>;

/// 4chan `threads.json`
#[derive(Deserialize, Serialize, Debug, Clone)]
// #[serde(default)]
pub struct Threads {
    pub page:    u8,
    pub threads: Vec<ThreadsPost>
}

/// 4chan thread in `threads.json`
#[derive(Deserialize, Serialize, Debug, Clone, Copy, Hash, Eq, Ord, PartialOrd)]
pub struct ThreadsPost {
    pub no:            u64,
    pub last_modified: u64,
    pub replies:       u32
}

impl ThreadsPost {
    pub fn new(no: u64, last_modified: u64, replies: u32) -> Self {
        Self { no, last_modified, replies }
    }
}

impl PartialEq for ThreadsPost {
    fn eq(&self, other: &Self) -> bool {
        self.no == other.no
    }
}

/// Types of diff. See [`DiffTrait`].
pub enum Diff {
    /// To get new + deleted + modified posts
    SymmetricDifference,

    /// To get the deleted posts
    Difference,

    /// To get combined posts
    Union
}

// union: get all the unique elements in both sets.

// difference: get all the elements that are in the first set but not the second.

// intersection: get all the elements that are only in both sets.

// symmetric_difference: get all the elements that are in one set or the other, but not both.

/// Diff functions
pub trait DiffTrait<T> {
    /// Visits the values representing the symmetric difference,
    /// i.e., the values that are in `self` or in `other` but not in both.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// let a: HashSet<_> = [1, 2, 3].iter().cloned().collect();
    /// let b: HashSet<_> = [4, 2, 3, 4].iter().cloned().collect();
    ///
    /// // Print 1, 4 in arbitrary order.
    /// for x in a.symmetric_difference(&b) {
    ///     println!("{}", x);
    /// }
    ///
    /// let diff1: HashSet<_> = a.symmetric_difference(&b).collect();
    /// let diff2: HashSet<_> = b.symmetric_difference(&a).collect();
    ///
    /// assert_eq!(diff1, diff2);
    /// assert_eq!(diff1, [1, 4].iter().collect());
    /// ```
    fn symmetric_difference(&self, endpoint: YotsubaEndpoint, other: &T) -> Result<Queue> {
        self.generic_diff(endpoint, other, Diff::SymmetricDifference)
    }

    /// Visits the values representing the difference,
    /// i.e., the values that are in `self` but not in `other`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// let a: HashSet<_> = [1, 2, 3].iter().cloned().collect();
    /// let b: HashSet<_> = [4, 2, 3, 4].iter().cloned().collect();
    ///
    /// // Can be seen as `a - b`.
    /// for x in a.difference(&b) {
    ///     println!("{}", x); // Print 1
    /// }
    ///
    /// let diff: HashSet<_> = a.difference(&b).collect();
    /// assert_eq!(diff, [1].iter().collect());
    ///
    /// // Note that difference is not symmetric,
    /// // and `b - a` means something else:
    /// let diff: HashSet<_> = b.difference(&a).collect();
    /// assert_eq!(diff, [4].iter().collect());
    /// ```
    fn difference(&self, endpoint: YotsubaEndpoint, other: &T) -> Result<Queue> {
        self.generic_diff(endpoint, other, Diff::Difference)
    }

    /// Visits the values representing the union,
    /// i.e., all the values in `self` or `other`, without duplicates.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashSet;
    /// let a: HashSet<_> = [1, 2, 3].iter().cloned().collect();
    /// let b: HashSet<_> = [4, 2, 3, 4].iter().cloned().collect();
    ///
    /// // Print 1, 2, 3, 4 in arbitrary order.
    /// for x in a.union(&b) {
    ///     println!("{}", x);
    /// }
    ///
    /// let union: HashSet<_> = a.union(&b).collect();
    /// assert_eq!(union, [1, 2, 3, 4].iter().collect());
    /// ```
    fn union(&self, endpoint: YotsubaEndpoint, other: &T) -> Result<Queue> {
        self.generic_diff(endpoint, other, Diff::Union)
    }

    fn generic_diff(&self, endpoint: YotsubaEndpoint, other: &T, diff: Diff) -> Result<Queue>;
}

impl DiffTrait<ThreadsList> for ThreadsList {
    fn generic_diff(
        &self, endpoint: YotsubaEndpoint, other: &ThreadsList, diff: Diff
    ) -> Result<Queue> {
        let tt: HashSet<ThreadsPost> = self
            .iter()
            .map(|x: &Threads| x.threads.iter())
            .flat_map(|it| it.map(|&post| post))
            .collect();
        let tt2: HashSet<ThreadsPost> = other
            .iter()
            .map(|x: &Threads| x.threads.iter())
            .flat_map(|it| it.map(|&post| post))
            .collect();
        let diff: HashSet<ThreadsPost> = match diff {
            Diff::SymmetricDifference => tt.symmetric_difference(&tt2).map(|&s| s).collect(),
            Diff::Difference => tt.difference(&tt2).map(|&s| s).collect(),
            Diff::Union => tt.union(&tt2).map(|&s| s).collect()
        };
        let mut diff: Vec<_> = diff.into_iter().collect();
        diff.sort();
        diff.dedup();
        Ok(diff.into_iter().map(|post| post.no).collect())
    }
}

impl DiffTrait<Vec<u8>> for Vec<u8> {
    fn generic_diff(
        &self, endpoint: YotsubaEndpoint, other: &Vec<u8>, diff: Diff
    ) -> Result<Queue> {
        match endpoint {
            YotsubaEndpoint::Archive => {
                let tt: Queue = serde_json::from_slice(self).unwrap();
                let tt2: Queue = serde_json::from_slice(other).unwrap();
                let diff: Queue = match diff {
                    Diff::SymmetricDifference =>
                        tt.symmetric_difference(&tt2).map(|&s| s).collect(),
                    Diff::Difference => tt.difference(&tt2).map(|&s| s).collect(),
                    Diff::Union => tt.union(&tt2).map(|&s| s).collect()
                };
                // let mut diff: Vec<_> = diff.into_iter().collect();
                // diff.sort();
                // diff.dedup();
                // return Ok(diff.into_iter().collect());
                return Ok(diff);
            }
            YotsubaEndpoint::Threads => {
                let set: ThreadsList = serde_json::from_slice(self).unwrap();
                let set2: ThreadsList = serde_json::from_slice(other).unwrap();
                let tt: HashSet<ThreadsPost> = set
                    .iter()
                    .map(|x: &Threads| x.threads.iter())
                    .flat_map(|it| it.map(|&post| post))
                    .collect();
                let tt2: HashSet<ThreadsPost> = set2
                    .iter()
                    .map(|x: &Threads| x.threads.iter())
                    .flat_map(|it| it.map(|&post| post))
                    .collect();
                match diff {
                    Diff::SymmetricDifference => {
                        let diff: HashSet<ThreadsPost> =
                            tt.symmetric_difference(&tt2).map(|&s| s).collect();
                        let mut diff: Vec<_> = diff.into_iter().collect();
                        diff.sort();
                        diff.dedup();
                        Ok(diff.into_iter().map(|post| post.no).collect())
                    }
                    Diff::Difference => {
                        let diff: HashSet<ThreadsPost> = tt.difference(&tt2).map(|&s| s).collect();
                        let mut diff: Vec<_> = diff.into_iter().collect();
                        diff.sort();
                        diff.dedup();
                        Ok(diff.into_iter().map(|post| post.no).collect())
                    }
                    Diff::Union => {
                        let diff: HashSet<ThreadsPost> = tt.union(&tt2).map(|&s| s).collect();
                        Ok(diff.into_iter().map(|post| post.no).collect())
                    }
                }
            }
            YotsubaEndpoint::Media => Err(anyhow!("|diff| Invalid endpoint: {}", endpoint))
        }
        // Err(anyhow!("|diff| An error has occurred"))
    }
}
