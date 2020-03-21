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
        &self, statements: &StatementStore<S>, endpoint: YotsubaEndpoint, board: YotsubaBoard,
        new_threads: &[u8]
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
    /// JSON validity checks before upserting to metadata  
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

/// 4chan thread.
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Thread {
    posts: Vec<Post>
}
impl Default for Thread {
    fn default() -> Self {
        Self { posts: vec![] }
    }
}

/// 4chan post for every row.
#[allow(dead_code)]
#[cold]
#[rustfmt::skip]
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(default)]
pub struct Post {
    no:             u64,
    sticky:         Option<u8>,
    closed:         Option<u8>,
    now:            Option<String>,
    name:           Option<String>,
    sub:            Option<String>,
    com:            Option<String>,
    filedeleted:    Option<u8>,
    spoiler:        Option<u8>,
    custom_spoiler: Option<u16>,
    filename:       Option<String>,
    ext:            Option<String>,
    w:              Option<u32>,
    h:              Option<u32>,
    tn_w:           Option<u32>,
    tn_h:           Option<u32>,
    tim:            Option<u64>,
    time:           u64,
    md5:            Option<String>,
    fsize:          Option<u64>,
    m_img:          Option<u8>,
    resto:          u64,
    trip:           Option<String>,
    id:             Option<String>,
    capcode:        Option<String>,
    country:        Option<String>,
    country_name:   Option<String>,
    archived:       Option<u8>,
    bumplimit:      Option<u8>,
    archived_on:    Option<u64>,
    imagelimit:     Option<u16>,
    semantic_url:   Option<String>,
    replies:        Option<u32>,
    images:         Option<u32>,
    unique_ips:     Option<u32>,
    tag:            Option<String>,
    since4pass:     Option<u16>
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

// https://stackoverflow.com/questions/34662713/how-can-i-create-parameterized-tests-in-rust
/// [`mysql_async`] only returns library or server errors. Queries such as /INSERT/DELETE
#[cfg(test)]
mod test {

    #[allow(unused_imports)]
    #[cfg(test)]
    use pretty_assertions::{assert_eq, assert_ne};

    #[cfg(test)]
    use once_cell::sync::Lazy;

    use super::*;
    use mysql_async::{Conn, Pool, Row};
    use serde_json::json;

    static BOARD: Lazy<YotsubaBoard> = Lazy::new(|| YotsubaBoard::a);
    // static POOL: Lazy<MyBox<Pool>> = Lazy::new(|| MyBox::new(Pool::new(DB_MYSQL)));

    const DB_MYSQL: &str = "mysql://root:@localhost:3306/asagi";
    const DB_POSTGRES: &str = "postgresql://postgres:zxc@localhost:5432/fdb2";
    const SCHEMA_NAME_POSTGRES: &str = "asagi";
    struct MyBox<T>(T);

    impl<T> MyBox<T> {
        fn new(x: T) -> MyBox<T> {
            MyBox(x)
        }
    }

    use std::ops::Deref;

    impl<T> Deref for MyBox<T> {
        type Target = T;

        fn deref(&self) -> &T {
            &self.0
        }
    }
    // fn get_config() -> config::Config {
    //     config::read_config("ena_config.json")
    // }

    // fn fib(i: u32) -> u32 {
    //     i
    // }

    // macro_rules! fib_tests {
    //     ($($name:ident: $value:expr,)*) => {
    //     $(
    //         #[test]
    //         #[ignore]
    //         fn $name() {
    //             let (input, expected) = $value;
    //             assert_eq!(expected, fib(input));
    //         }
    //     )*
    //     }
    // }

    // #[cfg(test)]
    // fib_tests! {
    //     fib_0: (0, 0),
    //     fib_1: (1, 1),
    //     fib_2: (2, 1),
    //     fib_3: (3, 2),
    //     fib_4: (4, 3),
    //     fib_5: (5, 5),
    //     fib_6: (6, 8),
    // }

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

    // #[test]
    // fn match_test() {
    //     let mode = YotsubaStatement::UpdateHashThumbs;
    //     assert_eq!(mode, YotsubaStatement::UpdateHashMedia);
    // }

    // #[test]
    // #[should_panic]
    // fn test_panic() {
    //     panic!("aHHH");
    // }
    // #[test]
    // #[should_panic(expected = "Divide result is zero")]
    // fn test_panic2() {
    //     panic!("Divide result is ");
    // }

    #[tokio::test]
    #[should_panic]
    async fn update_metadata_unknown_json() {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let json = serde_json::to_vec(&json!({"test":1})).unwrap();
        let _res =
            pool.update_metadata(&store, YotsubaEndpoint::Threads, *BOARD, &json).await.unwrap();
        assert_eq!(_res, 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn update_metadata_deprecated_fields_json() {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let json = serde_json::to_vec(&json!(
            [
                {
                  "page": 1,
                  "threads": [
                    { "no_more": 196649146, "last_modified": 1576266882, "replies": 349 },
                    { "no_more": 196656555, "last_modified": 1576266881, "replies": 7 }
                  ]
                },
                {
                  "page": 2,
                  "threads": [
                    { "no_more": 196650664, "last_modified": 1576266846, "replies": 387},
                    { "no_more": 196646963, "last_modified": 1576266845, "replies": 487 }
                  ]
                }
            ]

        ))
        .unwrap();
        let _res =
            pool.update_metadata(&store, YotsubaEndpoint::Threads, *BOARD, &json).await.unwrap();
        assert_eq!(_res, 1);
    }

    #[tokio::test]
    async fn update_metadata_added_fields_json() -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let json = serde_json::to_vec(&json!(
            [
                {
                  "page": 1,
                  "threads": [
                    { "no": 196649146, "last_modified": 1576266882, "replies": 349 },
                    { "no_more": 196656555, "last_modified": 1576266881, "replies": 7 }
                  ]
                },
                {
                  "page": 2,
                  "threads": [
                    { "no": 196650664, "last_modified": 1576266846, "replies": 387},
                    { "no_more": 196646963, "last_modified": 1576266845, "replies": 487 }
                  ]
                }
            ]
        ))?;
        let _res = pool.update_metadata(&store, YotsubaEndpoint::Threads, *BOARD, &json).await?;
        Ok(())
    }

    #[tokio::test]
    async fn update_metadata_valid_json() -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let json = serde_json::to_vec(&json!(
            [
                {
                  "page": 1,
                  "threads": [
                    { "no": 196649146, "last_modified": 1576266882, "replies": 349 },
                    { "no": 196656555, "last_modified": 1576266881, "replies": 7 }
                  ]
                },
                {
                  "page": 2,
                  "threads": [
                    { "no": 196650664, "last_modified": 1576266846, "replies": 387},
                    { "no": 196646963, "last_modified": 1576266845, "replies": 487 }
                  ]
                }
            ]
        ))?;
        let _res = pool.update_metadata(&store, YotsubaEndpoint::Threads, *BOARD, &json).await?;
        assert_eq!(_res, 1);
        Ok(())
    }

    async fn test_update_thread(
        endpoint: YotsubaEndpoint, board: YotsubaBoard, no: i64, json: Vec<u8>, passing: bool
    ) -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let _res = pool.update_thread(&store, endpoint, board, &json).await?;
        assert_eq!(_res, 1);

        let conn = pool.get_conn().await?;
        let (conn, opt): (Conn, Option<Row>) =
            conn.first(format!("select * from {} where num={}", board, no)).await?;
        let result = opt.map(|x| x).map(|z| z.get("num")).flatten().unwrap_or(0) as i64;

        if passing {
            assert_eq!(no, result);
        }

        let (conn, _): (Conn, Option<i64>) =
            conn.first(format!("delete from {} where num={}", board, no)).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn update_thread_empty_json() -> Result<()> {
        let json = serde_json::to_vec(&json!({}))?;
        test_update_thread(YotsubaEndpoint::Threads, YotsubaBoard::pol, 11111100, json, false)
            .await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn update_thread_unknown_json() -> Result<()> {
        let json = serde_json::to_vec(&json!({"test":1, "test2":2, "test3":3}))?;
        test_update_thread(YotsubaEndpoint::Threads, YotsubaBoard::pol, 11111110, json, false)
            .await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn update_thread_deprecated_fields_json() -> Result<()> {
        let no = 11111111;
        let json = serde_json::to_vec(&json!(
        {"posts":
            [
                {
                    "no": no,
                    "now": "12/08/19(Sun)22:57:22",
                    "name": "Anonymous",
                    "filename": "test",
                    "ext": ".png",
                    "w": 4657,
                    "h": 2499,
                    "tn_w": 125,
                    "tn_h": 67,
                    "tim": 1575863842923 as i64,
                    "time": 1575863842,
                    "md5": "yHPJ8le4osWFDOotUFcpBQ==",
                    "fsize": 3226453,
                    "resto": 73915762
                }
            ]
        }
        ))?;
        test_update_thread(YotsubaEndpoint::Threads, YotsubaBoard::pol, no, json, true).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn update_thread_added_fields_json() -> Result<()> {
        let no = 11111112;
        let json = serde_json::to_vec(&json!(
        {"posts":
            [
                {
                    "no": no,
                    "now": "12/08/19(Sun)22:57:22",
                    "name": "Anonymous",
                    "filename": "test",
                    "ext": ".png",
                    "w": 4657,
                    "h": 2499,
                    "tn_w": 125,
                    "tn_h": 67,
                    "tim": 1575863842923 as i64,
                    "time": 1575863842,
                    "md5": "yHPJ8le4osWFDOotUFcpBQ==",
                    "fsize": 3226453,
                    "resto": 73915762,
                    "email": "test@email.com",
                    "banned": 0,
                    "file_banned": 0,
                    "archived_on": 1575863843 as i64,
                    "deleted_on": 1575863843 as i64
                }
            ]
        }
        ))?;
        test_update_thread(YotsubaEndpoint::Threads, YotsubaBoard::pol, no, json, true).await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn update_thread_valid_json() -> Result<()> {
        let no = 11111113;
        let json = serde_json::to_vec(&json!(
        {"posts":
            [
                {
                    "no": no,
                    "now": "12/08/19(Sun)22:57:22",
                    "name": "Anonymous",
                    "filename": "test",
                    "ext": ".png",
                    "w": 4657,
                    "h": 2499,
                    "tn_w": 125,
                    "tn_h": 67,
                    "tim": 1575863842923 as i64,
                    "time": 1575863842,
                    "md5": "yHPJ8le4osWFDOotUFcpBQ==",
                    "fsize": 3226453,
                    "resto": 73915762
                }
            ]
        }
        ))?;
        test_update_thread(YotsubaEndpoint::Threads, YotsubaBoard::pol, no, json, true).await?;
        Ok(())
    }

    /// Silent delete. Will always pass unless there was a server error
    #[ignore]
    #[tokio::test]
    async fn delete_nonexistant_no() -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let _res = pool.delete(&store, YotsubaEndpoint::Threads, YotsubaBoard::pol, 1).await?;
        assert_eq!(_res, 1);
        Ok(())
    }
    /// Always passes for Asagi because there no hashing involved
    #[ignore]
    #[tokio::test]
    async fn update_hash_improper_hash() -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let hashsum = vec![];
        let _res = pool
            .update_hash(
                &store,
                YotsubaEndpoint::Threads,
                YotsubaBoard::pol,
                1,
                YotsubaStatement::UpdateHashMedia,
                hashsum
            )
            .await?;
        assert_eq!(_res, 1);
        Ok(())
    }

    /// Always passes for Asagi because there no hashing involved
    #[ignore]
    #[tokio::test]
    async fn update_hash_valid_hash() {}

    #[ignore]
    #[tokio::test]
    async fn get_metadata_missing() -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let _res = pool.metadata(&store, YotsubaEndpoint::Threads, YotsubaBoard::x).await?;
        assert_eq!(_res, false);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn get_metadata_existing() -> Result<()> {
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let _res = pool.metadata(&store, YotsubaEndpoint::Threads, YotsubaBoard::pol).await?;
        assert_eq!(_res, true);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn get_medias_send_invalid_no() -> Result<()> {
        let no = 111222;
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let _res = pool.medias(&store, YotsubaEndpoint::Threads, YotsubaBoard::pol, no).await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn get_medias_send_valid_no() -> Result<()> {
        let no = 249088192;
        let pool = Pool::new(DB_MYSQL);
        let store = HashMap::new();
        let _res = pool.medias(&store, YotsubaEndpoint::Threads, YotsubaBoard::pol, no).await?;
        Ok(())
    }

    enum JsonType {
        Unknown,
        Valid,
        DeprecatedFields,
        AddedFields,
        MixedFields
    }

    macro_rules! get_threads_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[tokio::test]
                // #[should_panic(expected = "|threads| Empty or null in getting threads")] // for unknowns
                async fn $name() -> Result<()> {
                    let (engine, endpoint, board, mode, json_type) = $value;
                    test_get_threads(engine, endpoint, board, mode, json_type).await?;
                    // assert_eq!(expected, fib(input));
                    Ok(())
                }
            )*
        }
    }

    macro_rules! get_threads_tests_panic {
        ($($name:ident: $value:expr,)*) => {
            $(
                // #[should_panic(expected = "|threads| Empty or null in getting threads")] // for unknowns
                #[tokio::test]
                #[should_panic]
                async fn $name() {
                    let (engine, endpoint, board, mode, json_type) = $value;
                    test_get_threads(engine, endpoint, board, mode, json_type).await.unwrap();
                    // assert_eq!(expected, fib(input));
                }
            )*
        }
    }

    #[cfg(test)]
    get_threads_tests! {

        // MySQL threads.json
        mysql_get_threads_send_valid_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Valid),
        mysql_get_threads_send_added_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::AddedFields),
        mysql_get_threads_send_mixed_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::MixedFields),

        mysql_get_threads_modified_send_unknown_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Unknown),
        mysql_get_threads_modified_send_valid_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Valid),
        mysql_get_threads_modified_send_deprecated_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        mysql_get_threads_modified_send_added_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        mysql_get_threads_modified_send_mixed_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::MixedFields),

        mysql_get_threads_combined_send_unknown_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Unknown),
        mysql_get_threads_combined_send_valid_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Valid),
        mysql_get_threads_combined_send_deprecated_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),
        mysql_get_threads_combined_send_added_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),
        mysql_get_threads_combined_send_mixed_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),

        // MySQL archive.json
        // mysql_get_archive_send_valid_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Valid),
        // mysql_get_archive_send_added_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::AddedFields),
        // mysql_get_archive_send_mixed_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::MixedFields),

        // mysql_get_archive_modified_send_valid_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Valid),
        // mysql_get_archive_modified_send_deprecated_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        // mysql_get_archive_modified_send_added_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        // mysql_get_archive_modified_send_mixed_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::MixedFields),

        // mysql_get_archive_combined_send_valid_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Valid),
        // mysql_get_archive_combined_send_deprecated_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),
        // mysql_get_archive_combined_send_added_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),
        // mysql_get_archive_combined_send_mixed_json: (Database::MySQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),


        // PostgreSQL threads.json
        pgsql_get_threads_send_valid_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Valid),
        pgsql_get_threads_send_added_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::AddedFields),
        pgsql_get_threads_send_mixed_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::MixedFields),

        pgsql_get_threads_modified_send_valid_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Valid),
        pgsql_get_threads_modified_send_deprecated_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        pgsql_get_threads_modified_send_added_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        pgsql_get_threads_modified_send_mixed_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::MixedFields),

        pgsql_get_threads_combined_send_valid_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Valid),
        pgsql_get_threads_combined_send_deprecated_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),
        pgsql_get_threads_combined_send_added_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),
        pgsql_get_threads_combined_send_mixed_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),

        // PostgreSQL archive.json
        pgsql_get_archive_send_valid_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Valid),
        pgsql_get_archive_modified_send_valid_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Valid),
        pgsql_get_archive_combined_send_valid_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Valid),

        pgsql_get_archive_send_deprecated_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::DeprecatedFields),
        pgsql_get_archive_modified_send_deprecated_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        pgsql_get_archive_combined_send_deprecated_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),

    }

    // Send JSONs that are JsonType::Unknown or JsonType::DeprecatedFields
    #[cfg(test)]
    get_threads_tests_panic! {
        // MySQL
        mysql_get_threads_send_unknown_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Unknown),
        mysql_get_threads_send_deprecated_json: (Database::MySQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::DeprecatedFields),

        // PostgreSQL threads.json
        pgsql_get_threads_send_unknown_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Unknown),
        pgsql_get_threads_send_deprecated_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::DeprecatedFields),
        pgsql_get_threads_modified_send_unknown_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Unknown),
        pgsql_get_threads_combined_send_unknown_json: (Database::PostgreSQL, YotsubaEndpoint::Threads, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Unknown),

        // PostgreSQL threads.json
        pgsql_get_archive_send_added_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::AddedFields),
        pgsql_get_archive_modified_send_added_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        pgsql_get_archive_combined_send_added_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),

        pgsql_get_archive_send_mixed_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::MixedFields),
        pgsql_get_archive_modified_send_mixed_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::MixedFields),
        pgsql_get_archive_combined_send_mixed_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),

        pgsql_get_archive_send_unknown_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::Threads, JsonType::Unknown),
        pgsql_get_archive_modified_send_unknown_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsModified, JsonType::Unknown),
        pgsql_get_archive_combined_send_unknown_json: (Database::PostgreSQL, YotsubaEndpoint::Archive, YotsubaBoard::pol, YotsubaStatement::ThreadsCombined, JsonType::Unknown),


    }

    async fn test_get_threads(
        engine: Database, endpoint: YotsubaEndpoint, board: YotsubaBoard, mode: YotsubaStatement,
        json_type: JsonType
    ) -> Result<VecDeque<u32>>
    {
        let _json = match json_type {
            JsonType::Unknown =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!({"test":1, "test2":2, "test3":3})
                } else {
                    json!(["1243234", "5645756", "75686786", "456454325", "test", "test1"])
                },
            JsonType::Valid =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349 },
                            { "no": 196656555, "last_modified": 1576266881, "replies": 6 },
                            { "no": 196654076, "last_modified": 1576266880, "replies": 191 },
                            { "no": 196637792, "last_modified": 1576266880, "replies": 233 },
                            { "no": 196647457, "last_modified": 1576266880, "replies": 110 },
                            { "no": 196624742, "last_modified": 1576266873, "replies": 103 },
                            { "no": 196656097, "last_modified": 1576266868, "replies": 7 },
                            { "no": 196645355, "last_modified": 1576266866, "replies": 361 },
                            { "no": 196655995, "last_modified": 1576266867, "replies": 3 },
                            { "no": 196655998, "last_modified": 1576266860, "replies": 5 },
                            { "no": 196652782, "last_modified": 1576266858, "replies": 42 },
                            { "no": 196656536, "last_modified": 1576266853, "replies": 5 },
                            { "no": 196621039, "last_modified": 1576266853, "replies": 189 },
                            { "no": 196640441, "last_modified": 1576266851, "replies": 495 },
                            { "no": 196637247, "last_modified": 1576266850, "replies": 101 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 29 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 387 },
                            { "no": 196648390, "last_modified": 1576266844, "replies": 36 },
                            { "no": 196651494, "last_modified": 1576266832, "replies": 10 },
                            { "no": 196656773, "last_modified": 1576266827, "replies": 0 },
                            { "no": 196653207, "last_modified": 1576266827, "replies": 20 },
                            { "no": 196643737, "last_modified": 1576266825, "replies": 82 },
                            { "no": 196626714, "last_modified": 1576266824, "replies": 467 },
                            { "no": 196654299, "last_modified": 1576266821, "replies": 9 },
                            { "no": 196636729, "last_modified": 1576266819, "replies": 216 },
                            { "no": 196655015, "last_modified": 1576266819, "replies": 3 },
                            { "no": 196642084, "last_modified": 1576266818, "replies": 233 },
                            { "no": 196649533, "last_modified": 1576266816, "replies": 122 },
                            { "no": 196640416, "last_modified": 1576266806, "replies": 381 },
                            { "no": 196656724, "last_modified": 1576266794, "replies": 1 }
                          ]
                        }
                    ])
                } else {
                    json!([1243234, 5645756, 75686786, 456454325, 231412, 564576567, 34523234])
                },
            JsonType::DeprecatedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no_more": 196649146, "last_modified": 1576266882, "replies": 349 },
                            { "no_more": 196656555, "last_modified": 1576266881, "replies": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no_more": 196650664, "last_modified": 1576266846, "replies": 387},
                            { "no_more": 196646963, "last_modified": 1576266845, "replies": 487 }
                          ]
                        }
                    ])
                } else {
                    json!([])
                },
            JsonType::AddedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349, "new_field": 349 },
                            { "no": 196656555, "last_modified": 1576266881, "replies": 6,  "new_field": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 387, "new_field": 387 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 487, "new_field": 487 },
                            { "no": 196648390, "last_modified": 1576266844, "replies": 36 , "new_field": 36 }
                          ]
                        }
                    ])
                } else {
                    json!([{}, "1243234", "5645756", "75686786", "456454325", "test", "test1"])
                },
            JsonType::MixedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349, "new_field": 349 },
                            { "no_more": 196656555, "last_modified": 1576266881, "replies": 6,  "new_field": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 387, "new_field": 387 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 487, "new_field": 487 },
                            { "no_more": 196648390, "last_modified": 1576266844, "replies": 36 , "new_field": 36 }
                          ]
                        }
                    ])
                } else {
                    json!(["1243234", 5645756, "75686786", 456454325, "test", "test1"])
                },
        };

        let json = serde_json::to_vec(&_json).unwrap();

        if engine.base() == Database::PostgreSQL {
            let (db_client, connection) =
                tokio_postgres::connect(DB_POSTGRES, tokio_postgres::NoTls).await?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });
            // Set search path
            db_client.init_schema(SCHEMA_NAME_POSTGRES, Database::PostgreSQL, "utf8").await?;

            let store = create_statements(&db_client, endpoint, board).await?;

            match mode {
                YotsubaStatement::Threads =>
                    db_client.threads(&store, endpoint, board, &json).await,
                YotsubaStatement::ThreadsModified =>
                    db_client.threads_modified(&store, endpoint, board, &json).await,
                _ => db_client.threads_combined(&store, endpoint, board, &json).await
            }
        } else {
            let pool = Pool::new(DB_MYSQL);
            let store = HashMap::new();
            match mode {
                YotsubaStatement::Threads =>
                    if endpoint == YotsubaEndpoint::Threads {
                        pool.threads(&store, endpoint, board, &json).await
                    } else {
                        Ok(serde_json::from_slice::<VecDeque<u32>>(&json)?)
                    },
                YotsubaStatement::ThreadsModified =>
                    pool.threads_modified(&store, endpoint, board, &json).await,
                _ => pool.threads_combined(&store, endpoint, board, &json).await
            }
        }
    }

    async fn create_statements(
        query: &tokio_postgres::Client, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> Result<StatementStore<tokio_postgres::Statement>> {
        let mut statement_store = HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        let gen_id = |stmt: YotsubaStatement| -> YotsubaIdentifier {
            YotsubaIdentifier::new(endpoint, board, stmt)
        };

        for &statement in statements.iter().filter(|&&x| {
            x != YotsubaStatement::Medias
                || x != YotsubaStatement::UpdateHashMedia
                || x != YotsubaStatement::UpdateHashThumbs
        }) {
            statement_store.insert(gen_id(statement), match statement {
                YotsubaStatement::UpdateMetadata =>
                    query.prepare(&query.query_update_metadata(endpoint)).await?,
                YotsubaStatement::UpdateThread =>
                    query.prepare(&query.query_update_thread(board)).await?,
                YotsubaStatement::Delete => query.prepare(&query.query_delete(board)).await?,
                YotsubaStatement::UpdateDeleteds =>
                    query.prepare(&query.query_update_deleteds(board)).await?,
                YotsubaStatement::UpdateHashMedia | YotsubaStatement::UpdateHashThumbs =>
                    query
                        .prepare(&query.query_update_hash(
                            board,
                            YotsubaHash::Sha256,
                            statement.is_thumbs_val()
                        ))
                        .await?,
                YotsubaStatement::Medias =>
                    query.prepare(&query.query_medias(board, statement)).await?,
                YotsubaStatement::Threads => query.prepare(&query.query_threads()).await?,
                YotsubaStatement::ThreadsModified =>
                    query.prepare(&query.query_threads_modified(endpoint)).await?,
                YotsubaStatement::ThreadsCombined =>
                    query.prepare(&query.query_threads_combined(board, endpoint)).await?,
                YotsubaStatement::Metadata => query.prepare(&query.query_metadata(endpoint)).await?
            });
        }

        Ok(statement_store)
    }
}
