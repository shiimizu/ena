#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::{sql::*, YotsubaBoard, YotsubaEndpoint, YotsubaIdentifier};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::prelude::*;
use std::{collections::VecDeque, convert::TryFrom};
use tokio_postgres::Statement;

#[derive(Debug, Copy, Clone)]
pub struct Schema;

impl Schema {
    pub fn new() -> Self {
        Schema {}
    }
}

impl SchemaTrait for Schema {
    fn init_schema(&self, schema: &str) -> String {
        unreachable!()
    }

    fn init_metadata(&self) -> String {
        format!(
                r#"
            CREATE TABLE IF NOT EXISTS metadata (
                board VARCHAR(10) NOT NULL PRIMARY key UNIQUE,
                threads json,
                archive json,
                INDEX metadata_board_idx (board)
            ) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
            "#
        )
    }

    fn delete(&self, schema: &str, board: YotsubaBoard) -> String {
        r#"UPDATE `?` SET deleted = 1, timestamp_expired = unix_timestamp() WHERE num = ? AND subnum = 0"#.to_string()
    }

    fn update_deleteds(&self, schema: &str, board: YotsubaBoard) -> String {
        unimplemented!()
    }

    fn update_hash(&self, board: YotsubaBoard, no: u64, hash_type: &str) -> String {
        unimplemented!()
    }

    fn update_metadata(&self, schema: &str, column: YotsubaEndpoint) -> String {
        unimplemented!()
    }

    fn medias(&self, board: YotsubaBoard) -> String {
        unimplemented!()
    }

    fn threads_modified(&self, schema: &str, endpoint: YotsubaEndpoint) -> String {
        unimplemented!()
    }

    fn threads<'a>(&self) -> &'a str {
        unimplemented!()
    }

    fn metadata(&self, schema: &str, column: YotsubaEndpoint) -> String {
        unimplemented!()
    }

    fn threads_combined(&self, schema: &str, board: YotsubaBoard, endpoint: YotsubaEndpoint)
                        -> String {
        unimplemented!()
    }

    fn init_board(&self, board: YotsubaBoard, schema: &str) -> String {
        r#"CREATE TABLE IF NOT EXISTS `?` (
            `doc_id` int unsigned NOT NULL auto_increment,
            `media_id` int unsigned NOT NULL DEFAULT '0',
            `poster_ip` decimal(39,0) unsigned NOT NULL DEFAULT '0',
            `num` int unsigned NOT NULL,
            `subnum` int unsigned NOT NULL,
            `thread_num` int unsigned NOT NULL DEFAULT '0',
            `op` bool NOT NULL DEFAULT '0',
            `timestamp` int unsigned NOT NULL,
            `timestamp_expired` int unsigned NOT NULL,
            `preview_orig` varchar(20),
            `preview_w` smallint unsigned NOT NULL DEFAULT '0',
            `preview_h` smallint unsigned NOT NULL DEFAULT '0',
            `media_filename` text,
            `media_w` smallint unsigned NOT NULL DEFAULT '0',
            `media_h` smallint unsigned NOT NULL DEFAULT '0',
            `media_size` int unsigned NOT NULL DEFAULT '0',
            `media_hash` varchar(25),
            `media_orig` varchar(20),
            `spoiler` bool NOT NULL DEFAULT '0',
            `deleted` bool NOT NULL DEFAULT '0',
            `capcode` varchar(1) NOT NULL DEFAULT 'N',
            `email` varchar(100),
            `name` varchar(100),
            `trip` varchar(25),
            `title` varchar(100),
            `comment` text,
            `delpass` tinytext,
            `sticky` bool NOT NULL DEFAULT '0',
            `locked` bool NOT NULL DEFAULT '0',
            `poster_hash` varchar(8),
            `poster_country` varchar(2),
            `exif` text,
          
            PRIMARY KEY (`doc_id`),
            UNIQUE num_subnum_index (`num`, `subnum`),
            INDEX thread_num_subnum_index (`thread_num`, `num`, `subnum`),
            INDEX subnum_index (`subnum`),
            INDEX op_index (`op`),
            INDEX media_id_index (`media_id`),
            INDEX media_hash_index (`media_hash`),
            INDEX media_orig_index (`media_orig`),
            INDEX name_trip_index (`name`, `trip`),
            INDEX trip_index (`trip`),
            INDEX email_index (`email`),
            INDEX poster_ip_index (`poster_ip`),
            INDEX timestamp_index (`timestamp`)
          ) engine=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;"#
                                                                       .to_string()
    }

    fn init_type(&self, schema: &str) -> String {
        unimplemented!()
    }

    fn init_views(&self, schema: &str, board: YotsubaBoard) -> String {
        unimplemented!()
    }

    fn update_thread(&self, schema: &str, board: YotsubaBoard) -> String {
        unimplemented!()
    }
}
