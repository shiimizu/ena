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
                board text NOT NULL,
                threads json,
                archive json,
                PRIMARY KEY (board),
                CONSTRAINT board_unique UNIQUE (board));

        CREATE INDEX IF NOT EXISTS metadata_board_idx on metadata(board);
        "#
        )
    }

    fn delete(&self, schema: &str, board: YotsubaBoard) -> String {
        unimplemented!()
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

    fn medias(&self, schema: &str, board: YotsubaBoard) -> String {
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
        unimplemented!()
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
