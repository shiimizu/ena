// use crate::{YotsubaArchiver, YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier};

pub mod archiver;
pub mod config;
pub mod enums;
pub mod mysql;
pub mod pgsql;
pub mod request;
pub mod sql;

pub trait Board {}

pub trait Endpoint {}
pub struct Identifier<E, B>
where
    E: Endpoint,
    B: Board {
    endpoint: E,
    board:    B
}

impl<E, B> Identifier<E, B>
where
    E: Endpoint,
    B: Board
{
    pub fn new(endpoint: E, board: B) -> Self {
        Self { endpoint, board }
    }
}

impl Board for enums::YotsubaBoard {}
impl Endpoint for enums::YotsubaEndpoint {}
