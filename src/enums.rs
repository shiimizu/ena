#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::sql::YotsubaStatement;
use enum_iterator::IntoEnumIterator;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, std::hash::Hash, std::cmp::Eq)]
pub enum YotsubaEndpoint {
    Archive = 1,
    Threads,
    Media
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum YotsubaHash {
    Sha256,
    Blake3
}
impl fmt::Display for YotsubaHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Sha256 => write!(f, "sha256"),
            Self::Blake3 => write!(f, "blake3")
        }
    }
}
impl fmt::Display for YotsubaEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Archive => write!(f, "archive"),
            Self::Threads => write!(f, "threads"),
            Self::Media => write!(f, "media")
        }
    }
}
#[derive(Debug, std::hash::Hash, PartialEq, std::cmp::Eq, Clone)]
pub struct YotsubaIdentifier {
    pub endpoint:  YotsubaEndpoint,
    pub board:     YotsubaBoard,
    pub statement: YotsubaStatement
}

impl fmt::Display for YotsubaBoard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => write!(f, ""),
            Self::_3 => write!(f, "3"),
            z => write!(f, "{:?}", z)
        }
    }
}

/// Proper deserialize from JSON
///
/// Help taken from this [blog](https://is.gd/Y8tCz3]
/// and [`serde/test_annotations.rs`](https://is.gd/7qt6Sl)
/// and [`strings`](https://is.gd/u54Y0T)
impl<'de> Deserialize<'de> for YotsubaBoard {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        // YotsubaBoard::into_enum_iter().for_each(|zz|println!("'{}'",s));
        if let Some(found) =
            YotsubaBoard::into_enum_iter().skip(1).find(|_board| _board.to_string() == s)
        {
            Ok(found)
        } else {
            let j = YotsubaBoard::into_enum_iter()
                .skip(1)
                .map(|zz| zz.to_string())
                .collect::<Vec<String>>()
                .join("`, `");
            Err(de::Error::custom(&format!("unknown variant `{}`, expected one of `{}`", s, j)))
            // Err(de::Error::unknown_variant(&s, ss))
        }
    }
}

/// Proper Serialization for proper display
///
/// Help taken from https://serde.rs/impl-serialize.html
impl Serialize for YotsubaBoard {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        // let s: Vec<_> = Self::into_enum_iter().map(|zz|zz.to_string()).collect();

        /*match *self {
            YotsubaBoard::None => serializer.serialize_unit_variant("YotsubaBoard", 0, ""),
            z => {
                // serializer.serialize_unit_variant("YotsubaBoard", z as u32, "z")
                serializer.serialize_str("YotsubaBoard")
            },
        }*/
        serializer.serialize_str(&self.to_string())
    }
}

pub trait StringExt {
    fn capitalize(&self) -> Self;
}

impl StringExt for String {
    fn capitalize(&self) -> Self {
        self.chars()
            .enumerate()
            .map(|(i, c)| if i == 0 { c.to_uppercase().to_string() } else { c.to_string() })
            .collect::<String>()
    }
}
#[allow(non_camel_case_types)]
#[derive(
    Debug, Copy, Clone, std::hash::Hash, PartialEq, std::cmp::Eq, enum_iterator::IntoEnumIterator,
)]
pub enum YotsubaBoard {
    None,
    _3,
    a,
    aco,
    adv,
    an,
    asp,
    b,
    bant,
    biz,
    c,
    cgl,
    ck,
    cm,
    co,
    d,
    diy,
    e,
    f,
    fa,
    fit,
    g,
    gd,
    gif,
    h,
    hc,
    his,
    hm,
    hr,
    i,
    ic,
    int,
    jp,
    k,
    lgbt,
    lit,
    m,
    mlp,
    mu,
    n,
    news,
    o,
    out,
    p,
    po,
    pol,
    qa,
    qst,
    r,
    r9k,
    s,
    s4s,
    sci,
    soc,
    sp,
    t,
    tg,
    toy,
    trash,
    trv,
    tv,
    u,
    v,
    vg,
    vip,
    vp,
    vr,
    w,
    wg,
    wsg,
    wsr,
    x,
    y
}
