use super::clean::*;
use crate::yotsuba;
use chrono::TimeZone;
use chrono_tz::America::New_York;
use fomat_macros::fomat;
use format_sql_query::QuotedData;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(default)]
pub struct Post {
    pub doc_id: u64,
    pub media_id: u64,
    pub poster_ip: f64,
    pub num: u64,
    pub subnum: u64,
    pub thread_num: u64,
    pub op: bool,
    pub timestamp: u64,
    pub timestamp_expired: u64,
    pub preview_orig: Option<String>,
    pub preview_w: u32,
    pub preview_h: u32,
    pub media_filename: Option<String>,
    pub media_w: u32,
    pub media_h: u32,
    pub media_size: u32,
    pub media_hash: Option<String>,
    pub media_orig: Option<String>,
    pub spoiler: bool,
    pub deleted: bool,
    pub capcode: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub trip: Option<String>,
    pub title: Option<String>,
    pub comment: Option<String>,
    pub delpass: Option<String>,
    pub sticky: bool,
    pub locked: bool,
    pub poster_hash: Option<String>,
    pub poster_country: Option<String>,
    pub exif: Option<String>,
}

impl Eq for Post {}
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
impl Hash for Post {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.doc_id.hash(state);
        self.media_id.hash(state);
        // (self.poster_ip as u64).hash(state);
        self.num.hash(state);
        self.subnum.hash(state);
        self.thread_num.hash(state);
        self.op.hash(state);
        self.timestamp.hash(state);
        self.timestamp_expired.hash(state);
        self.preview_orig.hash(state);
        self.preview_w.hash(state);
        self.preview_h.hash(state);
        self.media_filename.hash(state);
        self.media_w.hash(state);
        self.media_h.hash(state);
        self.media_size.hash(state);
        self.media_hash.hash(state);
        self.media_orig.hash(state);
        self.spoiler.hash(state);
        self.deleted.hash(state);
        self.capcode.hash(state);
        self.email.hash(state);
        self.name.hash(state);
        self.trip.hash(state);
        self.title.hash(state);
        self.comment.hash(state);
        self.delpass.hash(state);
        self.sticky.hash(state);
        self.locked.hash(state);
        self.poster_hash.hash(state);
        self.poster_country.hash(state);
        self.exif.hash(state);
    }
}

impl Default for Post {
    fn default() -> Self {
        Self {
            doc_id: 0,
            media_id: 0,
            poster_ip: 0.0,
            num: 0,
            subnum: 0,
            thread_num: 0,
            op: false,
            timestamp: 0,
            timestamp_expired: 0,
            preview_orig: None,
            preview_w: 0,
            preview_h: 0,
            media_filename: None,
            media_w: 0,
            media_h: 0,
            media_size: 0,
            media_hash: None,
            media_orig: None,
            spoiler: false,
            deleted: false,
            capcode: "N".into(),
            email: None,
            name: None,
            trip: None,
            title: None,
            comment: None,
            delpass: None,
            sticky: false,
            locked: false,
            poster_hash: None,
            poster_country: None,
            exif: None,
        }
    }
}

impl From<&yotsuba::Post> for Post {
    /// Convert a 4chan `Post` to an Asagi `Post`
    ///
    /// 4chan sometimes has a `\` character in their `md5`.
    fn from(post: &yotsuba::Post) -> Self {
        Self {
            doc_id: 0,
            media_id: 0,
            poster_ip: 0.0,
            num: post.no,
            subnum: 0,
            thread_num: if post.resto == 0 { post.no } else { post.resto },
            op: (post.resto == 0),
            timestamp: if post.time == 0 {
                0
            } else {
                Self::timestamp_nyc(post.time)
            },
            timestamp_expired: 0,
            preview_orig: post.tim.map(|tim| fomat!((tim)"s.jpg")),
            preview_w: post.tn_w.unwrap_or_default(),
            preview_h: post.tn_h.unwrap_or_default(),
            media_filename: if let Some(filename) = post.filename.as_ref() {
                if !filename.is_empty() {
                    if let Some(ext) = post.ext.as_ref() {
                        Some(fomat!((filename)(ext)))
                    } else {
                        Some(filename.clone())
                    }
                } else {
                    None
                }
            } else {
                None
            },
            media_w: post.w.unwrap_or_default(),
            media_h: post.h.unwrap_or_default(),
            media_size: post.fsize.unwrap_or_default(),
            media_hash: post.md5.as_ref().map(|md5| md5.replace("\\", "")),
            media_orig: if let Some(tim) = post.tim {
                if let Some(ext) = post.ext.as_ref() {
                    Some(fomat!((tim)(ext)))
                } else {
                    Some(fomat!((tim)))
                }
            } else {
                None
            },
            spoiler: post.spoiler.map_or_else(|| false, |v| v == 1),
            deleted: false,
            capcode: {
                fomat!(if let Some(cap) = &post.capcode {
                    if cap == "manager" || cap == "Manager" {
                        "G"
                    } else {
                        if let Some(c) = cap.chars().nth(0) {
                            (c.to_uppercase())
                        } else {
                            "N"
                        }
                    }
                } else {
                    "N"
                })
            },
            email: None,
            name: post
                .name
                .as_ref()
                .map(|s| s.as_str().clean().trim().to_string()),
            trip: post.trip.clone(),
            title: post
                .sub
                .as_ref()
                .map(|s| s.as_str().clean().trim().to_string()),
            comment: if post.sticky.unwrap_or_else(|| 0) == 1 && post.com.is_some() {
                let com = post.com.as_ref().unwrap();
                if !com.is_empty() {
                    let s = (&ammonia::Builder::default()
                        .rm_tags(&["span", "a", "p"])
                        .clean(
                            &com.replace("\r", "")
                                .replace("\n", "")
                                .replace("<br>", "\n")
                                .replace('\u{00ad}'.to_string().as_str(), ""),
                        )
                        .to_string())
                        .as_str()
                        .clean_full()
                        .to_string();
                    Some(s)
                } else {
                    None
                }
            } else {
                post.com
                    .as_ref()
                    .map(|s| s.as_str().clean_full().to_string())
            },
            delpass: None,
            sticky: post.sticky.map_or_else(|| false, |v| v == 1),
            locked: post.closed.map_or_else(|| false, |v| v == 1)
                && !post.archived.map_or_else(|| false, |v| v == 1),
            poster_hash: post.id.as_ref().map(|s| {
                if s == "Developer" {
                    "Dev".into()
                } else {
                    s.clone()
                }
            }),
            poster_country: post
                .country
                .as_ref()
                .filter(|&v| !(v == "XX" || v == "A1"))
                .map(|s| s.into()),
            exif: {
                let exif = Exif::parse(&post);
                if exif.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(&exif).unwrap())
                }
            },
        }
    }
}

impl From<mysql_async::Row> for Post {
    fn from(row: mysql_async::Row) -> Self {
        let row = &row;
        Self {
            doc_id: row.get("doc_id").unwrap(),
            media_id: row.get("media_id").unwrap(),
            poster_ip: row.get("poster_ip").unwrap(),
            num: row.get("num").unwrap(),
            subnum: row.get("subnum").unwrap(),
            thread_num: row.get("thread_num").unwrap(),
            op: row.get("op").unwrap(),
            timestamp: row.get("timestamp").unwrap(),
            timestamp_expired: row.get("timestamp_expired").unwrap(),
            preview_orig: row.get("preview_orig").unwrap(),
            preview_w: row.get("preview_w").unwrap(),
            preview_h: row.get("preview_h").unwrap(),
            media_filename: row.get("media_filename").unwrap(),
            media_w: row.get("media_w").unwrap(),
            media_h: row.get("media_h").unwrap(),
            media_size: row.get("media_size").unwrap(),
            media_hash: row.get("media_hash").unwrap(),
            media_orig: row.get("media_orig").unwrap(),
            spoiler: row.get("spoiler").unwrap(),
            deleted: row.get("deleted").unwrap(),
            capcode: row.get("capcode").unwrap(),
            email: row.get("email").unwrap(),
            name: row.get("name").unwrap(),
            trip: row.get("trip").unwrap(),
            title: row.get("title").unwrap(),
            comment: row.get("comment").unwrap(),
            delpass: row.get("delpass").unwrap(),
            sticky: row.get("sticky").unwrap(),
            locked: row.get("locked").unwrap(),
            poster_hash: row.get("poster_hash").unwrap(),
            poster_country: row.get("poster_country").unwrap(),
            exif: row.get("exif").unwrap(),
        }
    }
}

impl AsRef<Post> for Post {
    fn as_ref(&self) -> &Post {
        self
    }
}

impl Post {
    /// Convert a Post to a tuple of values able to use in an SQL query
    ///
    /// Without `doc_id`, `poster_ip`, `media_id`.  
    /// 4chan sometimes has a `\` character in their `md5`.
    pub fn to_sql(&self) -> String {
        fomat!(

            (self.poster_ip) ","
            (self.num) ","
            (self.subnum) ","
            (self.thread_num) ","
            (self.op) ","
            (self.timestamp) ","
            (self.timestamp_expired) ","
            if let Some(preview_orig) = self.preview_orig.as_ref() { (QuotedData(preview_orig)) } else { "NULL" } ","
            (self.preview_w) ","
            (self.preview_h) ","
            if let Some(media_filename) = self.media_filename.as_ref() { (QuotedData(media_filename)) } else { "NULL" } ","
            (self.media_w) ","
            (self.media_h) ","
            (self.media_size) ","
            if let Some(media_hash) = self.media_hash.as_ref() { (QuotedData(media_hash)) } else { "NULL" } ","
            if let Some(media_orig) = self.media_orig.as_ref() { (QuotedData(media_orig)) } else { "NULL" } ","
            (self.spoiler) ","
            "0,"
            (QuotedData(self.capcode.as_str())) ","
            "NULL,"
            if let Some(name) = self.name.as_ref() { (QuotedData(name)) } else { "NULL" } ","
            if let Some(trip) = self.trip.as_ref() { (QuotedData(trip)) } else { "NULL" } ","
            if let Some(title) = self.title.as_ref() { (QuotedData(title)) } else { "NULL" } ","
            if let Some(comment) = self.comment.as_ref() { (QuotedData(comment)) } else { "NULL" } ","
            "NULL,"
            (self.sticky) ","
            (self.locked) ","
            if let Some(poster_hash) = self.poster_hash.as_ref() { (QuotedData(poster_hash)) } else { "NULL" } ","
            if let Some(poster_country) = self.poster_country.as_ref() { (QuotedData(poster_country)) } else { "NULL" } ","
            if let Some(exif) = self.exif.as_ref() { (QuotedData(exif)) } else { "NULL" }

        )
    }

    pub fn timestamp_nyc(time: u64) -> u64 {
        chrono::Utc
            .timestamp(time as i64, 0)
            .with_timezone(&New_York)
            .naive_local()
            .timestamp() as u64
    }
}

use std::collections::HashMap;
#[derive(Debug, Serialize, Clone, Default)]
pub struct Exif {
    // #[serde(rename = "archivedOn", skip_serializing_if = "Option::is_none")]
    // archived_on:   Option<String>,
    #[serde(rename = "uniqueIps", skip_serializing_if = "Option::is_none")]
    unique_ips: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    since4pass: Option<String>,
    #[serde(rename = "trollCountry", skip_serializing_if = "Option::is_none")]
    troll_country: Option<String>,
    #[serde(rename = "Time", skip_serializing_if = "Option::is_none")]
    time: Option<String>,
    #[serde(rename = "Painter", skip_serializing_if = "Option::is_none")]
    painter: Option<String>,
    #[serde(rename = "Source", skip_serializing_if = "Option::is_none")]
    source: Option<String>,

    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    exif_data: HashMap<String, String>,
}

use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
static DRAW_RE: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new("<small><b>Oekaki \\s Post</b> \\s \\(Time: \\s (.*?), \\s Painter: \\s (.*?)(?:, \\s Source: \\s (.*?))?(?:, \\s Animation: \\s (.*?))?\\)</small>")
        .dot_matches_new_line(true)
        .ignore_whitespace(true)
        .build()
        .unwrap()
});
static EXIF_RE: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new("<table \\s class=\"exif\"[^>]*>(.*)</table>")
        .dot_matches_new_line(true)
        .ignore_whitespace(true)
        .build()
        .unwrap()
});
static EXIF_DATA_RE: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new("<tr><td>(.*?)</td><td>(.*?)</td></tr>")
        .dot_matches_new_line(true)
        .ignore_whitespace(true)
        .build()
        .unwrap()
});

impl Exif {
    fn parse(post: &yotsuba::Post) -> Self {
        let mut exif_data = HashMap::new();
        let mut time = None;
        let mut painter = None;
        let mut source = None;
        if let Some(text) = post.com.as_ref() {
            if let Some(exif) = EXIF_RE.captures(text.as_str()) {
                let data = exif[1].replace("<tr><td colspan=\"2\"></td></tr><tr>", "");
                for cap in EXIF_DATA_RE.captures_iter(&data) {
                    exif_data.insert(String::from(&cap[1]), String::from(&cap[2]));
                }
            }
            if let Some(draw) = DRAW_RE.captures(text.as_str()) {
                time = Some(String::from(&draw[1]));
                painter = Some(String::from(&draw[2]));
                source = draw
                    .get(3)
                    .map(|source| source.as_str().clean_full().to_string())
            }
        }

        if let Some(_extra) = post.extra.as_ref().and_then(|v| v.as_object()) {
            for (k, v) in _extra {
                if let Ok(val_str) = serde_json::to_string(v) {
                    if !val_str.is_empty() {
                        exif_data.insert(k.clone(), val_str);
                    }
                }
            }
        }

        Self {
            unique_ips: post.unique_ips.and_then(|ips| {
                if ips == 0 {
                    None
                } else {
                    Some(ips.to_string())
                }
            }),
            since4pass: post.since4pass.and_then(|year| {
                if year == 0 {
                    None
                } else {
                    Some(year.to_string())
                }
            }),
            troll_country: post.troll_country.clone(),
            time,
            painter,
            source,
            exif_data,
        }
    }

    fn is_empty(&self) -> bool {
        self.unique_ips.is_none()
            && self.since4pass.is_none()
            && self.troll_country.is_none()
            && self.time.is_none()
            && self.painter.is_none()
            && self.source.is_none()
            && self.exif_data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fomat_macros::{epintln, fomat, pintln};
    use pretty_assertions::{assert_eq, assert_ne};

    #[test]
    fn prev_dir_none() {
        let pat = Regex::new("(\\d+?)(\\d{2})\\d{0,3}$").unwrap();
        let thread = 2;
        let tstr = thread.to_string();
        let captures = pat.captures(&tstr);
        if let Some(cap) = &captures {
            let a = &cap[1];
            let b = &cap[2];
            println!("{:04?} {:02?}", a.parse::<u8>(), b.parse::<u8>());
            pintln!("a:"(a)" b: "(b));
        }
        assert_eq!(true, captures.is_none());
    }

    #[test]
    fn prev_dir_some() {
        let pat = Regex::new("(\\d+?)(\\d{2})\\d{0,3}$").unwrap();
        let thread = 6791207;
        let tstr = thread.to_string();
        let captures = pat.captures(&tstr);
        if let Some(cap) = &captures {
            let a = &cap[1];
            let b = &cap[2];
            // println!("{:04?} {:02?}", a.parse::<u8>(), b.parse::<u8>());
            assert_eq!("0067", format!("{:04}", a.parse::<u16>().unwrap()));
            assert_eq!("91", format!("{:02}", b.parse::<u8>().unwrap()));
        }
        assert_eq!(true, captures.is_some());
    }

    #[test]
    fn prev_dir_some2() {
        let pat = Regex::new("(\\d+?)(\\d{2})\\d{0,3}$").unwrap();
        let thread = 128008945;
        let tstr = thread.to_string();
        let captures = pat.captures(&tstr);
        if let Some(cap) = &captures {
            let a = &cap[1];
            let b = &cap[2];
            assert_eq!("1280", a);
            assert_eq!("08", b);
        }
        assert_eq!(true, captures.is_some());
    }

    #[test]
    fn time_nyc() {
        assert_eq!(1451954609, Post::timestamp_nyc(1451972609));
        assert_eq!(1452103812, Post::timestamp_nyc(1452121812));
    }

    #[test]
    fn test_from_trait_into() {
        let asagi_post = Post::default();
        let post = yotsuba::Post::default();

        let mut converted: Post = post.as_ref().into();
        converted.op = false;
        assert_eq!(asagi_post, converted);
    }
    #[test]
    fn test_from_trait_from() {
        let asagi_post = Post::default();
        let post = yotsuba::Post::default();

        let mut converted: Post = Post::from(&post);
        converted.op = false;
        assert_eq!(asagi_post, converted);
    }
    #[test]
    fn to_sql() {
        let post = yotsuba::Post::default();
        let mut converted: Post = Post::from(&post);
        converted.op = false;

        let converted_sql = converted.to_sql();
        let target = "0,0,0,0,false,0,0,NULL,0,0,NULL,0,0,0,NULL,NULL,false,0,\'N\',NULL,NULL,NULL,NULL,NULL,NULL,false,false,NULL,NULL,NULL";
        assert_eq!(target, &converted_sql);
    }

    #[test]
    fn post_comment_clean() {
        let mut post = yotsuba::Post::default();
        post.com = Some(r##"<a href="#p271855389" class="quotelink">>>271855389</a><br><span class="quote">>present day cowboys</span><br>No, the present day cowboys are still out there on ranches doing actual cowboy work."##.to_string());
        let mut converted: Post = Post::from(&post);
        let comment = converted.comment.as_ref().map(|com| com.as_str());
        let target = Some(">>271855389\n>present day cowboys\nNo, the present day cowboys are still out there on ranches doing actual cowboy work.");
        assert_eq!(target, comment);
    }
}

/*

            "\n("
            (post.no)
            ",0,"
            if post.resto == 0 { (post.no) } else { (post.resto) }","
            ( post.resto == 0 ) ","
            (post.time)","
            "0,"
            if let Some(tim) =  post.tim { (QuotedData(fomat!((tim)"s.jpg").as_str()))  } else { "NULL" }","
            (post.tn_w.unwrap_or_else(|| 0))","
            (post.tn_h.unwrap_or_else(|| 0))","
            if let Some(filename) =  &post.filename { (QuotedData(fomat!((filename)(post.ext.as_ref().unwrap())).as_str())) } else { "NULL" }","
            (post.w.unwrap_or_else(||0))","
            (post.h.unwrap_or_else(||0))","
            (post.fsize.unwrap_or_else(||0))","
            if let Some(md5) = &post.md5 { (QuotedData(&md5.replace("\\",""))) } else { "NULL" }","
            if let Some(tim) =  &post.tim {
                if let Some(ext) = &post.ext {
                    (QuotedData(fomat!((tim)(ext)).as_str()))
                } else {
                    "NULL"
                }
            } else { "NULL" }","
            (post.spoiler.unwrap_or_else(||0))","
            if let Some(cap) = &post.capcode { if cap=="manager"||cap=="Manager" { "'G'" } else { if let Some(c) = cap.chars().nth(0) { "'"(c.to_uppercase())"'" } else { "'N'" } } } else { "'N'" }","
            if let Some(name) = &post.name { (QuotedData(sanitize(name).as_ref())) } else { "NULL" } ","
            if let Some(trip) = &post.trip { (QuotedData(trip)) } else { "NULL" } ","
            if let Some(sub) = &post.sub { (QuotedData(sanitize(sub).as_ref())) } else { "NULL" } ","
            if post.sticky.unwrap_or_else(||0) == 1 && post.com.is_some() {
                if let Some(com) = &post.com {
                (
                    QuotedData(
                        (&ammonia::Builder::default()
                        .rm_tags(&["span", "a", "p"])
                        .clean(&com.replace("\n", "").replace("<br>", "\n").replace('\u{00ad}'.to_string().as_str(), ""))
                        .to_string()).clean_full().as_ref()
                    )
                )
                } else { "NULL" }
            } else {
                if let Some(com) = &post.com { (QuotedData(com.clean_full().as_ref())) } else { "NULL" }
            }","
            (post.sticky.unwrap_or_else(||0))","
            (post.closed.unwrap_or_else(||0) == 1 && !(post.archived.unwrap_or_else(||0) == 1))","
            if let Some(id) = &post.id { if id == "Developer" { "'Dev'" } else { (QuotedData(id)) } } else { "NULL" }","
            if let Some(country) = &post.country { if country == "XX" || country == "A1" { "NULL" } else { (QuotedData(country)) } } else { "NULL" } ","

                // This is a huge if-else-branch because I don't wan't to allocate a new Map
                // for each post just to find out there's nothing to insert
                //
                // Go here if theres extra keys
                if let Some(extra_json) = post.extra.as_ref() {
                    // Add to `exif`
                    (
                        if post.unique_ips.is_some() || post.since4pass.is_some() || post.archived_on.is_some() {
                            let mut extra_json_mut = extra_json.clone();
                            let mut _exif = extra_json_mut.as_object_mut().unwrap();
                            if let Some(unique_ips) = post.unique_ips {
                                _exif.insert(String::from("uniqueIps"), unique_ips.into());
                            }
                            if let Some(since4pass) = post.since4pass {
                                _exif.insert(String::from("since4pass"), since4pass.into());
                            }
                            if let Some(troll_country) = &post.troll_country {
                                _exif.insert(String::from("trollCountry"), troll_country.as_str().into());
                            }
                            if let Some(archived_on) = &post.archived_on {
                                _exif.insert(String::from("archivedOn"), (*archived_on).into());
                            }
                            let extra_string:String = serde_json::to_string(&extra_json_mut).unwrap();
                            let s = QuotedData(extra_string.as_str()).to_string();
                            extra_tmp.clear();
                            extra_tmp.push_str(&s);
                            &extra_tmp
                        } else { &_null }
                    )
                } else {
                    (
                        // Go here if there's no extra keys
                        if post.unique_ips.is_some() || post.since4pass.is_some() || post.archived_on.is_some() {
                            let mut _exif = serde_json::Map::new();
                            if let Some(unique_ips) = post.unique_ips {
                                _exif.insert(String::from("uniqueIps"), unique_ips.into());
                            }
                            if let Some(since4pass) = post.since4pass {
                                _exif.insert(String::from("since4pass"), since4pass.into());
                            }
                            if let Some(troll_country) = &post.troll_country {
                                _exif.insert(String::from("trollCountry"), troll_country.as_str().into());
                            }
                            if let Some(archived_on) = &post.archived_on {
                                _exif.insert(String::from("archivedOn"), (*archived_on).into());
                            }
                            let extra_string:String = serde_json::to_string(&_exif).unwrap();
                            let s = QuotedData(extra_string.as_str()).to_string();
                            extra_tmp.clear();
                            extra_tmp.push_str(&s);
                            &extra_tmp
                        } else { &_null }
                    )
                }

*/
