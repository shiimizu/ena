use super::clean::*;
use crate::yotsuba;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use fomat_macros::fomat;
use format_sql_query::QuotedData;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(default)]
pub struct Post {
    doc_id:            u64,
    media_id:          u64,
    poster_ip:         f64,
    num:               u64,
    subnum:            u64,
    thread_num:        u64,
    op:                bool,
    timestamp:         u64,
    timestamp_expired: u64,
    preview_orig:      Option<String>,
    preview_w:         u32,
    preview_h:         u32,
    media_filename:    Option<String>,
    media_w:           u32,
    media_h:           u32,
    media_size:        u32,
    media_hash:        Option<String>,
    media_orig:        Option<String>,
    spoiler:           bool,
    deleted:           bool,
    capcode:           String,
    email:             Option<String>,
    name:              Option<String>,
    trip:              Option<String>,
    title:             Option<String>,
    comment:           Option<String>,
    delpass:           Option<String>,
    sticky:            bool,
    locked:            bool,
    poster_hash:       Option<String>,
    poster_country:    Option<String>,
    exif:              Option<String>,
}

impl Eq for Post {}

impl Default for Post {
    fn default() -> Self {
        Self {
            doc_id:            0,
            media_id:          0,
            poster_ip:         0.0,
            num:               0,
            subnum:            0,
            thread_num:        0,
            op:                false,
            timestamp:         0,
            timestamp_expired: 0,
            preview_orig:      None,
            preview_w:         0,
            preview_h:         0,
            media_filename:    None,
            media_w:           0,
            media_h:           0,
            media_size:        0,
            media_hash:        None,
            media_orig:        None,
            spoiler:           false,
            deleted:           false,
            capcode:           "N".into(),
            email:             None,
            name:              None,
            trip:              None,
            title:             None,
            comment:           None,
            delpass:           None,
            sticky:            false,
            locked:            false,
            poster_hash:       None,
            poster_country:    None,
            exif:              None,
        }
    }
}

impl From<&yotsuba::Post> for Post {
    /// Convert a 4chan `Post` to an Asagi `Post`
    ///
    /// 4chan sometimes has a `\` character in their `md5`.
    fn from(post: &yotsuba::Post) -> Self {
        Self {
            doc_id:            0,
            media_id:          0,
            poster_ip:         0.0,
            num:               post.no,
            subnum:            0,
            thread_num:        if post.resto == 0 { post.no } else { post.resto },
            op:                (post.resto == 0),
            timestamp:         if post.time == 0 { 0 } else { Self::timestamp_nyc(post.time) },
            timestamp_expired: 0,
            preview_orig:      post.tim.map(|tim| fomat!((tim)"s.jpg")),
            preview_w:         post.tn_w.unwrap_or_default(),
            preview_h:         post.tn_h.unwrap_or_default(),
            media_filename:    if let Some(filename) = post.filename.as_ref() {
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
            media_w:           post.w.unwrap_or_default(),
            media_h:           post.h.unwrap_or_default(),
            media_size:        post.fsize.unwrap_or_default(),
            media_hash:        post.md5.as_ref().map(|md5| md5.replace("\\", "")),
            media_orig:        if let Some(tim) = post.tim {
                if let Some(ext) = post.ext.as_ref() {
                    Some(fomat!((tim)(ext)))
                } else {
                    Some(fomat!((tim)))
                }
            } else {
                None
            },
            spoiler:           post.spoiler.map_or_else(|| false, |v| v == 1),
            deleted:           false,
            capcode:           {
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
            email:             None,
            name:              post.name.as_ref().map(|s| s.as_str().clean().trim().to_string()),
            trip:              post.trip.clone(),
            title:             post.sub.as_ref().map(|s| s.as_str().clean().trim().to_string()),
            comment:           if post.sticky.unwrap_or_else(|| 0) == 1 && post.com.is_some() {
                let com = post.com.as_ref().unwrap();
                if !com.is_empty() {
                    let s = (&ammonia::Builder::default()
                        .rm_tags(&["span", "a", "p"])
                        .clean(&com.replace("\r", "").replace("\n", "").replace("<br>", "\n").replace('\u{00ad}'.to_string().as_str(), ""))
                        .to_string())
                        .as_str()
                        .clean_full()
                        .to_string();
                    Some(s)
                } else {
                    None
                }
            } else {
                post.com.as_ref().map(|s| s.as_str().clean_full().to_string())
            },
            delpass:           None,
            sticky:            post.sticky.map_or_else(|| false, |v| v == 1),
            locked:            post.closed.map_or_else(|| false, |v| v == 1) && !post.archived.map_or_else(|| false, |v| v == 1),
            poster_hash:       post.id.as_ref().map(|s| if s == "Developer" { "Dev".into() } else { s.clone() }),
            poster_country:    post.country.as_ref().filter(|&v| !(v == "XX" || v == "A1")).map(|s| s.into()),
            exif:              {
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
            "("
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
            ")"
        )
    }

    pub fn timestamp_nyc(time: u64) -> u64 {
        let dt = Utc.timestamp(time as i64, 0);
        let ts = dt.with_timezone(&New_York).naive_local().timestamp();
        ts as u64
    }
}

use std::collections::HashMap;
#[derive(Debug, Serialize, Clone, Default)]
pub struct Exif {
    #[serde(rename = "archivedOn", skip_serializing_if = "Option::is_none")]
    archived_on:   Option<String>,
    #[serde(rename = "uniqueIps", skip_serializing_if = "Option::is_none")]
    unique_ips:    Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    since4pass:    Option<String>,
    #[serde(rename = "trollCountry", skip_serializing_if = "Option::is_none")]
    troll_country: Option<String>,
    #[serde(rename = "Time", skip_serializing_if = "Option::is_none")]
    time:          Option<String>,
    #[serde(rename = "Painter", skip_serializing_if = "Option::is_none")]
    painter:       Option<String>,
    #[serde(rename = "Source", skip_serializing_if = "Option::is_none")]
    source:        Option<String>,

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
static EXIF_RE: Lazy<Regex> = Lazy::new(|| RegexBuilder::new("<table \\s class=\"exif\"[^>]*>(.*)</table>").dot_matches_new_line(true).ignore_whitespace(true).build().unwrap());
static EXIF_DATA_RE: Lazy<Regex> = Lazy::new(|| RegexBuilder::new("<tr><td>(.*?)</td><td>(.*?)</td></tr>").dot_matches_new_line(true).ignore_whitespace(true).build().unwrap());

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
                source = draw.get(3).map(|source| source.as_str().clean_full().to_string())
            }
        }

        if let Some(_extra) = post.extra.as_ref().and_then(|v| v.as_object()) {
            for (k, v) in _extra {
                if let Ok(val_str) = serde_json::to_string(v) {
                    exif_data.insert(k.clone(), val_str);
                }
            }
        }

        Self {
            archived_on: post.archived_on.map(|timestamp| timestamp.to_string()),
            unique_ips: post.unique_ips.and_then(|ips| if ips == 0 { None } else { Some(ips.to_string()) }),
            since4pass: post.since4pass.and_then(|year| if year == 0 { None } else { Some(year.to_string()) }),
            troll_country: post.troll_country.clone(),
            time,
            painter,
            source,
            exif_data,
        }
    }

    fn is_empty(&self) -> bool {
        self.archived_on.is_none()
            && self.unique_ips.is_none()
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
        let target = "(0,0,0,0,false,0,0,NULL,0,0,NULL,0,0,0,NULL,NULL,false,0,\'N\',NULL,NULL,NULL,NULL,NULL,NULL,false,false,NULL,NULL,NULL)";
        assert_eq!(target, &converted_sql);
    }
    
    #[test]
    fn post_comment_clean() {
        let mut post = yotsuba::Post::default();
        post.com = Some(r##"<a href="#p271855389" class="quotelink">>>271855389</a><br><span class="quote">>present day cowboys</span><br>No, the present day cowboys are still out there on ranches doing actual cowboy work."##.to_string());
        let mut converted: Post = Post::from(&post);
        let comment  = converted.comment.as_ref().map(|com| com.as_str());
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
