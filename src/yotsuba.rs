#![allow(unused_imports)]

use fomat_macros::fomat;
use format_sql_query::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

/// 4chan post with extra fields stored in `extra`
#[allow(dead_code)]
#[cold]
#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Default)]
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
    pub fsize:          Option<u32>,
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
    pub since4pass:     Option<u16>,
    pub extra:          Option<serde_json::Value>,
}

impl AsRef<Post> for Post {
    fn as_ref(&self) -> &Post {
        self
    }
}

impl Hash for Post {
    /// Account for `extra`
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.no.hash(state);
        self.sticky.hash(state);
        self.closed.hash(state);
        self.now.hash(state);
        self.name.hash(state);
        self.sub.hash(state);
        self.com.hash(state);
        self.filedeleted.hash(state);
        self.spoiler.hash(state);
        self.custom_spoiler.hash(state);
        self.filename.hash(state);
        self.ext.hash(state);
        self.w.hash(state);
        self.h.hash(state);
        self.tn_w.hash(state);
        self.tn_h.hash(state);
        self.tim.hash(state);
        self.time.hash(state);
        self.md5.hash(state);
        self.fsize.hash(state);
        self.m_img.hash(state);
        self.resto.hash(state);
        self.trip.hash(state);
        self.id.hash(state);
        self.capcode.hash(state);
        self.country.hash(state);
        self.troll_country.hash(state);
        self.country_name.hash(state);
        self.archived.hash(state);
        self.bumplimit.hash(state);
        self.archived_on.hash(state);
        self.imagelimit.hash(state);
        self.semantic_url.hash(state);
        self.replies.hash(state);
        self.images.hash(state);
        self.unique_ips.hash(state);
        self.tag.hash(state);
        self.since4pass.hash(state);
        if let Some(extra) = &self.extra {
            if let Some(obj) = extra.as_object() {
                for (k, v) in obj.iter() {
                    if let Some(_v) = v.as_str() {
                        _v.hash(state);
                        break;
                    }
                    if let Some(_v) = v.as_bool() {
                        _v.hash(state);
                        break;
                    }
                    if let Some(_v) = v.as_u64() {
                        _v.hash(state);
                        break;
                    }
                    if let Some(_v) = v.as_i64() {
                        _v.hash(state);
                        break;
                    }
                }
            }
        }
    }
}

pub fn update_post_with_extra(json: &mut serde_json::Value) {
    let post_default_json = serde_json::to_value(Post::default()).unwrap();
    let orig_keys: &Vec<&String> = &post_default_json.as_object().unwrap().keys().filter(|&s| s.as_str() != "extra").collect();
    /*let json_thread = json["posts"].as_array_mut().unwrap();
    let _ = json_thread.iter_mut().map(serde_json::value::Value::as_object_mut).flat_map(|mo| mo.map(|mut post| {

        let mut extra_map = serde_json::Map::new();

        let mut posts_with_extra_keys : &mut Vec<&String> = &mut(&mut post).iter_mut().filter(|(key, _)| !(key.as_str() == "tail_size" || key.as_str() == "tail_id" || orig_keys.iter().any(|key_orig| key == key_orig)))
        .map(|(k, v)| {
            // could be the culprit for using such large memory
            extra_map.insert(k.clone(), v.clone());
            k
        }).collect();

        // Somehow collecting it to a &mut Vec<&String> and clearing it works...
        posts_with_extra_keys.clear();

        // How the heck does this work if we already have a mutable reference
        post.insert("extra".into(), if extra_map.len() > 0 { serde_json::to_value(extra_map).unwrap() } else { serde_json::Value::Null });
    }));*/

    let mut posts = json["posts"].as_array_mut().unwrap().iter_mut();

    // let post_default_json = serde_json::to_value(yotsuba::Post::default()).unwrap();
    // let orig_keys: Vec<&String> = post_default_json.as_object().unwrap().keys().filter(|&s|
    // s.as_str() != "extra").collect();
    for post_j in posts {
        let mut extra_map = serde_json::Map::new();

        // get mut ref
        let mut post = post_j.as_object_mut().unwrap();
        let mut posts_with_extra_keys: &mut Vec<&String> = &mut post
            .iter_mut()
            .filter(|(key, _)| !(key.as_str() == "tail_size" || key.as_str() == "tail_id" || orig_keys.iter().any(|key_orig| key == key_orig)))
            .map(|(k, v)| {
                // could be the culprit for using such large memory
                extra_map.insert(k.clone(), v.clone());
                k
            })
            .collect();

        // Somehow collecting it to a &mut Vec<&String> and clearing it works...
        posts_with_extra_keys.clear();

        // How the heck does this work if we already have a mutable reference
        // post.insert("extra".into(), if extra_map.len() > 0 { serde_json::to_value(extra_map).unwrap() }
        // else { serde_json::Value::Null });
        if !extra_map.is_empty() {
            post.insert("extra".into(), serde_json::Value::Object(extra_map));
        }
    }

    // json
}

#[cfg(test)]
mod tests {
    use super::Post;
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use std::{
        collections::{HashMap, HashSet},
        hash::Hash,
    };
    #[derive(Clone, Debug, Copy, Eq, PartialEq, Hash)]
    struct ThreadEntry {
        no:            u64,
        last_modified: u64,
        replies:       u16,
    }

    #[allow(dead_code)]
    #[cold]
    #[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
    struct Thread {
        posts: Vec<Post>,
    }

    #[rustfmt::skip]
    #[test]
    fn threads_modified() {
        let a = get_set_a();
        let b = get_set_b();
        let opp: HashSet<_> = a.symmetric_difference(&b).unique_by(|s| s.no).collect();

        let res = vec![ThreadEntry { no: 8888, last_modified: 1576266850, replies: 101 }, ThreadEntry { no: 196656536, last_modified: 1576266853, replies: 5 }, ThreadEntry { no: 196621039, last_modified: 1576266853, replies: 189 }, ThreadEntry { no: 196621031, last_modified: 1576266853, replies: 189 }];

        assert_eq!(opp, res.iter().to_owned().collect());
    }

    #[rustfmt::skip]
    #[test]
    fn threads_combined() {
        let a = get_set_a();
        let b = get_set_b();
        let opp: HashSet<_> = a.union(&b).unique_by(|s| s.no).collect();

        let res = vec![ThreadEntry { no: 196637792, last_modified: 1576266880, replies: 233 }, ThreadEntry { no: 196652782, last_modified: 1576266858, replies: 42 }, ThreadEntry { no: 196621039, last_modified: 1576266853, replies: 189 }, ThreadEntry { no: 196655998, last_modified: 1576266860, replies: 5 }, ThreadEntry { no: 196656097, last_modified: 1576266868, replies: 7 }, ThreadEntry { no: 196649146, last_modified: 1576266882, replies: 349 }, ThreadEntry { no: 196655995, last_modified: 1576266867, replies: 3 }, ThreadEntry { no: 196656536, last_modified: 1576266853, replies: 6 }, ThreadEntry { no: 196640441, last_modified: 1576266851, replies: 495 }, ThreadEntry { no: 196647457, last_modified: 1576266880, replies: 110 }, ThreadEntry { no: 196621031, last_modified: 1576266853, replies: 189 }, ThreadEntry { no: 196637247, last_modified: 1576266850, replies: 101 }, ThreadEntry { no: 196645355, last_modified: 1576266866, replies: 361 }, ThreadEntry { no: 196624742, last_modified: 1576266873, replies: 103 }, ThreadEntry { no: 196654076, last_modified: 1576266880, replies: 191 }, ThreadEntry { no: 196656555, last_modified: 1576266881, replies: 6 }, ThreadEntry { no: 8888, last_modified: 1576266850, replies: 101 }];

        assert_eq!(opp, res.iter().to_owned().collect());
    }

    #[test]
    fn update_post() {
        let mut json: serde_json::Value = serde_json::from_str(include_str!("570368.json")).unwrap(); //[{},{},{}]
                                                                                                      // println!("{}", serde_json::to_string_pretty(&json["posts"][0]).unwrap());
        super::update_post_with_extra(&mut json);
        // println!("{}", serde_json::to_string_pretty(&json["posts"][0]).unwrap());

        let posts_arr = json["posts"].as_array().unwrap();
        assert_eq!(posts_arr[0]["extra"], serde_json::json!({"rawr":2, "another":"test"}));
        assert_eq!(posts_arr[1]["extra"], serde_json::json!({"what":2}));
        assert_eq!(posts_arr[2]["extra"], serde_json::Value::Null);
        // assert_eq!(2, 1);
    }

    fn get_set_a() -> HashSet<ThreadEntry> {
        // Also Skip archived, deleted, and duplicate threads
        let mut map = HashMap::new();
        let v = vec![
            ThreadEntry { no: 196649146, last_modified: 1576266882, replies: 349 },
            ThreadEntry { no: 196656555, last_modified: 1576266881, replies: 6 },
            ThreadEntry { no: 196654076, last_modified: 1576266880, replies: 191 },
            ThreadEntry { no: 196637792, last_modified: 1576266880, replies: 233 },
            ThreadEntry { no: 196647457, last_modified: 1576266880, replies: 110 },
            ThreadEntry { no: 196624742, last_modified: 1576266873, replies: 103 },
            ThreadEntry { no: 196656097, last_modified: 1576266868, replies: 7 },
            ThreadEntry { no: 196645355, last_modified: 1576266866, replies: 361 },
            ThreadEntry { no: 196655995, last_modified: 1576266867, replies: 3 },
            ThreadEntry { no: 196655998, last_modified: 1576266860, replies: 5 },
            ThreadEntry { no: 196652782, last_modified: 1576266858, replies: 42 },
            ThreadEntry { no: 196656536, last_modified: 1576266853, replies: 5 },
            ThreadEntry { no: 196621039, last_modified: 1576266853, replies: 189 },
            ThreadEntry { no: 196640441, last_modified: 1576266851, replies: 495 },
            ThreadEntry { no: 196637247, last_modified: 1576266850, replies: 101 },
        ];
        map.insert("threads".to_string(), v);
        let a: HashSet<_> = map.get("threads").unwrap().iter().cloned().collect();
        a
    }

    fn get_set_b() -> HashSet<ThreadEntry> {
        let mut map = HashMap::new();
        let v = vec![
            ThreadEntry { no: 196649146, last_modified: 1576266882, replies: 349 },
            ThreadEntry { no: 196656555, last_modified: 1576266881, replies: 6 },
            ThreadEntry { no: 196654076, last_modified: 1576266880, replies: 191 },
            ThreadEntry { no: 196637792, last_modified: 1576266880, replies: 233 },
            ThreadEntry { no: 196647457, last_modified: 1576266880, replies: 110 },
            ThreadEntry { no: 196624742, last_modified: 1576266873, replies: 103 },
            ThreadEntry { no: 196656097, last_modified: 1576266868, replies: 7 },
            ThreadEntry { no: 196645355, last_modified: 1576266866, replies: 361 },
            ThreadEntry { no: 196655995, last_modified: 1576266867, replies: 3 },
            ThreadEntry { no: 196655998, last_modified: 1576266860, replies: 5 },
            ThreadEntry { no: 196652782, last_modified: 1576266858, replies: 42 },
            ThreadEntry { no: 196656536, last_modified: 1576266853, replies: 6 },
            ThreadEntry { no: 196621031, last_modified: 1576266853, replies: 189 },
            ThreadEntry { no: 196640441, last_modified: 1576266851, replies: 495 },
            ThreadEntry { no: 196637247, last_modified: 1576266850, replies: 101 },
            ThreadEntry { no: 8888, last_modified: 1576266850, replies: 101 },
        ];
        map.insert("threads".to_string(), v);
        let a: HashSet<_> = map.get("threads").unwrap().iter().cloned().collect();
        a
    }
}
