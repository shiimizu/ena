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
                // This does not look recursively in objects
                for (k, v) in obj.iter() {
                    if let Some(_v) = v.as_str() {
                        _v.hash(state);
                    } else if let Some(_v) = v.as_bool() {
                        _v.hash(state);
                    } else if let Some(_v) = v.as_u64() {
                        _v.hash(state);
                    } else if let Some(_v) = v.as_i64() {
                        _v.hash(state);
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

    #[rustfmt::skip]
    #[test]
    fn update_post() {
        //[{},{},{}]
        let mut json: serde_json::Value = serde_json::from_str(r##"{"posts":[{"no":570368,"sticky":1,"closed":1,"now":"12/31/18(Mon)17:05:48","name":"Anonymous","sub":"Welcome to /po/!","com":"Welcome to /po/! We specialize in origami, papercraft, and everything that’s relevant to paper engineering. This board is also an great library of relevant PDF books and instructions, one of the best resource of its kind on the internet.<br><br>Questions and discussions of papercraft and origami are welcome. Threads for topics covered by paper engineering in general are also welcome, such as kirigami, bookbinding, printing technology, sticker making, gift boxes, greeting cards, and more.<br><br>Requesting is permitted, even encouraged if it’s a good request; fulfilled requests strengthens this board’s role as a repository of books and instructions. However do try to keep requests in relevant threads, if you can.<br><br>/po/ is a slow board! Do not needlessly bump threads.","filename":"yotsuba_folding","ext":".png","w":530,"h":449,"tn_w":250,"tn_h":211,"tim":1546293948883,"time":1546293948,"md5":"uZUeZeB14FVR+Mc2ScHvVA==","fsize":516657,"resto":0,"capcode":"mod","semantic_url":"welcome-to-po","replies":2,"rawr":2,"another":"test","tail_size":50,"tail_id":143219876,"images":2,"unique_ips":1},{"no":570370,"now":"12/31/18(Mon)17:14:56","name":"Anonymous","com":"<b>FAQs about papercraft</b><br>\n<br>\n<i>What paper should I use?</i><br>\n<br>\nSmall models can be made with light 100 to 150 gsm paper, while large ones are better with heavy 150 to 200+ gsm paper.<br>\n<br>\n<i>Where do I begin with papercraft can I find easy papercrafts?</i><br>\n<br>\nPapercraft also requires glue, and cutting tools. A PVA glue stick is works. A pen knife and cutting board is recommended, but otherwise scissors are okay for simple models.<br>\n<br>\nPapercraft normally involves printing and cutting out a number of nets, and and gluing tabs and pieces where appropriate to form a model.<br>\n<br>\nYou can find a variety of papercraft models on this board that may interest you. Ask for some otherwise, and be specific about what you would like. You can search online for ‘easy papercraft templates’, these links have many.<br>\n<br>\n<a href=\"http://papercraft.wikidot.com/papercraft\">http://papercraft.wikidot.com/paper<wbr>craft</a><br>\n<a href=\"http://cp.c-ij.com/en/categories/CAT-ST01-0071/top.html\">http://cp.c-ij.com/en/categories/CA<wbr>T-ST01-0071/top.html</a><br>\n<br>\n<i>What is Pepakura?</i><br>\n<br>\nPepakura Designer is a program that takes 3D models and `unfolds&#039; them to papercraft templates. Using Pepakura in conjunction with a 3D modelling software, such as Blender, you can design your own papercraft models.<br>\n<br>\n<a href=\"https://elementcrafts.wordpress.com/2014/04/22/a-complete-beginners-guide-to-papercraft-pepakura-windows-only/\">https://elementcrafts.wordpress.com<wbr>/2014/04/22/a-complete-beginners-gu<wbr>ide-to-papercraft-pepakura-windows-<wbr>only/</a><br>\n<br>\n<i>Hints and tips?</i>\n<br>\nGlue accurately for a model to hold well, and practice plenty.<br>\n<br>\n<a href=\"http://www.papercraftmuseum.com/advanced-tutorial/\">http://www.papercraftmuseum.com/adv<wbr>anced-tutorial/</a><br>","filename":"papercraft faq","ext":".png","w":318,"h":704,"tn_w":56,"tn_h":125,"tim":1546294496751,"time":1546294496,"md5":"0EqXBb4gGIyzQiaApMdFAA==","fsize":285358,"resto":570368,"what":2,"capcode":"mod"},{"no":570371,"now":"12/31/18(Mon)17:21:29","name":"Anonymous","com":"<b>FAQs about origami</b><br>\n<br>\n<i>Where do I begin with origami and how can I find easy models?</i><br>\n<br>\nTry browsing the board for guides, or other online resources listed below, for models you like and practice folding them.<br>\n<br>\nA great way to begin at origami is to participate in the Let’s Fold Together threads <a href=\"https://boards.4channel.org/po/catalog#s=lft\"><a href=\"//boards.4channel.org/po/catalog#s=lft\" class=\"quotelink\">&gt;&gt;&gt;/po/lft</a></a> - open up the PDF file and find a model you like, work on it, and discuss or post results.<br>\n<br>\n<a href=\"http://en.origami-club.com\">http://en.origami-club.com</a><br>\n<a href=\"https://origami.me/diagrams/\">https://origami.me/diagrams/</a><br>\n<a href=\"https://www.origami-resource-center.com/free-origami-instructions.html\">https://www.origami-resource-center<wbr>.com/free-origami-instructions.html<wbr></a><br>\n<a href=\"http://www.paperfolding.com/diagrams/\">http://www.paperfolding.com/diagram<wbr>s/</a><br>\n<br>\n<i>What paper should I use?</i><br>\n<br>\nIt depends on the model; for smaller models which involved 25 steps or fewer, 15 by 15 cm origami paper from a local craft store will be suitable. For larger models you will need larger or thinner paper, possibly from online shops. Boxpleated models require thin paper, such as sketching paper. Wet folded models require thicker paper, such as elephant hide.<br>\n<br>\n<a href=\"https://www.origami-shop.com/en/\">https://www.origami-shop.com/en/</a><br>\n<br>\n<i>Hints and tips?</i><br>\n<br>\nFor folding, The best advice is to always fold as cleanly as possible, and take your time. Everything else comes with experience.<br>\n<br>\n<a href=\"https://origami.me/beginners-guide/\">https://origami.me/beginners-guide/<wbr></a><br>\n<a href=\"https://origamiusa.org/glossary\">https://origamiusa.org/glossary</a><br>\n<br>\n<i>What are ‘CPs’?</i><br>\n<br>\nCrease patterns are a structural representations of origami models, shown as a schematic of lines; they are essentially origami models unfolded and laid flat. Lines on a crease pattern may be indicated by ‘mountain’ or ‘valley’ folds to show how the folds alternate. If you’re particularly skilled at origami, they become useful instructions for building models. A common base fold is usually discernable, all the intermediate details can be worked on from there.<br>\n<br>\n<a href=\"https://blog.giladnaor.com/2008/08/folding-from-crease-patterns.html\">https://blog.giladnaor.com/2008/08/<wbr>folding-from-crease-patterns.html</a><br>\n<a href=\"http://www.origamiaustria.at/articles.php?lang=2#a4\">http://www.origamiaustria.at/articl<wbr>es.php?lang=2#a4</a><br>","filename":"origami faq","ext":".jpg","w":762,"h":762,"tn_w":125,"tn_h":125,"tim":1546294889019,"time":1546294889,"md5":"vKWr7+oITdUBu7bUaypuCw==","fsize":163110,"resto":570368,"capcode":"mod"}]}"##).unwrap();
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
