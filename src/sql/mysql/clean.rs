use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::{Borrow, Cow};

/// Trait to clean comments
pub trait Clean {
    fn clean(&self) -> Cow<str>;
    fn clean_full(&self) -> Cow<str>;
}

impl Clean for &str {
    fn clean(&self) -> Cow<str> {
        html_escape::decode_html_entities(self)
    }

    // https://stackoverflow.com/a/49588741
    // https://stackoverflow.com/a/55670581
    fn clean_full(&self) -> Cow<str> {
        // This doesn't work
        // let mut s = Cow::from(*self);
        // for (re, to) in &*REPLACEMENTS {
        //     s = (*re).replace_all::<&str>(s.as_ref(), to);
        // }
        // return s;

        // So I have to do this
        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(0) };
        let s = (*re).replace_all::<&str>(self, to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(1) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(2) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(3) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(4) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(5) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(6) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(7) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(8) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(9) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(10) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(11) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(12) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(13) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(14) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(15) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(16) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(17) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(18) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(19) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(20) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(21) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let (re, to) = unsafe { REPLACEMENTS.get_unchecked(22) };
        let s = (*re).replace_all::<&str>(s.as_ref(), to);

        let s = s.as_ref();
        let s = s.clean();

        Cow::from(s.into_owned())
    }
}

static REPLACEMENTS: Lazy<[(Regex, &str); 23]> = Lazy::new(|| {
    [
        // Admin-Mod-Dev quotelinks
        (Regex::new("<span class=\"capcodeReplies\"><span style=\"font-size: smaller;\"><span style=\"font-weight: bold;\">(?:Administrator|Moderator|Developer) Repl(?:y|ies):</span>.*?</span><br></span>").unwrap(), ""),
        // Non-public tags
        (Regex::new("\\[(/?(banned|moot|spoiler|code))]").unwrap(), "[$1:lit]"),
        // Comment too long, also EXIF tag toggle
        (Regex::new("<span class=\"abbr\">.*?</span>").unwrap(), ""),
        // EXIF data
        (Regex::new("<table class=\"exif\"[^>]*>.*?</table>").unwrap(), ""),
        // DRAW data
        (Regex::new("<br><br><small><b>Oekaki Post</b>.*?</small>").unwrap(), ""),
        // Banned/Warned text
        (Regex::new("<(?:b|strong) style=\"color:\\s*red;\">(.*?)</(?:b|strong)>").unwrap(), "[banned]$1[/banned]"),
        // moot text
        (Regex::new("<div style=\"padding: 5px;margin-left: \\.5em;border-color: #faa;border: 2px dashed rgba\\(255,0,0,\\.1\\),border-radius: 2px\">(.*?)</div>").unwrap(), "[moot]$1[/moot]"),
        // fortune text
        (Regex::new("<span class=\"fortune\" style=\"color:(.*?)\"><br><br><b>(.*?)</b></span>").unwrap(), "\n\n[fortune color=\"$1\"]$2[/fortune]"),
        // bold text
        (Regex::new("<(?:b|strong)>(.*?)</(?:b|strong)>").unwrap(), "[b]$1[/b]"),
        // code tags
        (Regex::new("<pre[^>]*>").unwrap(), "[code]"),
        (Regex::new("</pre>").unwrap(), "[/code]"),
        // math tags
        (Regex::new("<span class=\"math\">(.*?)</span>").unwrap(), "[math]$1[/math]"),
        (Regex::new("<div class=\"math\">(.*?)</div>").unwrap(), "[eqn]$1[/eqn]"),
        // > implying I'm quoting someone
        (Regex::new("<font class=\"unkfunc\">(.*?)</font>").unwrap(), "$1"),
        (Regex::new("<span class=\"quote\">(.*?)</span>").unwrap(), "$1"),
        (Regex::new("<span class=\"(?:[^\"]*)?deadlink\">(.*?)</span>").unwrap(), "$1"),
        // Links
        (Regex::new("<a[^>]*>(.*?)</a>").unwrap(), "$1"),
        // old spoilers
        (Regex::new("<span class=\"spoiler\"[^>]*>(.*?)</span>").unwrap(), "[spoiler]$1[/spoiler]"),
        // ShiftJIS
        (Regex::new("<span class=\"sjis\">(.*?)</span>").unwrap(), "[shiftjis]$1[/shiftjis]"),
        // new spoilers
        (Regex::new("<s>").unwrap(), "[spoiler]"),
        (Regex::new("</s>").unwrap(), "[/spoiler]"),
        // new line/wbr
        (Regex::new("<br\\s*/?>").unwrap(), "\n"),
        (Regex::new("<wbr>").unwrap(), ""),
    ]
});

#[cfg(test)]
mod tests {
    use super::*;
    use fomat_macros::{epintln, fomat, pintln};
    use pretty_assertions::{assert_eq, assert_ne};

    #[test]
    fn g_sticky() {
        let com = "This board is for the discussion of technology and related topics.<br> <br> \nReminder that instigating OR participating in flame/brand wars will result in a ban.<br>\nTech support threads should be posted to <a href=\"/wsr/\" class=\"quotelink\"><a href=\"//boards.4channel.org/wsr/\" class=\"quotelink\">&gt;&gt;&gt;/wsr/</a></a><br> \nCryptocurrency discussion belongs on <a href=\"/biz/\" class=\"quotelink\"><a href=\"//boards.4channel.org/biz/\" class=\"quotelink\">&gt;&gt;&gt;/biz/</a></a><br> <br> To use the Code tag, book-end your body of code with: [co\u{00ad}de] and [/co\u{00ad}de]<br> <br> The /g/ Wiki: <a href=\"https://wiki.installgentoo.com/\">https://wiki.installgentoo.com/</a>".to_string();
        let s =
            (&ammonia::Builder::default().rm_tags(&["span", "a", "p"]).clean(&com.replace("\r", "").replace("\n", "").replace("<br>", "\n").replace('\u{00ad}'.to_string().as_str(), "")).to_string())
                .as_str()
                .clean_full()
                .to_string();
        let target = "This board is for the discussion of technology and related topics.\n \n Reminder that instigating OR participating in flame/brand wars will result in a ban.\nTech support threads should be posted to >>>/wsr/\n Cryptocurrency discussion belongs on >>>/biz/\n \n To use the Code tag, book-end your body of code with: [code:lit] and [/code:lit]\n \n The /g/ Wiki: https://wiki.installgentoo.com/";
        pintln!([s]);
        assert_eq!(&s, target);
    }

    #[test]
    fn x_sticky() {
        let com = "Welcome to /x/ - Paranormal. This is not a board for the faint of heart. If you need something to get started with, see the below lists for some basic resources. We hope you enjoy your venture into the spooks, the creeps and the unknown.\r\n<br>\r\n<br>\r\nThe resources in this thread are not exhaustive and are merely meant for beginners to get their footing.\r\n<br>\r\n<br>\r\n<img src=\"//s.4cdn.org/image/temp/danger.gif\" alt=\"\">".to_string();
        let s =
            (&ammonia::Builder::default().rm_tags(&["span", "a", "p"]).clean(&com.replace("\r", "").replace("\n", "").replace("<br>", "\n").replace('\u{00ad}'.to_string().as_str(), "")).to_string())
                .as_str()
                .clean_full()
                .to_string();
        let target = "Welcome to /x/ - Paranormal. This is not a board for the faint of heart. If you need something to get started with, see the below lists for some basic resources. We hope you enjoy your venture into the spooks, the creeps and the unknown.\n\nThe resources in this thread are not exhaustive and are merely meant for beginners to get their footing.\n\n<img src=\"//s.4cdn.org/image/temp/danger.gif\" alt=\"\">";
        pintln!([s]);
        assert_eq!(&s, target);
    }

    #[test]
    fn vrpg_sticky() {
        let com = "/vrpg/ is a place to discuss all types of role-playing video games, including single-player, multi-player, and massively multi-player, turn-based and real-time action, western-style and JRPG.<br>\n<br>\nDoes this mean RPGs are banned on other video game boards? <span style=\"font-size:15px;font-weight:bold;\">No!</span> /vrpg/ is just a separate board specifically focused on RPGs where discussions about your favorite games can thrive.<br>\n<br>\nPlease familiarize yourself with <a href=\"https://www.4channel.org/rules#vrpg\">the rules</a> and remember to <a href=\"https://www.4channel.org/faq#spoiler\">use the spoiler function where appropriate</a>!<br>\n<p style=\"font-size:15px;font-weight:bold;\">Please note that, like /v/, &quot;Generals&quot;\u{2014}long-term, one-after-the-other, recurring threads about a specific game are not permitted on /vrpg/. Such threads belong on <a href=\"https://boards.4channel.org/vg/\"><a href=\"//boards.4channel.org/vg/\" class=\"quotelink\">&gt;&gt;&gt;/vg/</a></a>.</p>".to_string();
        let s =
            (&ammonia::Builder::default().rm_tags(&["span", "a", "p"]).clean(&com.replace("\r", "").replace("\n", "").replace("<br>", "\n").replace('\u{00ad}'.to_string().as_str(), "")).to_string())
                .as_str()
                .clean_full()
                .to_string();
        let target = "/vrpg/ is a place to discuss all types of role-playing video games, including single-player, multi-player, and massively multi-player, turn-based and real-time action, western-style and JRPG.\n\nDoes this mean RPGs are banned on other video game boards? No! /vrpg/ is just a separate board specifically focused on RPGs where discussions about your favorite games can thrive.\n\nPlease familiarize yourself with the rules and remember to use the spoiler function where appropriate!\nPlease note that, like /v/, \"Generals\"—long-term, one-after-the-other, recurring threads about a specific game are not permitted on /vrpg/. Such threads belong on >>>/vg/.";
        pintln!([s]);
        assert_eq!(&s, target);
    }
}
