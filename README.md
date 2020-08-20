<!--

# New
****
[![Latest Version][latest-badge]][latest-link]
[![License][license-badge]][license-url]
[![Lines Of Code][tokei-loc-badge]][repo-url]
[![Build Status][build-badge]][build-url]
[![Unsafe Forbidden][safety-badge]][safety-url]
[![Documentation][doc-badge]][doc-url]
[![rustc](https://img.shields.io/badge/rustc-1.41+-blue.svg)](https://blog.rust-lang.org/2020/03/12/Rust-1.42.html)

<!--[![Matrix Chat][matrix-chat-badge]][matrix-chat-link]
[![Discord Chat][discord-chat-badge]][discord-chat-link]



[repo-url]: https://github.com/shiimizu/ena
[tokei-loc-badge]: https://tokei.rs/b1/github/shiimizu/ena?category=code
[license-badge]: https://img.shields.io/github/license/shiimizu/ena?color=blue
[license-url]: LICENSE
[latest-badge]: https://img.shields.io/github/v/release/shiimizu/ena?color=orange
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[build-badge]: https://img.shields.io/github/workflow/status/shiimizu/ena/Rust?logo=github
[build-url]: https://github.com/shiimizu/ena/actions?query=workflow%3ARust
[safety-badge]: https://img.shields.io/badge/unsafe-forbidden-green.svg
[safety-url]: https://github.com/rust-secure-code/safety-dance/
[doc-badge]: https://img.shields.io/badge/docs-latest-%235075A7.svg
[doc-url]: https://shiimizu.github.io/ena.docs/doc/ena/pgsql/core/struct.Post.html
[discord-chat-link]: https://discord.gg/phPHTEs
[discord-chat-badge]: https://img.shields.io/badge/chat-on%20discord-%23788BD8?logo=discord
[matrix-chat-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[matrix-chat-badge]: https://img.shields.io/matrix/bibanon-chat:matrix.org?logo=matrix&color=green


# From where you left off. TODO:
* set -x RUST_BACKTRACE 'full'
* $env:RUST_BACKTRACE='full'
* $env:RUST_BACKTRACE='1'
* [for DBA and frontend](https://hakibenita.com/sql-tricks-application-dba)
* `./target/release/asql -b a aco adv an asp b bant biz c cgl ck cm co d diy e f fa fit g gd gif h hc his hm hr i ic int jp k lgbt lit m mlp mu n news o out p po pol qa qst r r9k s s4s sci soc sp t tg toy trash trv tv u v vg vip vp vr w wg wsg wsr x y`
* `cls; cat up-mysql.sql | mysql -u root --password="zxc" ena2`
* `cargo depgraph --dedup-transitive-deps | dot -Tpng > graph2.png`
* `cargo deps | dot -Tpng > graph.png`
* `cargo doc --lib --no-deps -j 4`
* `cargo doc --bin asql --no-deps -j 4`
* `cls; c r -q -j4 -- --asagi --engine mysql --name ena2 --schema ena2 --port 3306 --username root --password zxc --charset utf8mb4 --collate utf8mb4_unicode_ci --watch-boards -b b`

-->
# Ena
```
ena 0.8.0-dev
An ultra-low resource imageboard archiver

USAGE:
    ena.exe [FLAGS] [OPTIONS] --boards <boards>... --threads <threads>...

FLAGS:
        --asagi              Use Ena as an Asagi drop-in
        --quickstart         Download everything in the beginning with no limits and then throttle
        --strict             Download sequentially rather than concurrently. This sets limit to 1
        --watch-boards       Enable archiving the boards
        --watch-threads      Enable archiving the live threads until it's deleted or archived
        --with-archives      Download archived threads as well
        --with-full-media    Download full media as well
        --with-tail          Prefer to use tail json if available
        --with-threads       Download live threads as well
        --with-thumbnails    Download thumbnails as well
    -h, --help               Prints help information
    -V, --version            Prints version information

OPTIONS:
    -c, --config <config>                        Config file or `-` for stdin [env: CONFIG]  [default: config.yml]
    -b, --boards <boards>...                     Get boards [example: a,b,c] [env: BOARDS]
    -e, --exclude-boards <boards-excluded>...    Exclude boards [example: a,b,c] [env: BOARDS_EXCLUDED]
    -t, --threads <threads>...                   Get threads [env: THREADS]
        --interval-boards <interval-boards>      Delay (ms) between each board [env: INTERVAL_BOARDS]  [default: 30000]
        --interval-threads <interval-threads>    Delay (ms) between each thread [env: INTERVAL_THREADS]  [default: 1000]
        --limit <limit>                          Limit concurrency getting threads [env: LIMIT]  [default: 151]
        --limit-media <limit-media>              Limit concurrency getting media [env: LIMIT_MEDIA]  [default: 151]
    -m, --media-dir <media-dir>                  Media download location [env: MEDIA_DIR]
        --retry-attempts <retry-attempts>        Retry number of HTTP GET requests [env: RETRY_ATTEMPTS]  [default: 3]
    -A, --user-agent <user-agent>                Set user agent [env: USER_AGENT]
        --api-url <api-url>                      Set api endpoint url [env: API_URL]
        --media-url <media-url>                  Set media endpoint url [env: MEDIA_URL]
        --db-url <url>                           Set database url [env: ENA_DATABASE_URL]
        --engine <engine>                        Set database engine [env: ENA_DATABASE_ENGINE]
        --name <name>                            Set database name [env: ENA_DATABASE_NAME]
        --schema <schema>                        Set database schema [env: ENA_DATABASE_SCHEMA]
        --host <host>                            Set database host [env: ENA_DATABASE_HOST]
        --port <port>                            Set database port [env: ENA_DATABASE_PORT]
        --username <username>                    Set database user [env: ENA_DATABASE_USERNAME]
        --password <password>                    Set database password [env: ENA_DATABASE_PASSWORD]
        --charset <charset>                      Set database charset [env: ENA_DATABASE_CHARSET]
        --collate <collate>                      Set database charset [env: ENA_DATABASE_COLLATE]

```

# General
- [x] TimeScale
- [x] Proxy

# SQL
- [x] get & set deleted in program when diffing
- [x] when deleted/archived - update and calculate correct # of replies & images, use after update trigger, only if delted, increment? but then how does the upsert work so that the replies/images stays the same afterwards?.. in the UPSERT get COUNT of replies/images so that it will be excluded, -> only insert thread, don't update images/replies, use triggers for that? on insert, then you would have to do the same ON DELETED
- [x] remove now column

# Scraper
- [x] [`CONNECTION` header](https://docs.rs/reqwest/0.10.7/reqwest/header/constant.CONNECTION.html)
- [x] schema search path
- [x] verify api_url and media_url with `URL` crate
- [x] Remove extra: {"tail_size": 50}
- [ ] ouput error, only if on last retry
- [x] Semaphore & limit to threads due to sockets
  - [x] don't concurrently get all boards? process a single board first then move on? better for memory
- [x] CTRL+C
- [ ] Config cleanup and implement
- [x] Use tail json whenever possible: extra: {"tail_size": 50} --use-tail-json (to save bandwidth? possibly less accurate, since mods can change any post before the tail_size)
- [x] use boards.json to validate boards, upsert to db as id 0
- [x] last_modified
  - [x] `boards.json` `threads.json` `archive.json`
- [x] deleted: compare single threads based on the new one's initial reply num as a starting for the in-db (>=) [double check if OP stays or fades away for large threads?]
- [x] only update threads.json & its last_modified at the end
- [x] use a thread's last post `time` in-db as its `last_modified` for the GET request
- [x] combined threads on startup, modified threads afterwards
- [x] If-Mofified-Since: Use threads.json and archive.json's `date` to input for their last_modified, so that we can state save resume for program. use cache if we wanna go from where we left off, otherwise get combined
- [x] for posts only update if modeified, ie if it was updated, otherwise null
- [x] (--watch--board, --watch--thread) loop on network errors, grabbing boards should never end
- [x] Add loop retry
- [x] Don't insert Map object on key "extra" if it's empty
- [x] Update do set least modified AFTER you upsert
- [x] Update deleted only if null and 0
- [x] update `is_board` function in `main.rs`
- [x] in trigger after insert for each statement, update replies/images to be the count(*)
- [x] Don't update replies/images in the UPSERT (only insert) (inserting won't really matter if an after insert trigger occurs each statement)
- [x] Combined/modified threads - Tokio poistrges stream
- [ ] Don't deleted threads on combined? (startup), only deleted threads on modified function? that way you know it was actually deleted, rather than deleting from cache+new(combined)? 
- [x] mysql: upserted amount
- [x] mysql: update deleteds
- [x] mysql : update_deleteds: v_latest doesnt have all posts? what?
- [x] URL cli option (makes things easier), patch as well
- [x] CLI OPT boards needs to be patched with board_settings
- [ ] interleave boards when strict?
- [ ] in DatabaseOpt -> make engine an enum
- [x] asagi_mode requiired when engine=="mysql" variant/ or not postgres 
- [x] Patch the rest of Opt fields. See `Opt::default()`. So far we only did BoardSettings
- [x] watch-threads
- [x] excluded boards
- [ ] SQL migrations up.sql
- [ ] Custom parse for timescaledb interval (just validate, either num or interval )
- [x] limit / strict / media-threads
- [ ] watch-boards: needs to exit a board, not the whole loop
- [x] watch-thread on an archive should end
- [x] percent_encode the filename to get the correct url for /f/
- [x] Media
- [x] Config, DB_URL, default-rust->yaml->env->cli
- [ ] modules bin lib workspaces, Module system, Lib in diff crate so ppl can actually use without pulling executor
- [ ] Kuroba threads + other sites
- [ ] Move wiki to in-code doc

<!--
Tests
- [ ] [load test](https://github.com/tag1consulting/goose)
- [ ] [http mock](https://github.com/LukeMathWalker/wiremock-rs)

Ena-Server?


# Media
- [ ] Insert media in db (auto hash) or store on disk
- [ ] check md5 before dl
- [ ] --media-force-stop-on-ctrl-c
- [ ] dynamic config? hot reload? 
- [ ] check if file exists needs to be done efficiently so it won't wear out the drive. no exessive read seeaks?
- [ ] list of dirs to check for media, first one is where it's attempted to be saved, then to others, if still err, make new dir and save there, if still err, drive probably has no space and output error
- [ ] press ctrl-c again to cancel media fetching (requires to check for media in all combined threads regardless of if-modified, this is so you can ctrl c anytime + state save, check if need to dl, this happens all the time after getting threads.json, how efficient is this vs billions of rows?.. if you check it and it's already been found, dont check it again)
- [ ] Quote all postgres identifiers

-->
