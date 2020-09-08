

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

-->
# Ena

[![License][license-badge]][license-url]

[license-badge]: https://img.shields.io/github/license/shiimizu/ena?color=blue
[license-url]: LICENSE
[matrix-chat-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[matrix-chat-badge]: https://img.shields.io/matrix/bibanon-chat:matrix.org?logo=matrix&color=green

Low resource and high performance archiver to save posts, images and all relevant data from an imageboard into a local database and local image store. The project currently only supports 4chan.  

**This development branch is currently undergoing active breaking changes towards ena v0.8.0. Do not use in production. See the current [status](#Status).**

## Tracking changes
<!-- * 🚧 Fix getting all 4 variants of thumbnails 🚧 -->
* Better last modified detection
* Ability to use `-tail` json
* Fix the correct number of `replies` and `images`
* Add ability to use proxies
* Faster CTRL+C
* Actual Asagi support (support for legacy mysql)
* More options, flags, and customization
* Improved logging. Outputs to both `stdout` and `stderr`.
* Switch to YAML
* Lives up to the description by being able to get threads and/or boards in a oneshot fashion
* Reduce postgres min version. Probably >= `9` now.
* Now gets the correct number of thumbnails (op & reply) *
* **Database schema changes. Now one big `posts` table instead of seperated by board.**
* No longer reports the current position/total while logging since things are fetched concurrently
* Media fetching is no longer done in a background thread
* Now relying on md5 check before downloading media, which means no sha256sum collision detection
* Introduction of `unsafe` to clean post comments for Asagi.  

<small>* in progress</small>

## Usage

```
ena 0.8.0-dev
An ultra-low resource imageboard archiver

USAGE:
    ena [FLAGS] [OPTIONS] --boards <boards>... --threads <threads>...

FLAGS:
        --asagi               Use Ena as an Asagi drop-in
        --strict              Download sequentially rather than concurrently. This sets limit to 1
        --watch-boards        Enable archiving the boards
        --watch-threads       Enable archiving the live threads until deleted or archived (only applies to threadslist)
        --with-archives       Grab threads from archive.json
        --with-full-media     Download full media as well
        --with-tail           Prefer to use tail json if available
        --with-threads        Grab threads from threads.json
        --with-thumbnails     Download thumbnails as well
        --interval-dynamic    Add 5s on intervals for each NOT_MODIFIED. Capped
        --skip-board-check    Skip checking if a board is valid
    -h, --help                Prints help information
    -V, --version             Prints version information

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

## Features
* Ability to get `archive` threads
* Save bandwidth. Uses `If-Modified-Since` and `Last-Modified`. 
* Save even more bandwidth with the option of using `-tail.json`.
* State save restore. Resume right where you left off. No-frills updating to new releases.
* Poll boards/threads or oneshot
* Dynamic (consecutively increasing) thread refresh rate
* Retain `unique_ips` after archived
* Proxies
* TimescaleDB support
* Asagi drop-in replacement support
* `UPSERT` for superior thread accuracy and correctness
* `SHA256` for full media and thumbnails to prevent duplicates

## Quickstart
1. The easiest way to start is to give a config file:  
        `ena -c config.yml`
1. A database is created if it wasn't already.
1. ???
1. Profit

### Asagi drop-in
1. Set `asagi_mode` to `true` in the config or pass `--asagi` in the command line.
2. ???
2. Profit

## Building/Updating
1. Install [Rust](https://www.rust-lang.org/tools/install) if you haven't already
1. Clone this branch and `cd` if you haven't already
   ```shell
   $ git clone --single-branch --branch dev https://github.com/shiimizu/ena.git ena-dev
   $ cd ena-dev
   ```

2. Building/Updating  
    ```shell
    $ cargo build --release -j4
    ```  
    Build artifacts can be found in `target/release/`  

## Status  
Core functionality works. There are things that could be improved on:  
* Things are a bit fragile at the moment as the codebase is littered with `unwrap()` (for debugging) and can panic if any one of them sets off. (This will be cleaned up as things are finalized)
* Postgres side
  * Currently not getting the correct amount of media files.  
        Solution found and implementation is underway. See [this report](error-media-log.md) for more information.

## Asagi drop-in status
Stable.

## Please respect the 4chan API guidelines as best you can
1. Use `Last-Modified` and `If-Modified-Since`
1. Ratelimit of atleast 1 second  

Respecting the first rule will get you very far.

## Architecture flow
1. Boards are fetched sequentially since doing so concurrently requires quite a bit of memory.

1. The list of threads are fetched from `threads.json` or `archive.json` and stored in the database.
   * The `Last-Modified` from the HTTP header is then stored in the database as a timestamp. Before fetching the threads list, this is checked so to not download the list again if it's not modified.

1. On startup of the program, threads are combined between in-db (if any) and received so as to get the **union** of both.  
   After the initial startup, all subsequent list of threads are the result of a **symmetric-difference** between in-db and received threads,  
   e.g only the modified threads such as deleted/updated/added/modified, resulting in a lower amount of threads to be processed.

2. Then each thread is fetched concurrently (based on `limit` setting).
    1. In each thread, every media file & thumbnail is fetched concurrently (based on `limit_media` setting) and its entry is upserted to the database.
    2. The thread is upserted to the database after media is done (if any), and then the `Last-Modified` from the HTTP header is stored in the `last_modified` column of the thread.
        * Before fetching a thread this is checked so to not download the thread again if it's not modified.

3. Once a board is finished (all its threads are fetched), the next one is fetched and the whole process starts all over again.


## Asagi specific implementation changes
* Added `with_utc_timestamps` as an option which adds `utc_timestamp`, `utc_timestamp_expired`, columns to boards and `utc_archived_on` to `{board}_threads` table
* Added `with_extra_columns` as an option which adds any extra or future columns to `exif`
* Added `boards` table to cache `threads.json`|`archive.json` for state save restore.
* Fixed updating `sticky` and `locked` for threads in triggers
* More accurate `deleted` posts due to upserts
* Cleaner sticky comments
* Use `{board}_threads`'s `time_last` to store `Last-Modified` from HTTP header. Nobody uses the `time_last` column so it's OK. 
* Any new changes for posts made by this implementation will overwrite the entry in your database due to upserts. 




## FAQ
##### Why not use `catalog.json`?
I've found it to be inaccurate.