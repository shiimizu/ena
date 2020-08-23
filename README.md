

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
* `archive` threads
* Save bandwidth. Uses `If-Modified-Since` and `Last-Modified`. 
* Save even more bandwidth with the option of using `-tail.json`.
* State save restore. Resume right where you left off.
* Poll boards/threads or oneshot
* Dynamic thread refresh rate
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

## Reading the output
Generally, the ouput looks something like:  

```
({function}): (threads|archive) /{board}/{thread} {Response-Last-Modified} | {In-Database-Last-Modified} | {NEW|UPSERTED|DELETED}
```  

If you see a line it means the thread/post was modified and reported back to the screen.  
The two datetimes are used to compare the times of modification.


## Status  
Core functionality works. There are things that could be improved on:  
* Things are a bit fragile at the moment as the codebase is littered with `unwrap()` (for debugging) and can panic if any one of them sets off. (This will be cleaned up as things are finalized)
  * Also panics on no network connection. Workaround currently is to set a really high `retry_attempts`.
* Logging could be better
* Postgres is not getting to correct amount of media files.  
    Solution found and implementation is underway. See [this report](error-media-log.md) for more information.

## Asagi drop-in status
* Posts deleted that are outputted to the screen will likely appear twice. Don't worry, it's just displaying issue.
    Finding a way to incorporate `RETURNING *` for `mysql|mariadb` would fix this.
* `UPSERTED` doesnt display the upserted count like the postgres version. Same issue and fix as above.
