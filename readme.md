[![Latest][latest-badge]][latest-link]

# Ena

Low resource and high performance archiver to dump and archive posts, images and all relevant data from 4chan into a local database and local image store.

## Features
* Asagi compatible - On top of having a new engine and schema, you can still use the Asagi schema alongside with it. How cool is that?!
* Memory efficient - Using less than 5mb for a single board. Less than 30mb for all 72 boards.
* Bandwidth efficient - API requests stay low without sacrificing any posts by using only `threads.json` and `archive.json`, instead of continously polling every thread for updates.

<!--
# Edge cases covered
* banned posts
* thread/post/file deletions
* massive threads consisting of thousands of posts
  -->
## Runtime dependencies
* [PostgreSQL](https://www.postgresql.org/download/)

## Changes from Asagi
* Schema changes<b>\*</b>, albeit a reasonable one. 1 table for `metadata` (for caches). Only 1 table for each board. The [schema inside each board](https://github.com/shiimizu/ena/blob/master/img/schema1.png) is straight from [4chan's thread endpoint](https://github.com/4chan/4chan-API/blob/master/pages/Threads.md).
* config changes
* media downloading uses sha256 as it's filename and it's directory structure is the same as yuki.la's
* PostgreSQL 12 is the database engine that was used and tested with

<superscrpt>\*There's an expermental [Asagi schema](https://github.com/eksopl/asagi/blob/master/src/main/resources/net/easymodo/asagi/sql/Pgsql/boards.sql) (as a [`view`](http://www.postgresqltutorial.com/managing-postgresql-views/)) created alongside the existing one.<br>Query it like `select * from a_asagi`, where `a` is the board name</superscrpt>

## Installation
1. Download the [pre-compiled binaries][latest-link] _**or**_ build from source for the latest builds.  
You'll need [Rust](https://www.rust-lang.org/tools/install) installed. After that, clone the repo and build.
	```console
	$ git clone https://github.com/shiimizu/ena.git
	$ cd ena
	$ cargo build --release
	```

2. Start your PostgreSQL server either through the command-line or through PgAdmin.

3. Edit the config file and put in your DB connection info, media directory, what boards you want to archive, and tweak any of the other settings that look interesting. Don't go below 0.12 or so for the ratelimit or you'll get banned. Follow the API rules and keep it at or above 1 unless you really need to (If you're not sure whether you need to, you probably don't need to).

4. You should now be able to run `ena` and have it start archiving, and report status to the standard output, showing requests as they happen, as well as a display of current queued tasks.<br>Ctrl-C will stop Ena. To leave Ena running long term, you can use screen (or byobu or any such tool).

## FAQ
Check the [wiki](https://github.com/shiimizu/ena/wiki) for more FAQs and information.

### Why?
Much of my personal time and research went into 4ch for educational purposes and self development. I value the things I've learned there and have a plethora of archived threads. Archival sites have been crumbling down due to requiring several tens of gigabytes to keep [Asagi](https://github.com/eksopl/asagi) archiving every board. The current avaliable solutions are not practical nor production ready. So I decided to help out.

### Why Rust?
I wanted something fast, safe, and ideally be able to withstand long-term usage.

### What's with the name?
> Asagi is the eldest of the Ayase sisters. Fuuka is the middle sister. The Ayase family lives next door to Yotsuba. Get it?

It just so happens that Ena is the youngest of the Ayase sisters. I liked the name so I stuck with it.

[latest-badge]: https://img.shields.io/github/v/release/shiimizu/ena?color=ca7f85&style=flat-square
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[matrix-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[matrix-badge]: https://img.shields.io/badge/matrix-join-ca7f85.svg?style=flat-square
