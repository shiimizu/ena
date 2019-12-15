# Ena

Ena aims to be a low resource, high performance archiver to dump and archive posts, images and all relevant data from 4chan into a local database and local image store.

## Features
* **Memory efficient:** Using less than 4mb
* **Bandwith efficient:** API requests stay low without sacrificing any posts by using only `threads.json` and `archive.json`, instead of continously polling every thread for updates.

<!--
# Edge cases covered
* banned posts
* thread/post/file deletions
* massive threads consisting of thousands of posts
  -->
## Runtime dependencies
* [PostgreSQL](https://www.postgresql.org/download/)

## Changes from Asagi
* Schema changes, albeit a reasonable change. Only 1 table for `metadata` (for caches). 1 table for each board. The schema inside each board is straight from [4chan's thread endpoint](https://github.com/4chan/4chan-API/blob/master/pages/Threads.md).
* config changes
* media downloading uses sha256 as it's filename and it's directory structure is the same as yuki.la's
* PostgreSQL 12 is the database engine that was used and tested with

## Installation
1. Download the [pre-compiled binaries][latest-link] _**or**_ build from source for the latest builds.  
You'll need [Rust](https://www.rust-lang.org/tools/install) installed. After that, clone the repo and build.
	```console
	$ git clone https://github.com/shiimizu/ena.git
	$ cd ena
	$ cargo build --release
	```
2. Edit the config file and put in your DB connection info, media directory, what boards you want to archive, and tweak any of the other settings that look interesting. Don't go below 0.12 or so for the ratelimit or you'll get banned. Follow the API rules and keep it at or above 1 unless you really need to (If you're not sure whether you need to, you probably don't need to).

3. You should now be able to run `ena` and have it start archiving, and report status to the standard output, showing requests as they happen, as well as a display of current queued tasks. Ctrl-C will stop Ena. To leave Ena running long term, you can use screen (or byobu or any such tool).

## Querying the data
The schema is practically straight from 4chan's API. You can run this command to view the relational version of it (where `a` is the board and `no` and `resto` is the OP num):
```sql
select * from a where no = 196659047 or resto = 196659047 order by no;
```

## FAQ
### Why?
Much of my personal time and research went into 4ch for educational purposes and self development. I value the things I've learned there and have a plethora of archived threads. Archival sites have been crumbling down due to requiring several tens of gigabytes to keep Asagi archiving every board. The current avaliable solutions are not practical nor production ready. So I decided to help out.

### Why Rust?
I wanted something fast, safe, and ideally able to withstand long-term usage.

### What's with the name?
> Asagi is the eldest of the Ayase sisters. Fuuka is the middle sister. The Ayase family lives next door to Yotsuba. Get it?

It just so happens that Ena is the youngest of the Ayase sisters. I liked the name so I stuck with it.

[latest-badge]: https://img.shields.io/badge/latest-v0.1.0-ca7f85.svg?style=flat-square
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[matrix-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[matrix-badge]: https://img.shields.io/badge/matrix-join-ca7f85.svg?style=flat-square
