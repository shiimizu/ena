# Ena

Ena aims to be a low resource, high performance archiver to dump and archive posts, images and all relevant data from 4chan into a local database and local image store.

## Features
* Much more memory efficient than [Asagi](https://github.com/eksopl/asagi)
  * Less than ~4mb on my machine
* Less bandwith
  * By using `threads.json` and `archive.json`, API requests stay efficient without sacrificing any posts, instead of continously polling every thread for updates.
* More to come

<!--
# Edge cases covered
* banned posts
* thread/post/file deletions
* massive threads consisting of thousands of posts
  -->

## Pre-release
This pre-release is for developers who want to try it out. Though, there are some **caveats**:
* only 1 board at the moment (the first one in `ena_config.json`)
* only `threads.json` at the moment. no `archive.json`
* doesn't check md5 for initial downloading of media, but does check the sha256 hashsums on subsequent thread updates
* no logs
* if an `unwrap` or `expect` sets off, it'll panic the program and exit (has to be handled but it rarely sets off)
* some config settings aren't implemented yet
* media folder is currently in "./archive/media" from your current working directory
* proxies are not yet implemented
* media downloading is turned off (Oops. It'll be turned back on the next release)
* `retryAttempts` is `3`
* `refreshDelay` is `10`
* `throttleMillisec` is `1000`
* cache is stored in-db in the metadata table. The archiver assumes all threads have been downloaded as per whats in the cache, but that may not be the case (maybe you stopped the program). To trigger a full re-update of all available threads online, just remove the entry and restart the archiver:  
  ```sql
  DELETE FROM metadata WHERE board='a';
  ```
## Changes from Asagi
* Schema changes, albeit a reasonable change. Only 1 table for `metadata` (for caches). 1 table for each board. The schema inside each board is straight from [4chan's thread endpoint](https://github.com/4chan/4chan-API/blob/master/pages/Threads.md).
* config changes
* media downloading uses sha256 as it's filename and it's directory structure is the same as yuki.la's
* PostgreSQL 12 is the database engine that was used and tested with

## Installation
1. Download the pre-compiled [binaries][latest-link] **or** build from source for the latest builds.  
You'll need Rust installed. After that, clone the repo and build.
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
## Build info
```
Windows 10 Pro 1909  
Intel Core i5-6500 @ 3.20GHz  
16GB RAM  
NVIDIA GeForce GTX 1060 3GB

Windows:
nightly-x86_64-pc-windows-msvc
rustc 1.41.0-nightly (412f43ac5 2019-11-24)

Linux (WSL):
nightly-x86_64-unknown-linux-gnu (default)
rustc 1.41.0-nightly (c8ea4ace9 2019-12-14)
```

## FAQ
### Why?
Much of my personal time and research went into 4ch for educational purposes and self development. I cherish the things I've learned there and have a plethora of archived threads. Archival sites have been crumbling down due to requiring several tens of gigabytes to keep Asagi archiving every board.. The current avaliable solutions are not practical nor production ready. I couldn't just take without giving back.

### Why Rust?
I wanted something fast, safe, and ideally able to withstand long-term usage.

### What's with the name?
> Asagi is the eldest of the Ayase sisters. Fuuka is the middle sister. The Ayase family lives next door to Yotsuba. Get it?

Ena is the youngest of the Ayase sisters. I was thinking if a new archiver was born, why not be a continuation from the Ayase family?

[latest-badge]: https://img.shields.io/badge/latest-v0.1.0-ca7f85.svg?style=flat-square
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[matrix-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[matrix-badge]: https://img.shields.io/badge/matrix-join-ca7f85.svg?style=flat-square
