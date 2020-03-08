<h1 align="center">
	Ena
	<br>
  <img src="./img/yotsuba-and-ena.png" alt="Yotsuba&Ena!" width="470" /><br>
  
</h1>

<div align="center">

<b>Lightweight 4chan thread and board archiver</b><br>

[![Latest Version][latest-badge]][latest-link]
[![License][license-badge]][license-url]
[![Build Status][build-badge]][build-url]
[![Unsafe Forbidden][safety-badge]][safety-url]
[![Matrix Chat][chat-badge]][chat-link]

[latest-badge]: https://img.shields.io/github/v/release/shiimizu/ena?color=ca7f85&style=flat-square
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[license-badge]: https://img.shields.io/github/license/shiimizu/ena?color=blue&style=flat-square
[license-url]: LICENSE
[build-badge]: https://img.shields.io/github/workflow/status/shiimizu/ena/Rust?style=flat-square
[build-url]: https://github.com/shiimizu/ena/releases/latest
[safety-badge]: https://img.shields.io/badge/unsafe-forbidden-green.svg?style=flat-square
[safety-url]: https://github.com/rust-secure-code/safety-dance/
[chat-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[chat-badge]: https://img.shields.io/matrix/bibanon-chat:matrix.org?logo=matrix&style=flat-square

</div>

<br>

Low resource and high performance archiver to save posts, images and all relevant data from 4chan into a local database and local image store. It is:

* Asagi compatible - On top of having a new engine and schema, you can still use [Asagi](https://github.com/eksopl/asagi)'s schema alongside with it.
* Memory efficient - Using less than 5mb for a single board. Less than 30mb for all 72 boards.
* Bandwidth efficient - [API](https://github.com/4chan/4chan-API) requests stay low without sacrificing any posts by using only `threads.json` and `archive.json`, instead of continously polling every thread for updates.

<!--
# Edge cases covered
* banned posts
* thread/post/file deletions
* massive threads consisting of thousands of posts
  -->
## Runtime dependencies
* [PostgreSQL](https://www.postgresql.org/download/) >= 11.0

## Changes from Asagi
* PostgreSQL as the database engine
* Media files use sha256 as its filename and its directory structure is the same as yuki.la's
* Comments are preserved and untouched

## Installation
1. [Download][latest-link] the pre-compiled binaries _**or**_ build from source for the latest builds.  
You'll need [Rust](https://www.rust-lang.org/tools/install) installed. After that, clone the repo and build.
	```console
	$ git clone https://github.com/shiimizu/ena.git
	$ cd ena
	$ cargo build --release
	```

2. Start your PostgreSQL server either through the command-line or through PgAdmin.

3. Edit the config file and put in your DB connection info, media directory, what boards you want to archive, and tweak any of the other settings that look interesting. Don't go below 0.12s or so for the ratelimit or you'll get banned. Follow the API rules and keep it at or above 1 unless you really need to (If you're not sure whether you need to, you probably don't need to).

4. You should now be able to run `ena` and have it start archiving, and report status to the standard output, showing requests as they happen, as well as a display of current queued tasks.<br>Ctrl-C will stop Ena. To leave Ena running long term, you can use screen (or byobu or any such tool).

## FAQ
Check the [wiki](https://github.com/shiimizu/ena/wiki) for more FAQs and information.

### Why?
Much of my personal time and research went into 4chan for educational purposes and self development. I value the things I've learned there and have a plethora of threads saved. Archival sites have been crumbling down due to requiring several tens of gigabytes to keep Asagi archiving every board. The current avaliable solutions are not practical nor production ready so I decided to help out.

### Why Rust?
I wanted something fast, safe, and ideally able to withstand long-term usage.
* [Energy Efficiency across Programming Languages](https://sites.google.com/view/energy-efficiency-languages/results)
* [Which programs are fastest?](https://benchmarksgame-team.pages.debian.net/benchmarksgame/which-programs-are-fastest.html)
* [Why should I rust?](https://www.reddit.com/r/rust/comments/ekuiql/why_should_i_rust/)

### What's with the name?
> Asagi is the eldest of the Ayase sisters. Fuuka is the middle sister. The Ayase family lives next door to Yotsuba. Get it?

Being the next generation of archivers, I wanted a name to reflect that.  
Ena happens to be the youngest of the Ayase sisters. I liked the name so I stuck with it.

