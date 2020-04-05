<h1 align="center"><img src="./img/yotsuba-and-ena.png" alt="Yotsuba&Ena!" width="470" /><br>
Ena</h1><h4 align="center">An ultra lightweight imageboard archiver<br><br>
<div align="center">

[![Latest Version][latest-badge]][latest-link]
[![License][license-badge]][license-url]
[![Build Status][build-badge]][build-url]
[![Unsafe Forbidden][safety-badge]][safety-url]
[![Matrix Chat][chat-badge]][chat-link]

[latest-badge]: https://img.shields.io/github/v/release/shiimizu/ena?color=ca7f85&style=flat-square
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[license-badge]: https://img.shields.io/github/license/shiimizu/ena?color=blue&style=flat-square
[license-url]: LICENSE
[build-badge]: https://img.shields.io/github/workflow/status/shiimizu/ena/Rust?logo=github&style=flat-square
[build-url]: https://github.com/shiimizu/ena/actions?query=workflow%3ARust
[safety-badge]: https://img.shields.io/badge/unsafe-forbidden-green.svg?style=flat-square
[safety-url]: https://github.com/rust-secure-code/safety-dance/
[chat-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[chat-badge]: https://img.shields.io/matrix/bibanon-chat:matrix.org?logo=matrix&style=flat-square

</div>

</h4>

<br>

Low resource and high performance archiver to save posts, images and all relevant data from 4chan into a local database and local image store. Check the [wiki](https://github.com/shiimizu/ena/wiki) for more information.

### Features
* Asagi compatible - Use as a [drop-in replacement](https://github.com/shiimizu/ena/wiki/Asagi) if you want.
* Memory efficient - Using less than 5mb for a single board. Less than 30mb for all 72 boards.
* Bandwidth efficient - [API](https://github.com/4chan/4chan-API) requests stay low without sacrificing any posts by using only `threads.json` and `archive.json`, instead of continously polling every thread for updates.
* Preserved comments - Comments are untouched in their original HTML format
* Hashed files - Media files and thumbnails are hashed with SHA256

<!--
# Edge cases covered
* banned posts
* thread/post/file deletions
* massive threads consisting of thousands of posts
  -->

### Runtime dependencies
* [PostgreSQL](https://www.postgresql.org/download/) >= 11.0

### Installation
1. [Download][latest-link] the pre-compiled binaries _**or**_ build from source for the latest builds.  
You'll need [Rust](https://www.rust-lang.org/tools/install) installed. After that, clone the repo and build. Set your [environment variable](https://github.com/shiimizu/ena/wiki/Configuration#extra-environment-variables) to view output.  
	```console
	$ git clone https://github.com/shiimizu/ena.git
	$ cd ena
	$ cargo build --release
	```

2. Start your PostgreSQL server either through the command-line or through PgAdmin.

3. Edit the config file and put in your DB connection info, media directory, what boards you want to archive, and tweak any of the other settings that look interesting. Don't go below 0.12s or so for the ratelimit or you'll get banned. Follow the API rules and keep it at or above 1 unless you really need to (If you're not sure whether you need to, you probably don't need to).

4. You should now be able to run `ena` and have it start archiving, and report status to the standard output, showing requests as they happen, as well as a display of current queued tasks.<br>Ctrl-C will stop Ena. To leave Ena running long term, you can use screen (or byobu or any such tool).

### Why do this?
Much of my personal time and research went into 4chan for *educational purposes* and self development. I value the things I've learned there and have a plethora of threads saved. Archival sites have been crumbling down due to requiring several tens of gigabytes to keep Asagi archiving every board. At the time there wasn't any avaliable practical solutions nor were they production ready, so I decided to help out.

### Why Rust?
I wanted something fast, safe, and ideally able to withstand long-term usage.
* [Energy Efficiency across Programming Languages](https://sites.google.com/view/energy-efficiency-languages/results)
* [Which programs are fastest?](https://benchmarksgame-team.pages.debian.net/benchmarksgame/which-programs-are-fastest.html)
* [Why should I rust?](https://www.reddit.com/r/rust/comments/ekuiql/why_should_i_rust/)

### What's with the name?
> Asagi is the eldest of the Ayase sisters. Fuuka is the middle sister. The Ayase family lives next door to Yotsuba. Get it?

Ena is the next in line and youngest of the Ayase sisters.

