<h1 align="center"><img src="./img/yotsuba-and-ena.png" alt="Yotsuba&Ena!" width="470" /><br>
Ena</h1><p align="center">An ultra lightweight imageboard archiver</p><h4 align="center">
<div align="center">

[![Latest Version][latest-badge]][latest-link]
[![Documentation][doc-badge]][doc-url]
[![License][license-badge]][license-url]
[![Build Status][build-badge]][build-url]
[![Unsafe Forbidden][safety-badge]][safety-url]
[![Discord Chat][discord-chat-badge]][discord-chat-link]
[![Matrix Chat][matrix-chat-badge]][matrix-chat-link]

[repo-url]: https://github.com/shiimizu/ena
[latest-badge]: https://img.shields.io/github/v/release/shiimizu/ena?color=ca7f85&style=flat-square
[latest-link]: https://github.com/shiimizu/ena/releases/latest
[license-badge]: https://img.shields.io/github/license/shiimizu/ena?color=blue&style=flat-square
[license-url]: LICENSE
[doc-badge]: https://img.shields.io/badge/docs-latest-orange.svg?style=flat-square
[doc-url]: https://shiimizu.github.io/ena.docs
[build-badge]: https://img.shields.io/github/workflow/status/shiimizu/ena/Rust?logo=github&style=flat-square
[build-url]: https://github.com/shiimizu/ena/actions?query=workflow%3ARust
[safety-badge]: https://img.shields.io/badge/unsafe-forbidden-green.svg?style=flat-square
[safety-url]: https://github.com/rust-secure-code/safety-dance/
[discord-chat-link]: https://discord.gg/phPHTEs
[discord-chat-badge]: https://img.shields.io/discord/134020776251752448?logo=discord&style=flat-square
[matrix-chat-link]: https://matrix.to/#/#bibanon-chat:matrix.org
[matrix-chat-badge]: https://img.shields.io/matrix/bibanon-chat:matrix.org?logo=matrix&style=flat-square
[scc-code-badge]: https://sloc.xyz/github/shiimizu/ena?category=code
[scc-cocomo-badge]: https://sloc.xyz/github/shiimizu/ena?category=cocomo

</div>

</h4>

<br>

Low resource and high performance archiver to save posts, images and all relevant data from 4chan into a local database and local image store.  

## Features

* **Asagi compatible**<br>
 Capable as a [drop-in replacement](https://github.com/shiimizu/ena/wiki/Asagi)

* **Memory efficient**<br>
 Using less than 5mb for a single board and less than 30mb for all 72 boards
 
* **Bandwidth efficient**<br>
 Minimal [API](https://github.com/4chan/4chan-API) requests by using `threads.json` and `archive.json` instead of continuously polling every thread for updates
 
* **Accurate**<br>
 Threads are diffed, patched, and merged by the database so posts are always correct and up-to-date
    
* **Collision resistant**<br>
 Media files and thumbnails are hashed with SHA256 and deduped by using it as the filename
    
* **Preserved comments**<br>
 Untouched in their original HTML format

<!--
# Edge cases covered
* banned posts
* thread/post/file deletions
* massive threads consisting of thousands of posts
  -->

## Runtime dependencies
* [PostgreSQL](https://www.postgresql.org/download/) >= 11.0

## Getting Started
Download the [latest][latest-link] release binaries or build it from source.  

You can simply build for your host machine as follows:
```bash
git clone https://github.com/shiimizu/ena.git
cd ena
cargo build --release
```
Grab the binary from `target/release/`. For more information, see the [building guide](https://github.com/shiimizu/ena/wiki/Building).  

Set your environment variable to `ENA_LOG=ena=info` for console output. 

Edit the `ena_config.json` file and put in your database connection details, media directory, what boards you want to archive, and tweak any of the other settings that look interesting. Don't go below 0.12s or so for the ratelimit or you'll get banned. Follow the API rules and keep it at or above 1 unless you really need to (If you're not sure whether you need to, you probably don't need to). For more information, see the [configuration page](https://github.com/shiimizu/ena/wiki/Configuration).

Make sure your PostgreSQL server is running.  

You should now be able to run `ena` and have it start archiving, and report status to the standard output, showing requests as they happen, as well as a display of current queued tasks.  
Ctrl-C will stop Ena. To leave Ena running long term, you can use screen (or byobu or any such tool).

## Why do this?
Much of my personal time and research went into 4chan for *educational purposes* and self development. I value the things I've learned there and have a plethora of threads saved. Archival sites have been crumbling down due to requiring several tens of gigabytes to keep Asagi archiving every board. At the time there wasn't any avaliable practical solutions nor were they production ready, so I decided to help out.

## Why Rust?
I wanted something fast, durable, and ideally able to withstand long-term usage. Rust has memory safety guarantee, no GC, speed like C, and an ecosystem like Python's. Paying upfront in development time in return for less debugging and runtime errors is a small price to pay. Generally, if it compiles, you can be rest assured there won't be any low level hiccups as would be in other languages. For more reasons, see the [Why Rust? page](https://github.com/shiimizu/ena/wiki/Why-Rust%3F).

## What's with the name?
> Asagi is the eldest of the Ayase sisters. Fuuka is the middle sister. The Ayase family lives next door to Yotsuba. Get it?  
> – *ekopsl*  

Ena is the next in line and youngest of the Ayase sisters.

