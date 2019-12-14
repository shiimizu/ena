#![allow(dead_code)]
#![feature(type_ascription)]

#![allow(unused_imports)]
#![allow(unused_must_use)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]
#![deny(unsafe_code)]
#![feature(exclusive_range_pattern)]
#![allow(unused_assignments)]

#![recursion_limit="1024"]
#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;

extern crate pretty_env_logger;
#[macro_use] extern crate log;

use std::error::Error;
//use std::fs::File;
//use std::io::BufReader;
use std::path::Path;

use std::time::{SystemTime, Duration, UNIX_EPOCH, Instant};
use std::thread::sleep;

use reqwest;
use reqwest::header::{HeaderMap, HeaderValue, LAST_MODIFIED, USER_AGENT, CONTENT_TYPE, IF_MODIFIED_SINCE,CACHE_CONTROL};

extern crate chrono;
use chrono::prelude::{DateTime, Local};
use chrono::Utc;

use std::thread;
//use std::sync::{Mutex, Arc, Barrier};
extern crate pipeliner;
use pipeliner::Pipeline;

use crossbeam_utils;
use crossbeam_utils::sync::WaitGroup;
use crossbeam_queue::{PopError, SegQueue};
use crossbeam_channel::bounded;
use crossbeam_utils::thread::scope;

use postgres::{Connection, TlsMode};
use async_std::pin::Pin;
use futures::{
    future::{Fuse, FusedFuture, FutureExt},
    stream::{self, FusedStream, FuturesUnordered, Stream, StreamExt},
    channel::oneshot,
    pin_mut,
    select,
};
use async_std::fs::File;
use async_std::io::BufReader;
use async_std::prelude::*;
use async_std::task;
use async_std::sync::{Arc, Mutex, Barrier};
use rand::Rng;
use rand::distributions::{Distribution, Uniform};

use async_log::span;
use log::info;

/*
fn read_file(path: &str) -> io::Result<Vec<String>> {
    let file = fs::File::open(path)?;
    let file = io::BufReader::new(file);
    file.lines().collect()
}

fn timestamp(path: &str) -> io::Result<String> {
    let metadata = fs::metadata(path)?;
    let filetime: DateTime<Local> = DateTime::from(metadata.modified()?);
    Ok(filetime.format("%Y-%m-%d %H:%M:%S.%f %z").to_string())
}*/
async fn compile_threads_outside_generator(threads: Vec<u32>) -> Vec<u32> {
    threads
}
async fn compile_threads_outside_generator2(threads: &Vec<u32>, i:usize) -> u32 {
    threads[i]
}

async fn compile_threads_outside_generator3(i:u32) -> u32 {
    i
}
async fn read_file(path: &str) -> async_std::io::Result<String> {
    let mut file = File::open(path).await?;
    /*let mut reader = BufReader::new(&file);
    
    let mut filee = std::fs::File::open(path).expect("file should open read only");
    let mut reader = std::io::BufReader::new(&mut filee);
    let s : serde_json::Value = serde_json::from_reader(&mut reader).expect("file should be proper JSON");
    println!("{:?}", reader);*/
    
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;
    Ok(contents)
    /*
    //println!("Trying to read JSON");
    let result = read_file("570368.json").await;
    match result {
        Ok(s) => {},//println!("{:?}", serde_json::from_str::<serde_json::Value>(&s).unwrap()),
        Err(e) => println!("Error reading file: {:?}", e)
    };
    */
}
async fn run_on_new_num(
    num: u8
) -> u8 {
    num
}
async fn get_this(
    num: String
) -> String {
    num
}
async fn get_new_num(
) -> u8 { 3
}
async fn run_loop(
    mut interval_timer: impl Stream<Item = ()> + FusedStream + Unpin,
    starting_num: u8,
) {
    let mut run_on_new_num_futs = FuturesUnordered::new();
    run_on_new_num_futs.push(run_on_new_num(starting_num));
    let get_new_num_fut = Fuse::terminated();
    pin_mut!(get_new_num_fut);
    loop {
        select! {
            () = interval_timer.select_next_some() => {
                // The timer has elapsed. Start a new `get_new_num_fut`
                // if one was not already running.
                if get_new_num_fut.is_terminated() {
                    get_new_num_fut.set(get_new_num().fuse());
                }
            },
            new_num = get_new_num_fut => {
                // A new number has arrived-- start a new `run_on_new_num_fut`.
                run_on_new_num_futs.push(run_on_new_num(new_num));
            },
            // Run the `run_on_new_num_futs` and check if any have completed
            res = run_on_new_num_futs.select_next_some() => {
                println!("run_on_new_num_fut returned {:?}", res);
            },
            // panic if everything completed, since the `interval_timer` should
            // keep yielding values indefinitely.
            complete => panic!("`interval_timer` completed unexpectedly"),
        }
    }
}

fn setup_async_logger() {
    let logger = femme::pretty::Logger::new();
    async_log::Logger::wrap(logger, || 12)
        .start(log::LevelFilter::Debug)
        .unwrap();
}
#[async_std::main]
async fn main() {
    //pretty_env_logger::init();
    //setup_async_logger();
    //$ENV:RUST_LOG="trace,debug,info,warn,error"
    //$ENV:RUST_LOG="trace"
    println!("Start {}", current_time().to_rfc2822());
    
    
    let api_endpoints = vec!["https://a.4cdn.org/boards.json","https://a.4cdn.org/po/threads.json", "https://a.4cdn.org/po/catalog.json", "https://a.4cdn.org/po/archive.json", "https://a.4cdn.org/po/3.json", "https://a.4cdn.org/po/thread/570368.json"];
    
    let thread_update_delay: [i8; 10] = [10,15,20,30,35,40,45,50,55,60];
    let mut thread_update_delay_ = (2..=12).map(|x| x*5).collect::<Vec<u8>>();
    
    let client = reqwest::Client::new();
    let len :usize= api_endpoints.len();
    let ratelimit = Duration::from_millis(1000);
    
    let db = Database {
        conn: Connection::connect("postgresql://postgres:zxc@localhost:5432/myDB", TlsMode::None).expect("Error connecting")
    };
    
    let now = Instant::now();


    /*let mut incomplete_threads: Vec<u32> = serde_json::from_value(
            db.get_incomplete_threads().unwrap().get(0).get(0)
            ).unwrap();
    let incomplete_threads : Vec<u32>= incomplete_threads.drain(1..).collect();
    println!("{:?}",incomplete_threads);
    */
    println!("{}", &db.index_at_thread(1239).unwrap());
    println!("Execution ran successfully in {} msec.", now.elapsed().as_millis());
    /*match &db.get_incomplete_threads() {
        Ok(q) => {
            //println!("Execution ran successfully in {} msec.", now.elapsed().as_millis());
            for row in q {
                let r : serde_json::Value = row.get(0);
                //println!("{}",serde_json::to_string(&r).unwrap());
               let vv:Vec<u32> = serde_json::from_value(r).unwrap();
                println!("{:?}",vv);
                //println!("po\n{:?}", row);
            }
        },
        Err(e) => println!("{:?}", e),
    }*/
    // whewre threads arent archived or have media without hashes
    // + archives.json + threads.json

    /*let mut rng = rand::thread_rng();
    let die = Uniform::from(1..=3);
    let list = vec![api_endpoints[1],api_endpoints[3]];
    let fut = stream::iter(list).for_each_concurrent (
        // Inside this block represents a single thread
        /* limit */ None,
        |rx| async move {
            let mut c:usize = 0;
            let mut last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
            loop {
                //println!("\nThread {} Counter {}", rx, c);
                println!("{}", rx);
                c +=1;
                
                // Reducer: Simulate API call, All threads must obey
                // https://docs.rs/async-std/1.2.0/async_std/future/trait.Future.html#tymethod.poll
                async_std::future::ready(1).delay(Duration::from_millis(1000)).await;
        
        
                  
                 //   task::sleep(Duration::from_secs(1)).await;
                
                //debug!("Thread {} Counter {} Now processing", rx, c);
                
                
                // On each task
                // Simulate work
                let dd =die.sample(&mut rng);
                let rr  =rx.clone();
                task::spawn(async move {

                    loop {
                    println!("Thread > {} API CALL",current_time());
                    async_std::future::ready(1).delay(Duration::from_millis(7000)).await;
                    println!("Thread > {} DONE {}",current_time(), rx);
                    task::sleep(Duration::from_secs(dd)).await;

                    }

                });
            }
        }
        
    );
     task::block_on(async {
        fut.await; 
        })
*/
/*
let start = Instant::now();

// emit value every 5 milliseconds
let s = stream::interval(Duration::from_millis(5));

// throttle for 10 milliseconds
let mut s = s.throttle(Duration::from_millis(10));

s.next().await;
println!("{:?}", start.elapsed().as_millis() );

s.next().await;
println!("{:?}", start.elapsed().as_millis() );*/

/*
use async_std::sync::channel;
use async_std::task;


let (s, r) = channel(1);

s.send(current_time().timestamp_millis()).await;

let rr= &r;
let ss= &s;
let cclient= &client;
let tthread_update_delay = &thread_update_delay_;

let list = vec![api_endpoints[1],api_endpoints[3]];*/
// Run concurrently unti lcompletion
use reqwest::header;
let mut headers = header::HeaderMap::new();
headers.insert(header::AUTHORIZATION, header::HeaderValue::from_static("secret"));

//println!("Starting seperate thread with ‭14,632 concurrent async‬ tasks");
// Do all work in a background thread to keep the main ui thread running smoothly
// This essentially keeps CPU and power usage extremely low 
use futures::{future, select};
use futures::stream::{StreamExt, FuturesUnordered};
thread::spawn(move || {
    let mut rng = rand::thread_rng();
    let die = Uniform::from(1..=3);
    


    // By using async tasks we can process an enormous amount of tasks concurrently without the performance hit and resources
    // if it was done with threads. 
    task::block_on(async {
        let client_builder = reqwest::Client::builder()
                    //.cookie_store(true)
                    .proxy(reqwest::Proxy::http("http://my.prox").unwrap())
                    .timeout(Duration::from_secs(5))
                        .connect_timeout(Duration::from_secs(5));
        /*use async_std::prelude::*;
        use async_std::stream;
        let timer = stream::interval(Duration::from_secs(4)).fuse();*/
        /*let mut fut = future::ready(1);
        let mut async_tasks = FuturesUnordered::new();
        let mut threads_fut = FuturesUnordered::new();
        let get_new_num_fut = Fuse::terminated();
        pin_mut!(get_new_num_fut);
        let mut total = 0;*/

        let mut incomplete_threads_fut = future::ready(());
        let mut archives_interval = FuturesUnordered::new();
        let mut threads_interval = FuturesUnordered::new();
        let mut process_threads_futs = FuturesUnordered::new();
        let mut process_thread_futs = FuturesUnordered::new();

        use futures::executor::block_on_stream;
        use futures::stream::{self, StreamExt};
        use futures::task::Poll;
        let one_sec = Duration::from_secs(1);
        let two_sec = Duration::from_secs(2);
        let one_millis = Duration::from_millis(1);
        let ten_sec = Duration::from_secs(10);
        let sixty_sec = Duration::from_secs(60);
        //let one_msec = Duration::from_millis(10);
        //let mut stream_interval = FuturesUnordered::new();
        archives_interval.push(task::sleep(one_millis));
        threads_interval.push(task::sleep(one_millis));


        /*
        let mut proxy_stream = ProxyStream::new();
        &proxy_stream.urls.push("1230.41.0243".to_string());
        &proxy_stream.urls.push("1230.41.43535".to_string());
        &proxy_stream.urls.push("1230.41.02342e43".to_string());
        println!("{}",proxy_stream.next().await.unwrap());
        println!("{}",proxy_stream.next().await.unwrap());*/

        /*let mut i =0;
        let one_sec = Duration::from_secs(1);
    let mut futures = FuturesUnordered::new();
        futures.push(task::sleep(one_sec));
        futures.push(task::sleep(one_sec));
        futures.push(task::sleep(one_sec));
        futures.push(task::sleep(one_sec));
        futures.push(task::sleep(one_sec));
        futures.push(task::sleep(one_sec));
        futures.push(task::sleep(ten_sec));
          while let Some(value_returned_from_the_future) = futures.next().await {
            i+=1;
           println!("Sleeping , no: {:?}", i);
        task::sleep(one_sec);

        }*/

//let mut stream_interval = (0..10)
//        .map(|_| async {task::sleep(one_sec).await;  ()})
//        .collect::<FuturesUnordered<_>>();
//let mut stuff = FuturesUnordered::new();
//(0..3).for_each(|x| stuff.push(task::sleep(Duration::from_secs(1)))
//    );
        // https://rust-lang.github.io/async-book/06_multiple_futures/03_select.html
        // https://docs.rs/futures/0.3.1/futures/stream/trait.StreamExt.html#method.select_next_some
        // https://www.philipdaniels.com/blog/2019/async-std-demo1/

        //let thread_list = Arc::new(Mutex::new(vec![]));
    span!("main, depth={}", 1, {
    info!("Start loop");
        loop {
            select! {
                () = incomplete_threads_fut => {
                    span!("incomplete_threads_fut, depth={}", 2, {
                        debug!("GET incomplete threads if any");
                    });
                    task::sleep(one_sec).await; // Simulate getting from DB
                    let incomplete_threads = vec![1,2,3,4]; // If any, push
                    process_threads_futs.push(compile_threads_outside_generator(incomplete_threads).fuse());
                },
                () = archives_interval.select_next_some() => {
                    // Time has elapsed
                    // Load cache first
                    span!("archives_interval, depth={}", 2, {
                        debug!("GET archive.json");
                    });
                    //task::sleep(one_sec).await; // Here we use channels to sync API calls
                    let mut tt: Vec<u32> = serde_json::from_value(
                                            serde_json::from_str::<serde_json::Value>(&read_file("archive.json").await.unwrap()).unwrap()
                                        ).unwrap();
                    //let tt = vec![7,8,9,10];
                    process_threads_futs.push(compile_threads_outside_generator(tt).fuse());
                    // README. the async is awaited FIRST then this whole block is run
                    // So that's why we do this
                    // push to recurse
                    // sleep tail rather than sleep head. (tail recursion)
                    archives_interval.push(task::sleep(ten_sec)); 
                    //task::sleep(sixty_sec).await;
                },
                () = threads_interval.select_next_some() => {
                    span!("threads_interval, depth={}", 2, {
                        debug!("GET threads.json");
                    });
                    // Load cache first
                    //task::sleep(one_sec).await; // Here we use channels to sync API calls
                    let tt = vec![10,20,30,40];
                    process_threads_futs.push(compile_threads_outside_generator(tt).fuse());
                    threads_interval.push(task::sleep(ten_sec));
                    //task::sleep(sixty_sec).await;
                },
                mut threads = process_threads_futs.select_next_some() => {
                    if threads.len() < 5 {
                        span!("process_threads_futs, depth={}", 2, {
                            info!("Received threads {:?}", threads);
                        });

                    }


                    let mut cf = threads.iter()
                        .map(|&url| compile_threads_outside_generator3(url))
                        .collect::<FuturesUnordered<_>>();
                        process_thread_futs.push(cf);
                        /*for l in threads.drain(..) {
                            process_thread_futs.push(compile_threads_outside_generator3(l).fuse());
                        }*/

                    //process_thread_futs.push(futures::stream::iter(threads).fuse());
                    //threads.iter().map( |&t| process_thread_futs.push(async{t.to_owned()}));
                },
                thread = process_thread_futs.select_next_some() => {
                    //println!("{:?} processing thread", thread);
                    span!("process_thread_futs, depth={}", 3, {
                        task::block_on(async {
                                while let return_val = cf.next().await {
                                    span!("process_thread_futs, depth={}", 4, {
                                        warn!("Processing thread {}", return_val);
                                        task::sleep(one_sec).await;
                                    });
                                }
                            });
                        //task::spawn(async move{ // to have non blocking sleep
                            // each thread has its own thread to have all parallel
                            
                        //println!("{:?} processing thread", thread);
                        //});
                    });



                },
                complete => panic!("intervals completed unexpectedly")
            }
        }
    });

        });
    
}).join().unwrap();


println!("Done");

/*let fut = futures::stream::iter(list).for_each_concurrent (
        // Inside this block represents a single thread
        /* limit */ None,
        |rx| async move {
            let url = rx;
            let mut last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
            let mut c = 0;
            loop {
                // Channels will sync threads
                // Between reach API call is the ratelimit
                // https://docs.rs/async-std/1.2.0/async_std/sync/fn.channel.html
                rr.recv().await;
                task::sleep(Duration::from_millis(1100)).await;
                ss.send(current_time().timestamp_millis()).await;
                
                
                let res = cclient.get(url)
                .headers(construct_headers(last_modified.as_str()))
                .send().unwrap();
                
                // Resume multithreading here

                // For some reason need to get this to get the correct status?
                let resp_last_modified = res.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap();
                let status = res.status();
                match  status {
                    reqwest::StatusCode::NOT_MODIFIED => {
                        let delay = tthread_update_delay[c];
                        let len = tthread_update_delay.len();
                        c += 1;
                        if c >= len {
                            c = 0;
                        }
                        if delay == tthread_update_delay[len-1] {
                            c = len-1;
                        }
                    
                        println!("GET {} {} Waiting {}s", url, &status, delay);
                        task::sleep(Duration::from_secs(delay.into())).await;
                        //sleep(Duration::from_secs(delay.into()));
                    },
                    _ => {
                        println!("GET {} {} Processing",url, status);
                        last_modified.clear();
                        last_modified.push_str(res.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap());
                        
                        
                        task::sleep(Duration::from_secs(2)).await;
                        //sleep(Duration::from_secs(2)); // Simulate work
                    },
                };
            }
        });

     task::block_on(async {
        fut.await; 
        });
r.recv().await;*/

    // api call queue => waiting for last api call to finish
    /*
    
    Ratelimit
    Get boards
       Archived same as threadlist
       Spawn for Threadlist & check
          Before spawning,
          check if in db or in-db not archived, reduce, new list
          Spawn async task to poll
              If-Modified-Since
              On not modified:
                throttle with delay
              On MODIFIED
                On archived or closed or sticky 304
                   commit to db, exit loop
                Commit to that db
                DL Media and commit to db for each post without sha, to prevent data loss if program suddenly stops, we'll still have it there
                    Commit to other media table -> id, sha256sum key
    
    If ignore media/thumbs, dont dl them and dont check for them in db
    On the fly options: https://github.com/davidMcneil/unified-dynamic-rust-app-config
    Handle other http codes, maybe blocked, but data is there
    
    if !db.has_boards() {
        async {
            let boards_str = get_boards(); //json_str
            db.init_boards(boards_str); //inserts threads if doesn't exist
        }.await
    }
    let boards = get_boards().json().boards;
    // Read settings to get which boards to archive
    let boards_to_archive = vec!["3","a","po"];
    
    // Archived
    let archive_task = async {
        loop {}
    }
    let threads_task = async {
        loop {
            for each thread {
                if !in db || in-db and not archived || post has no sha256 {
                    spawn_async {
                        let current_time = get_current_time();
                        loop {
                            // Lock and sync with other async tasks
                            sleep(reatelimit);
                            
                            // Resume multithreading here
                            let resp = DL.with_headers(current_time)// respect If-Modified-Since
                            
                            let res = cclient.get(*url)
                            .headers(construct_headers(last_modified.as_str()))
                            .send().unwrap();
                            
                            // For some reason need to get this to get the correct status?
                            let resp_last_modified = res.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap();
                            let status = res.status();
                            match status {
                                STATUSCODE::NOT_MODIFIED => {
                                    let delay = tthread_update_delay[c];
                                    let len = tthread_update_delay.len();
                                    c += 1;
                                    if c >= len {
                                        c = 0;
                                    }
                                    if delay == tthread_update_delay[len-1] {
                                        c = len-1;
                                    }
                                
                                    println!("GET {} {} Waiting {}s", url, &status, delay);
                                    
                                    sleep(Duration::from_secs(delay.into()));
                                }
                                _ => {
                                    db.commit();
                                    let media_queue = vec![];
                                    for i in 0..posts.len() {
                                        if has posts[i].media && not posts[i].sha256 {
                                            media_queue.push(async{
                                                download(media_link);
                                            });
                                        }
                                    }
                                    
                                    for media_task in media_queue {
                                        join!() / run concurrently
                                        db.commit();
                                        db.commit_media();
                                    }
                                    
                                    // Once it gets archived, it'll be modified and enter this block
                                    if json.archived or json.closed or sticky 304 {
                                        commit to db, exit loop
                                    }
                                
                                }
                            }
                            
                            
                               
                        }
                        
                    }
                }
            
            }
            
            
            
        }
    }
    join!(archive_task, );
    boards.iter().position(|&x| x == "po").unwrap()
    
    
    SQL null
    Sync between db upserts
    On the fly mods
    Rotating Proxies,UA to DL faster and bypass ratelimiting
    Also have to handle CloudFlare
    
    **Friendly:** Aims to be easy to use, easy to understand, encourage development, allow to be easily adopted for anyone.
    */
    
    
    
    /*
    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let (tx3, rx3) = oneshot::channel();
    // Spawn n threads and runs them concurrently until completion
    let fut = stream::iter(0..3).for_each_concurrent (
        // Inside this block represents a single thread
        /* limit */ None,
        |rx| async move {
        let mut c:usize = 0;
        //let result = read_file("570368.json").await;
        loop {
            
            println!("Thread {} Counter {}", rx, &c);
            c += 1;
            //String::from("TESTING THIS STRING 981237604871");
            
            //println!("Trying to read JSON");
            /*match &result {
                Ok(s) => {serde_json::from_str::<serde_json::Value>(&s).unwrap();},
                Err(e) => println!("Error reading file: {:?}", e)
            };*/
            task::sleep(Duration::from_secs(1)).await;
        }
            //;
        }
    );
    tx1.send(()).unwrap();
    tx2.send(()).unwrap();
    tx3.send(()).unwrap();
    fut.await;*/
    
    
    // Use boards.json to get list of boards
    // Use /<board>/threads.json because sometimes a thread isn't in Catalog
    // Don't use Catalog
    // Use archive.json cause once a thread is archived, won't show it threads.json
    // Use <board>/thread/<op_no>.json for media and posts in a thread
    // For threads, handle post deletions
    
    // Media
    // Hash flexibility
    // Folder structure
    
    // BoardsList -> ThreadsList -> Each Thread: Poll (thread_update_delay)
    //                              -> Insert to jsonb
    //                              -> Poll for updates
    //                              -> If 304, gone -> Set to archived and Done
    //                              -> Check archived, if yes -> Done
    // Archived list -> Make sure we have these in the DB cause they won't show up
    // in threads.json
    //
    // Each Post: -> Media
    //            -> filenames as hashes
    //            -> dir structure
    // Check deleted media
    
    
    
    //let file_1 = "570368.json";
    //let file_2 = "5other.json";
    //let context_radius = 3;

    //let text1 = read_file(file_1).map_err(NError::Reading).unwrap();
    //let text2 = read_file(file_2).map_err(NError::Reading).unwrap();

    //let mut patch_str = String::new();
    /*
    patch_str.push_str(format!("--- {}\t{}\n", file_1, timestamp(file_1).map_err(NError::Filesystem).unwrap()).as_str());
    patch_str.push_str(format!("+++ {}\t{}\n", file_2, timestamp(file_2).map_err(NError::Filesystem).unwrap()).as_str());

    for s in diff_rs::diff(&text1, &text2, context_radius).map_err(NError::Diff).unwrap() {
        patch_str += format!("{}\n", &s).as_str();
    }
    
    //println!("{}", patch_str);
    
    //println!("{}", &patch_str.as_str());
    let parser = PatchProcessor::converted(text1, &patch_str).map_err(NError::Patch).unwrap();
    let sv = parser.process().map_err(NError::Patch).unwrap();
    let ss: String = sv.into_iter().map(|mut s| { s += "\n"; s } ).collect();
    let ss = format!("{}\n", ss);
    println!("{}", ss);*/
    
    //let patch = Patch::from_single(&patch_str).unwrap();
    //println!("{}", format!("{}", patch));
    
    //let patch = Patch::from_single(patch_str.as_str()).unwrap();
    //println!("{}", format!("{}\n", patch));
    // https://docs.rs/crossbeam/0.7.3/crossbeam/channel/index.html
    /*let (s, r) = bounded(1);
    s.send(current_time().timestamp_millis()).unwrap();
    scope(|scope| {
        let mut threads = vec![];
        let rr = &r;
        let ss = &s;
        let cclient = &client;
        let tthread_update_delay = &thread_update_delay_;
        for endpoint in &api_endpoints {
            // Spawn a thread that receives a message and then sends one.
            threads.push (
                scope.spawn(move |_| {
                    let url = endpoint;
                    let mut last_modified = String::from("Sun, 04 Aug 2019 00:08:35 GMT");
                    let mut c = 0;
                    loop {
                        // Blocking messages will sync threads
                        // Between reach API call is the ratelimit
                        rr.recv().unwrap();
                        sleep(ratelimit);
                        ss.send(current_time().timestamp_millis()).unwrap();
                        
                        // Resume multithreading here
                        
                        let res = cclient.get(*url)
                        .headers(construct_headers(last_modified.as_str()))
                        .send().unwrap();
                        
                        // For some reason need to get this to get the correct status?
                        let resp_last_modified = res.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap();
                        let status = res.status();
                        match  status {
                            reqwest::StatusCode::NOT_MODIFIED => {
                                let delay = tthread_update_delay[c];
                                let len = tthread_update_delay.len();
                                c += 1;
                                if c >= len {
                                    c = 0;
                                }
                                if delay == tthread_update_delay[len-1] {
                                    c = len-1;
                                }
                            
                                println!("GET {} {} Waiting {}s", url, &status, delay);
                                
                                sleep(Duration::from_secs(delay.into()));
                            },
                            _ => {
                                println!("GET {} {} Processing",url, status);
                                last_modified.clear();
                                last_modified.push_str(res.headers().get(LAST_MODIFIED).unwrap().to_str().unwrap());
                                
                                
                                sleep(Duration::from_secs(2)); // Simulate work
                            },
                        };
                    }
                })
            );
        }
        
        // Ensure thread executes completely
        for thread in threads {
            thread.join().unwrap();
        }
        
        // Send a message and then receive one.
        //s.send(1).unwrap();
        //println!("{:?}", r.recv().unwrap())
        
    }).unwrap();*/
    
    // https://is.gd/VkNOl7
    // How Last-Modified and If-Modified-Since works
    
    //println!{"{:?}","Sun 04 Aug 2019 00:08:36 Z".parse::<DateTime<Utc>>()};
    /*let custom = DateTime::parse_from_str("Sun 04 Aug 2019 00:08:36 GMT", "%d %M %Y %H:%M %P %z");
    match custom {
        Ok(c) => println!("{}", c),
        Err(e) => {}
    }*/
    
    //task::sleep(Duration::from_millis(1000)).await;
    //sleep(Duration::from_secs(5)); // Simulate work
    //sys_time.elapsed().unwrap() >= one_sec
    //let boards = read_json("boards.json");
    //println!("{}", boards.prettify().unwrap());
    
    // 140 threads * 72 boards + archived threads (4552 from /a/) = ‭14632‬
    /*loop{}
    scope(|scope| {
        
            let mut tthreads = vec![];
            (0..10080).for_each(|_| {
                tthreads.push(thread::spawn(move || {

                    loop {
                        sleep(Duration::from_secs(1));
                    }
                }));
            });
            for th in tthreads {
                th.join().unwrap();
            }
            //sleep(Duration::from_secs(2));
        
    });
    */
    /*
    let json1 = read_json(file_1);
    let json2 = read_json(file_2);
    let u = read_user_from_file(&file_1).unwrap();
    //println!("{:#?}", u);
    
    println!("{:#?}", json1.get("posts").unwrap().as_array().unwrap()[0].get("capcode"));*/
    
    //let mb:MinimalBoards = read_user_from_file("boards.json").unwrap();
    //println!("{:#?}", mb.boards[0].board);
}
/*
trait GenericDatabase {
    
}
*/


/// A cycle stream that can append new values
#[derive(Debug)]
struct ProxyStream {
    urls: Vec<String>,
    count: usize,
}

// we want our count to start at one, so let's add a new() method to help.
// This isn't strictly necessary, but is convenient. Note that we start
// `count` at zero, we'll see why in `next()`'s implementation below.
impl ProxyStream {
    fn push(mut self, s: String) -> ProxyStream{
        self.urls.push(s);
        self
    }
    fn new() -> ProxyStream {
        ProxyStream { urls: vec![], count: 0 }
    }
}

// Then, we implement `Stream` for our `Counter`:

impl Stream for ProxyStream {
    // we will be counting with usize
    type Item = String;
    // poll_next() is the only required method
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut async_std::task::Context<'_>) -> async_std::task::Poll<Option<Self::Item>> {
        // Increment our count. This is why we started at zero.
        //self.urls.push("".to_string());
        self.count += 1;
        let mut c = 0;
        let max = self.urls.len();
        match self.count {
            0 => { c = 0 },
            x if x >= 1 && x < self.urls.len() => { c = x },
            _ => {
                self.count = 0;
                c = 0;
            }
        }
        async_std::task::Poll::Ready(Some(self.urls[c].to_owned()))
        
    }
}

#[derive(Debug)]
struct Database {
    conn: Connection
}

impl Database {

    fn init_boards(self, boards_json_str:String) -> Result<u64, postgres::error::Error> {
        let sql = r#"
        select jsonb_build_object('boards', 
                          jsonb_object_agg(board::text, val::jsonb || '{"threads":{}}'::jsonb)
                         ) from (
        select 
        jsonb_array_elements('{boards_json_str}'::jsonb->'boards')::jsonb->>'board' as board,
        jsonb_array_elements('{boards_json_str}'::jsonb->'boards') as val
        from bbtable where id=1
        ) x ;"#;
        self.conn.execute(&sql.replace("{boards_json_str}", &boards_json_str), &[])
    }
    
    fn patch_posts() {
        let sql = r#"
        SELECT jsonb_agg(
            COALESCE((oldv || newv), oldv))  from
        (select jsonb_array_elements(data->'posts') as oldv FROM bbtable where id=1) x
         left JOIN
        (select jsonb_array_elements(data->'posts') as newv FROM bbtable where id=2) z
        ON oldv->'no' <@ (newv -> 'no')"#;
    }
    
    fn patch_posts_new() {
        // FULL JOIN to get missing posts a new thread may have deleted
        // Combine into one with COALESCE
        // Another COALESCE to combine them into one but this one is baseing data from prev
        // Then concat the NEW with PREV so we preserve hashsums from our PREV
        let sql = r#"
        SELECT COALESCE(newv,oldv) || COALESCE(oldv,newv) from
        (select jsonb_array_elements(data->'posts') as oldv FROM main where id=3) x
        FULL JOIN
        (select jsonb_array_elements(data->'posts') as newv FROM main where id=4) z
        ON  oldv->'no' = (newv -> 'no')"#;
    }

    fn get_deleted_and_modified_threads() {
        // Combine new and prev threads.json into one. This retains the prev threads (which the new json doesn't contain, meaning they're either pruned or archived).
        //  That's especially useful for boards without archives.
        // Use the WHERE clause to select only modified threads. Now we basically have a list of deleted and modified threads.
        // Return back this list to be processed.
        // Use the new threads.json as the base now.  
        let sql r#"
            SELECT jsonb_agg(COALESCE(newv,prev)->'last_modified') from
            (select jsonb_path_query(data, '$[*].threads[*]') as prev from main where id = 5)x
            full JOIN
            (select jsonb_path_query(data, '$[*].threads[*]') as newv from main where id = 6)z
            ON prev->'no' = (newv -> 'no') 
            where newv is null or not prev->'last_modified' <@ (newv -> 'last_modified')
        "#;

    }



    fn update_thread(self, thread_no:String) -> Result<u64, postgres::error::Error> {
        let sql = r#"
        UPDATE bbtable
        set data = jsonb_set(data, '{boards,a,threads}',
                             data->'boards'->'a'->'threads'||'{"{thread_no}":{}}'::jsonb, true)
        where id=2;"#;
        self.conn.execute(&sql.replace("{thread_no}",&thread_no   ), &[])
    }
    fn convert_thread(self, thread:String, json_str: String) -> Result<postgres::rows::Rows, postgres::error::Error> {
        let sql = format!("select jsonb_build_object('{thread}', '{json_str}'::jsonb->'posts')", thread = thread, json_str = json_str);
        self.conn.query(&sql, &[])
    }
    fn get_incomplete_threads(self) -> Result<postgres::rows::Rows, postgres::error::Error> {
        let sql = "
        select jsonb_agg(dd->'no') from (
            SELECT jsonb_path_query(data, '$.boards[*].threads[*].posts[*]') as dd
                                          FROM bbtable where id =4
                    )x where not dd ? 'archived' or dd->>'archived'=0::text or 
                    (dd ? 'md5' and not dd ? 'sha256') or
                    (dd ? 'md5' and not dd ? 'sha256t')
        ";
        self.conn.query(&sql, &[])
    }
    
    fn get_incomplete_threads_new_schema(self) -> Result<postgres::rows::Rows, postgres::error::Error> {
        //https://stackoverflow.com/a/28857901
        let sql = r#"
        select key as thread,posts as post --first_currency.key, first_currency.value
        from main t
             , jsonb_path_query(data, '$.boards[*] ? (@.board=="3") .threads') element
             , jsonb_each(element) first_currency
             , jsonb_path_query(first_currency.value, '$.posts[*]' ) as posts
        where id=1 and (    
                            (posts @> '{"resto":0}' and (not posts ? 'archived' or posts @> '{"archived":0}')) or 
                            (posts ? 'md5' and not posts ? 'sha256') or
                            (posts ? 'md5' and not posts ? 'sha256t'))
        "#;
        self.conn.query(&sql, &[])
    }
    fn index_at_thread(self, thread:u32) -> Option<i64> {
        let sql = r#"
        select t.i from jsonb_array_elements('{"threads": [ {"no":1235},{"no":1238},{"no":1239} ] } '::jsonb->'threads')
         WITH ORDINALITY AS t (v, i) where t.v::jsonb->'no' @> '{thread}';
        "#;
        let mut res: Option<i64> = None;
        for row in &self.conn.query(&sql.replace("{thread}",format!("{}",thread).as_str()),&[]).unwrap() {
            res = Some(row.get(0));
        }
        res
    }/*
    
SELECT jsonb_path_query_array(data, '$.boards[*].threads[*].posts[*] ? 
                              (@.archived == null || @.archived == 0 || 
                               @.sha256 == null || @.sha256t == null
                              ) .no') 
                              FROM bbtable where id =4;
    */
}

#[derive(Deserialize, Debug)]
struct User {
    fingerprint: Option<String>,
    location: Option<String>,
}
fn construct_headers(date: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"));
    headers.insert(IF_MODIFIED_SINCE, HeaderValue::from_str(date).unwrap());
    //headers.insert(CACHE_CONTROL, HeaderValue::from_static("private, must-revalidate, max-age=0"));
    headers
}
fn current_time() -> DateTime<Utc> {
    //let utc = Utc::now();
    //let local = Local::now();
    //let converted: DateTime<Local> = DateTime::from(utc);
    //let d = UNIX_EPOCH + current_time();
    //let datetime = DateTime::<Utc>::from(d);
    //SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
    Utc::now()
}

#[derive(Serialize, Deserialize, Debug)]
struct MinimalBoards {
    boards: Vec<MinimalBoard>
}
#[derive(Serialize, Deserialize, Debug)]
struct MinimalBoard {
    board: String
}

fn read_user_from_file<P: AsRef<Path>>(path: P) -> Result<MinimalBoards, Box<dyn std::error::Error>> {
    // Open the file in read-only mode with buffer.
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let u :MinimalBoards= serde_json::from_reader(reader)?;

    // Return the `User`.
    Ok(u)
}
/*
fn read_json(s: &str) -> serde_json::Value {
    let mut file = File::open(s).expect("file should open read only");
    let mut reader = BufReader::new(&mut file);
    serde_json::from_reader(&mut reader).expect("file should be proper JSON")
}*/

trait PrettyJson {
    fn prettify(&self) -> std::result::Result<std::string::String, serde_json::Error>;
}

impl PrettyJson for serde_json::Value {
    fn prettify(&self) -> std::result::Result<std::string::String, serde_json::Error> {
        serde_json::to_string_pretty(&self)
    }
}


/// All API calls need to ensure ratelimit
#[derive(Debug, Copy, Clone)]
struct API {
    previous_call_time: DateTime<Utc>,
}

impl API {
    
    fn new() -> API {
        API {
            previous_call_time: current_time(),
        }
    }
    
    fn check_position(&mut self) {
        Pause::untilb(Duration::from_millis
        (self.previous_call_time.timestamp_millis() as u64) + Duration::from_millis(1000));
        self.previous_call_time = current_time();
    }
    
    fn download_json(&mut self) {
        self.check_position();
        println!("Doing work");
    }
}

struct Pause {}
impl Pause {
    fn untilb(until_time: Duration) {
        println!("Pausing from {:?} to {:?}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap(), until_time);
        loop {
            if SystemTime::now().duration_since(UNIX_EPOCH).unwrap() > until_time {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    async fn until(until_time: Duration) -> bool {
        let mut res = false;
        loop {
            if SystemTime::now().duration_since(UNIX_EPOCH).unwrap() > until_time {
                break;
            }
            task::sleep(Duration::from_millis(1)).await;
            if !res {
                res = true;
            }
        }
        println!("done" );
        res
    }
}


/*
trait NoDeleteParser {
    fn process_no_delete(&self) -> PatchResult<Vec<String>>;
}
impl NoDeleteParser for PatchProcessor {

    fn process_no_delete(&self) -> PatchResult<Vec<String>> {
        let mut file2_text = Vec::new();
        let mut file1_ptr: usize = 0;

        for context in &self.patch.contexts {
            for i in file1_ptr..context.header.file1_l-1 {
                file2_text.push(
                    self.text
                        .get(i)
                        .ok_or_else(|| patch_rs::error::Error::AbruptInput(i))?
                        .to_owned(),
                );
            }
            file1_ptr = context.header.file1_l-1;
            for line in &context.data {
                match line {
                    Line::Context(ref data) => {
                        if self
                            .text
                            .get(file1_ptr)
                            .ok_or_else(|| Error::AbruptInput(file1_ptr))?
                            != data
                        {
                            return Err(Error::PatchInputMismatch(file1_ptr));
                        }
                        file2_text.push(data.to_owned());
                        file1_ptr += 1;
                    }
                    Line::Delete(ref data) => {
                        if self
                            .text
                            .get(file1_ptr)
                            .ok_or_else(|| Error::AbruptInput(file1_ptr))?
                            != data
                        {
                            return Err(Error::PatchInputMismatch(file1_ptr));
                        }
                        file1_ptr += 1;
                    }
                    Line::Insert(ref data) => {
                        file2_text.push(data.to_owned());
                    }
                }
            }
        }

        for i in file1_ptr..self.text.len() {
            file2_text.push(
                self.text
                    .get(i)
                    .ok_or_else(|| Error::AbruptInput(i))?
                    .to_owned(),
            );
        }

        Ok(file2_text)
    }
}*/