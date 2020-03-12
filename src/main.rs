#![forbid(unsafe_code)]
#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(irrefutable_let_patterns)]
#![allow(clippy::range_plus_one)]

mod config;
mod enums;
mod mysql;
mod pgsql;
mod request;
mod sql;

use crate::{config::{BoardSettings, Config},
            enums::*,
            request::*,
            sql::*};

use std::{collections::{HashMap, VecDeque},
          convert::TryFrom,
          env,
          path::Path,
          sync::{atomic::AtomicBool, Arc}};

use tokio::{runtime::Builder,
            sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
            time::{delay_for as sleep, Duration}};

use anyhow::{anyhow, Context, Result};
use chrono::Local;
use core::sync::atomic::Ordering;
use enum_iterator::IntoEnumIterator;
use futures::stream::{FuturesUnordered, StreamExt as FutureStreamExt};
use log::*;
use mysql_async::prelude::*;
use reqwest::{self, StatusCode};
use sha2::{Digest, Sha256};
// use ctrlc;
// use pretty_env_logger;
// use serde_json;

fn main() {
    println!(
             r#"
    ⣿⠟⣽⣿⣿⣿⣿⣿⢣⠟⠋⡜⠄⢸⣿⣿⡟⣬⢁⠠⠁⣤⠄⢰⠄⠇⢻⢸
    ⢏⣾⣿⣿⣿⠿⣟⢁⡴⡀⡜⣠⣶⢸⣿⣿⢃⡇⠂⢁⣶⣦⣅⠈⠇⠄⢸⢸
    ⣹⣿⣿⣿⡗⣾⡟⡜⣵⠃⣴⣿⣿⢸⣿⣿⢸⠘⢰⣿⣿⣿⣿⡀⢱⠄⠨⢸       ____ 
    ⣿⣿⣿⣿⡇⣿⢁⣾⣿⣾⣿⣿⣿⣿⣸⣿⡎⠐⠒⠚⠛⠛⠿⢧⠄⠄⢠⣼      /\  _`\
    ⣿⣿⣿⣿⠃⠿⢸⡿⠭⠭⢽⣿⣿⣿⢂⣿⠃⣤⠄⠄⠄⠄⠄⠄⠄⠄⣿⡾      \ \ \L\_     ___      __  
    ⣼⠏⣿⡏⠄⠄⢠⣤⣶⣶⣾⣿⣿⣟⣾⣾⣼⣿⠒⠄⠄⠄⡠⣴⡄⢠⣿⣵       \ \  __\  /' _ `\  /'__`\
    ⣳⠄⣿⠄⠄⢣⠸⣹⣿⡟⣻⣿⣿⣿⣿⣿⣿⡿⡻⡖⠦⢤⣔⣯⡅⣼⡿⣹        \ \ \___\/\ \/\ \/\ \L\.\_ 
    ⡿⣼⢸⠄⠄⣷⣷⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣕⡜⡌⡝⡸⠙⣼⠟⢱⠏         \ \____/\ \_\ \_\ \__/.\_\
    ⡇⣿⣧⡰⡄⣿⣿⣿⣿⡿⠿⠿⠿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣋⣪⣥⢠⠏⠄          \/___/  \/_/\/_/\/__/\/_/   v{}
    ⣧⢻⣿⣷⣧⢻⣿⣿⣿⡇⠄⢀⣀⣀⡙⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠂⠄⠄
    ⢹⣼⣿⣿⣿⣧⡻⣿⣿⣇⣴⣿⣿⣿⣷⢸⣿⣿⣿⣿⣿⣿⣿⣿⣰⠄⠄⠄
    ⣼⡟⡟⣿⢸⣿⣿⣝⢿⣿⣾⣿⣿⣿⢟⣾⣿⣿⣿⣿⣿⣿⣿⣿⠟⠄⡀⡀        A lightweight 4chan archiver (¬ ‿ ¬ )
    ⣿⢰⣿⢹⢸⣿⣿⣿⣷⣝⢿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠛⠉⠄⠄⣸⢰⡇
    ⣿⣾⣹⣏⢸⣿⣿⣿⣿⣿⣷⣍⡻⣛⣛⣛⡉⠁⠄⠄⠄⠄⠄⠄⢀⢇⡏⠄
    "#,
             config::version()
    );

    let start_time = Local::now();
    pretty_env_logger::try_init_timed_custom_env("ENA_LOG").unwrap();

    match Builder::new().enable_all().threaded_scheduler().build() {
        Ok(mut runtime) => runtime.block_on(async {
                                      if let Err(e) = async_main().await {
                                          error!("{}", e);
                                      }
                                  }),
        Err(e) => error!("{}", e)
    }

    info!("\nStarted on:\t{}\nFinished on:\t{}",
          start_time.to_rfc2822(),
          Local::now().to_rfc2822());
}

#[allow(unused_mut)]
async fn async_main() -> Result<u64, tokio_postgres::error::Error> {
    let (config, conn_url) = config::read_config("ena_config.json");

    // Find duplicate boards
    for a in &config.boards {
        let c = &config.boards.iter().filter(|&n| n.board == a.board).count();
        if *c > 1 {
            panic!("Multiple occurrences of `{}` found :: {}", a.board, c);
        }
    }

    if config.settings.asagi_mode && config.settings.engine != Database::MySQL {
        unimplemented!("Asagi mode outside of MySQL. Found {:?}", &config.settings.engine)
    }

    // info!("{}", config.pretty());
    // TODO
    // Asagi
    // https://rust-cli.github.io/book/tutorial/index.html
    // https://doc.rust-lang.org/edition-guide/rust-2018/trait-system/impl-trait-for-returning-complex-types-with-ease.html
    // debug!("{} {:#?}", &config.board_settings.board,&config.board_settings.board);
    // return Ok(0);

    // let pool = mysql_async::Pool::new(&conn_url);
    // let yb = YotsubaDatabase::new(pool);
    // let conn = yb.get_conn().await.unwrap();

    // let stmt = conn.prepare("select ?, ?").await.unwrap();
    // stmt.execute((1u8, 2u8))
    //     .await
    //     .unwrap()
    //     .for_each(|row| {
    //         let z: u8 = row.get(0).unwrap();
    //         let x: u8 = row.get(1).unwrap();
    //         println!("{} {}", z, x);
    //     })
    //     .await
    //     .unwrap();
    // yb.0.disconnect().await.unwrap();
    // info!("Connected with:\t{}", conn_url);
    // return Ok(1);

    let (db_client, connection) = tokio_postgres::connect(&conn_url, tokio_postgres::NoTls)
        .await
        .context(format!("\nPlease check your settings. Connection url used: {}", conn_url))
        .expect("Connecting to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {}", e);
        }
    });

    let http_client = reqwest::ClientBuilder::new()
        .default_headers(config::default_headers(&config.settings.user_agent).unwrap())
        .build()
        .expect("Err building the HTTP Client");

    let ss = pgsql::Schema::new();
    let archiver = YotsubaArchiver::new(http_client, db_client, config, ss).await;
    archiver.listen_to_exit();
    archiver.query.init_schema(&archiver.config.settings.schema).await;
    archiver.query.init_type(&archiver.config.settings.schema).await?;
    archiver.query.init_metadata().await;
    // let q = archiver.query.query("SELECT * FROM pg_type WHERE typname =
    // 'schema_4chan';", &[]).await.unwrap(); for z in q.iter() {
    //     let no: String = z.get("typname");
    //     println!("{:?}", no)
    // }

    sleep(Duration::from_millis(1200)).await;

    let mut boards: &Vec<BoardSettings> = &archiver.config.boards;

    // Push each board to queue to be run concurrently
    let archiver_ref = &archiver;
    let mut fut2 = FuturesUnordered::new();
    for board in boards.iter() {
        archiver_ref.query.init_board(board.board, &archiver_ref.config.settings.schema).await;
        archiver_ref.query.init_views(board.board, &archiver_ref.config.settings.schema).await;

        let (mut tx, mut _rx) = mpsc::unbounded_channel();
        if board.download_archives {
            fut2.push(archiver_ref.compute(YotsubaEndpoint::Archive,
                                           board,
                                           Some(tx.clone()),
                                           None));
        }
        fut2.push(archiver_ref.compute(YotsubaEndpoint::Threads, board, Some(tx.clone()), None));
        // fut2.push(archiver_ref.compute(YotsubaEndpoint::Media, board, None,
        // Some(_rx)));
    }

    // Waiting for this task causes a loop that's intended.
    // Run each task concurrently
    while let Some(_) = fut2.next().await {}

    Ok(0)
}

/// A struct to store variables without using global statics.
/// It also allows passing http client as reference.
pub struct YotsubaArchiver<H: request::HttpClient, S: sql::SchemaTrait> {
    client:   YotsubaHttpClient<H>,
    query:    tokio_postgres::Client,
    config:   Config,
    sql:      YotsubaSchema<S>,
    finished: Arc<AtomicBool>
}
impl<H, S> YotsubaArchiver<H, S>
    where H: request::HttpClient,
          S: sql::SchemaTrait
{
    async fn new(http_client: H, db_client: tokio_postgres::Client, config: Config, _sql: S)
                 -> Self {
        Self { client: YotsubaHttpClient::new(http_client),
               query: db_client,
               config,
               sql: sql::YotsubaSchema::new(_sql),
               finished: Arc::new(AtomicBool::new(false)) }
    }

    fn schema(&self) -> &str {
        &self.config.settings.schema
    }

    fn get_path(&self) -> &str {
        &self.config.settings.path
    }

    fn listen_to_exit(&self) {
        let finished_clone = Arc::clone(&self.finished);
        ctrlc::set_handler(move || {
            finished_clone.compare_and_swap(false, true, Ordering::Relaxed);
        }).expect("Error setting Ctrl-C handler");
    }

    async fn create_statements(&self, endpoint: YotsubaEndpoint, board: YotsubaBoard)
                               -> StatementStore {
        // This function is only called by fetch_board so it'll never be media.
        let mut statement_store: StatementStore = HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        /*let y = YotsubaIdentifier::Board(YotsubaBoard::a);
        match y {
            YotsubaIdentifier::Endpoint(v) => {},
            YotsubaIdentifier::Statement(v) => {},
            YotsubaIdentifier::Board(v) => {},
        }*/
        for statement in statements {
            statement_store.insert(YotsubaIdentifier { endpoint, board, statement },
                                   match statement {
                                       YotsubaStatement::UpdateMetadata =>
                                           self.query
                                               .prepare(self.sql
                                                            .update_metadata(self.schema(),
                                                                             endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .update_metadata(self.schema(),
                                       //                                            endpoint)
                                       //                           .as_str(),
                                       //                       &[tokio_postgres::types::Type::TEXT,
                                       //
                                       // tokio_postgres::types::Type::JSONB])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::UpdateThread =>
                                           self.query
                                               .prepare(self.sql
                                                            .update_thread(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .update_thread(self.schema(),
                                       //                                          board)
                                       //                           .as_str(),
                                       //
                                       // &[tokio_postgres::types::Type::JSONB])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::Delete =>
                                           self.query
                                               .prepare(self.sql
                                                            .delete(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .delete(self.schema(), board)
                                       //                           .as_str(),
                                       //
                                       // &[tokio_postgres::types::Type::INT8])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::UpdateDeleteds =>
                                           self.query
                                               .prepare(self.sql
                                                            .update_deleteds(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .update_deleteds(self.schema(),
                                       //                                            board)
                                       //                           .as_str(),
                                       //
                                       // &[tokio_postgres::types::Type::JSONB,
                                       //
                                       // tokio_postgres::types::Type::INT8])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::Medias =>
                                           self.query
                                               .prepare(self.sql
                                                            .medias(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .medias(self.schema(), board)
                                       //                           .as_str(),
                                       //
                                       // &[tokio_postgres::types::Type::INT8])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::Threads =>
                                           self.query.prepare(self.sql.threads()).await.unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql.threads(),
                                       //
                                       // &[tokio_postgres::types::Type::JSONB])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::ThreadsModified =>
                                           self.query
                                               .prepare(self.sql
                                                            .threads_modified(self.schema(),
                                                                              endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .threads_modified(self.schema(),
                                       //                                             endpoint)
                                       //                           .as_str(),
                                       //                       &[tokio_postgres::types::Type::TEXT,
                                       //
                                       // tokio_postgres::types::Type::JSONB])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::ThreadsCombined =>
                                           self.query
                                               .prepare(self.sql
                                                            .threads_combined(self.schema(),
                                                                              board,
                                                                              endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       //    self.query
                                       //        .prepare_typed(self.sql
                                       //                           .threads_combined(self.schema(),
                                       //                                             board,
                                       //                                             endpoint)
                                       //                           .as_str(),
                                       //                       &[tokio_postgres::types::Type::TEXT,
                                       //
                                       // tokio_postgres::types::Type::JSONB])
                                       //        .await
                                       //        .unwrap(),
                                       YotsubaStatement::Metadata =>
                                           self.query
                                               .prepare(self.sql
                                                            .metadata(self.schema(), endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       /*    self.query
                                        *        .prepare_typed(self.sql
                                        *                           .metadata(self.schema(),
                                        * endpoint)
                                        *                           .as_str(),
                                        *
                                        * &[tokio_postgres::types::Type::TEXT])
                                        *        .await
                                        *        .unwrap(), */
                                   });
        }
        statement_store
    }

    #[allow(unused_mut)]
    async fn compute(&self,
                     endpoint: YotsubaEndpoint,
                     board_settings: &BoardSettings,
                     // media_statement: &tokio_postgres::Statement,
                     mut tx: Option<UnboundedSender<u32>>,
                     mut rx: Option<UnboundedReceiver<u32>>)
    {
        match endpoint {
            YotsubaEndpoint::Archive | YotsubaEndpoint::Threads => {
                if let Some(t) = tx {
                    // loop {
                    let bs = board_settings.clone();
                    if self.fetch_board(endpoint, bs, &t).await.is_some() {};
                    // }
                }
            }
            YotsubaEndpoint::Media => {
                if let Some(mut r) = rx {
                    loop {
                        while let Some(thread) = r.recv().await {
                            // let bs = board_settings.clone();
                            self.fetch_media(board_settings, thread).await;
                        }
                        if self.finished.load(Ordering::Relaxed) {
                            break;
                        }
                        sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        }
    }

    async fn fetch_media(&self, info: &BoardSettings, thread: u32) {
        // FETCH MEDIA
        if !(info.download_media || info.download_thumbnails) {
            return;
        }
        let ms =
            self.query.prepare(self.sql.medias(self.schema(), info.board).as_str()).await.unwrap();
        // self.query
        //  .prepare_typed(self.sql.medias(self.schema(), info.board).as_str(),
        //                 &[tokio_postgres::types::Type::INT8])
        //  .await
        //  .unwrap();
        match self.query.medias(thread, &ms).await {
            Ok(media_list) => {
                let mut fut = FuturesUnordered::new();
                // let client = &self.client;
                let mut has_media = false;
                for row in media_list.iter() {
                    has_media = true;
                    // let no: i64 = row.get("no");

                    // Preliminary checks before downloading
                    let sha256: Option<Vec<u8>> = row.get("sha256");
                    let sha256t: Option<Vec<u8>> = row.get("sha256t");
                    let mut dl_media = false;
                    if info.download_media {
                        match sha256 {
                            Some(h) => {
                                // Improper sha, re-dl
                                if h.len() < (65 / 2) {
                                    dl_media = true;
                                }
                            }
                            None => {
                                // No thumbs, proceed to dl
                                dl_media = true;
                            }
                        }
                        if dl_media {
                            fut.push(self.dl_media_post2(row, info, thread, false));
                        }
                    }
                    let mut dl_thumb = false;
                    if info.download_thumbnails {
                        match sha256t {
                            Some(h) => {
                                // Improper sha, re-dl
                                if h.len() < (65 / 2) {
                                    dl_thumb = true;
                                }
                            }
                            None => {
                                // No thumbs, proceed to dl
                                dl_thumb = true;
                            }
                        }
                        if dl_thumb {
                            fut.push(self.dl_media_post2(row, info, thread, true));
                        }
                    }
                }
                if has_media {
                    let s = &self;
                    while let Some(hh) = fut.next().await {
                        if let Some((no, hashsum, thumb)) = hh {
                            if let Some(hsum) = hashsum {
                                // Media info
                                // if !thumb {
                                //     info!(
                                //         "{}/{} Upserting sha256: ({})",
                                //         &info.board,
                                //         no,
                                //         if thumb { "thumb" } else { "media" }
                                //     );
                                // }

                                s.query
                                 .update_hash(info.board,
                                              no,
                                              if thumb { "sha256t" } else { "sha256" },
                                              hsum)
                                 .await;
                            } else {
                                error!("Error unwrapping hashsum");
                            }
                        } else {
                            error!("Error running hashsum function");
                        }
                        // // Listen to CTRL-C
                        // if self.finished.load(Ordering::Relaxed) {
                        //     return;
                        // }
                    }
                }
            }
            Err(e) => error!("/{}/{}\tError getting missing media -> {}", info.board, thread, e)
        }
    }

    /// Downloads the endpoint threads
    async fn get_generic_thread(&self, endpoint: YotsubaEndpoint, bs: &BoardSettings,
                                last_modified: &mut String,
                                fetched_threads: &mut Option<Vec<u8>>,
                                local_threads_list: &mut VecDeque<u32>, init: &mut bool,
                                update_metadata: &mut bool, has_archives: &mut bool,
                                statements: &StatementStore)
    {
        if endpoint == YotsubaEndpoint::Archive && !*has_archives {
            return;
        }

        let current_board = bs.board;
        for retry_attempt in 0..(bs.retry_attempts + 1) {
            match self.client
                      .get(&format!("{domain}/{board}/{endpoint}.json",
                                    domain = &self.config.settings.api_url,
                                    board = current_board,
                                    endpoint = endpoint),
                           Some(last_modified))
                      .await
            {
                Ok((last_modified_new, status, body)) => {
                    if last_modified_new.is_empty() {
                        error!("({})\t/{}/\t<{}> An error has occurred getting the last_modified date",
                               endpoint, current_board, status);
                    } else if *last_modified != last_modified_new {
                        last_modified.clear();
                        last_modified.push_str(&last_modified_new);
                    }
                    match status {
                        StatusCode::OK => {
                            if body.is_empty() {
                                error!("({})\t/{}/\t<{}> Fetched threads was found to be empty!",
                                       endpoint, current_board, status);
                            } else {
                                info!("({})\t/{}/\tReceived new threads", endpoint, current_board);
                                *fetched_threads = Some(body.to_owned());

                                // Check if there's an entry in the metadata
                                if self.query.metadata(&statements, endpoint, current_board).await {
                                    let ena_resume =
                                        env::var("ENA_RESUME").ok()
                                                              .map(|a| a.parse::<bool>().ok())
                                                              .flatten()
                                                              .unwrap_or(false);

                                    // if there's cache
                                    // if this is a first startup
                                    // and ena_resume is false or thread type is archive
                                    // this will trigger getting archives from last left off
                                    // regardless of ena_resume. ena_resume only affects threads, so
                                    // a refetch won't be triggered.
                                    //
                                    // Clippy lint
                                    // if *init && (!ena_resume || (ena_resume && endpoint ==
                                    // YotsubaEndpoint::Archive))
                                    if *init
                                       && (endpoint == YotsubaEndpoint::Archive || !ena_resume)
                                    {
                                        // going here means the program was restarted
                                        // use combination of ALL threads from cache + new threads,
                                        // getting a total of 150+ threads
                                        // (excluding archived, deleted, and duplicate threads)
                                        if let Ok(mut list) = self.query
                                                                  .threads_combined(&statements,
                                                                                    endpoint,
                                                                                    current_board,
                                                                                    &body)
                                                                  .await
                                        {
                                            local_threads_list.append(&mut list);
                                        } else {
                                            info!("({})\t/{}/\tSeems like there was no modified threads at startup..",
                                                  endpoint, current_board);
                                        }

                                        // update base at the end
                                        *update_metadata = true;
                                        *init = false;
                                    } else {
                                        // Here is when we have cache and the program in continously
                                        // running
                                        // ONLY get the new/modified/deleted threads
                                        // Compare time modified and get the new threads
                                        let id = YotsubaIdentifier {
                                            endpoint: endpoint,
                                            board: current_board,
                                            statement: YotsubaStatement::ThreadsModified
                                        };
                                        match &statements.get(&id) {
                                            Some(statement_recieved) => {
                                                if let Ok(mut list) =
                                                    self.query
                                                        .threads_modified(current_board,
                                                                          &body,
                                                                          statement_recieved)
                                                        .await
                                                {
                                                    local_threads_list.append(&mut list);
                                                } else {
                                                    info!("({})\t/{}/\tSeems like there was no modified threads..",
                                                          endpoint, current_board)
                                                }
                                            }
                                            None =>
                                                error!("Statement: {} was not found!", id.statement),
                                        }

                                        // update base at the end
                                        *update_metadata = true;
                                        *init = false;
                                    }
                                } else {
                                    // No cache found, use fetched_threads
                                    if let Err(e) = self.query
                                                        .update_metadata(&statements,
                                                                         endpoint,
                                                                         current_board,
                                                                         &body)
                                                        .await
                                    {
                                        error!("Error running update_metadata function! {}", e)
                                    }
                                    *update_metadata = false;
                                    *init = false;

                                    match if endpoint == YotsubaEndpoint::Threads {
                                              self.query
                                                  .threads(&statements,
                                                           endpoint,
                                                           current_board,
                                                           &body)
                                                  .await
                                          } else {
                                              // Converting to anyhow
                                              match serde_json::from_slice::<VecDeque<u32>>(&body) {
                                                  Ok(t) => Ok(t),
                                                  Err(e) =>
                                                      Err(anyhow!("Error converting body to VecDeque for query.threads() {}",
                                                                  e)),
                                              }
                                          } {
                                        Ok(mut list) => local_threads_list.append(&mut list),
                                        Err(e) =>
                                            warn!("({})\t/{}/\tSeems like there was no modified threads in the beginning?.. {}",
                                                  endpoint, current_board, e),
                                    }
                                }
                            }
                        }
                        StatusCode::NOT_MODIFIED =>
                            info!("({})\t/{}/\t<{}>", endpoint, current_board, status),
                        StatusCode::NOT_FOUND => {
                            error!("({})\t/{}/\t<{}> No {} found! {}",
                                   endpoint,
                                   current_board,
                                   status,
                                   endpoint,
                                   if retry_attempt == 0 {
                                       "".into()
                                   } else {
                                       format!("Retry attempt: #{}", retry_attempt)
                                   });
                            sleep(Duration::from_secs(1)).await;
                            if endpoint == YotsubaEndpoint::Archive {
                                *has_archives = false;
                            }
                            continue;
                        }
                        _ => error!("({})\t/{}/\t<{}> An unforeseen event has occurred!",
                                    endpoint, current_board, status)
                    };
                }
                Err(e) => error!("({})\t/{}/\tError fetching the {}.json.\t{}",
                                 endpoint, current_board, endpoint, e)
            }
            if endpoint == YotsubaEndpoint::Archive {
                *has_archives = true;
            }
            break;
        }
    }

    /// Manages a single board
    async fn fetch_board(&self, endpoint: YotsubaEndpoint, bs: BoardSettings,
                         _t: &UnboundedSender<u32> /* not used because the 3rd thread for
                                                    * media dl is not used */)
                         -> Option<()>
    {
        // This function is only called by fetch_board so it'll never be media.

        let current_board = bs.board;
        let mut threads_last_modified = String::new();
        let mut local_threads_list: VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut init = true;
        let mut has_archives = true;
        let statements = self.create_statements(endpoint, current_board).await;

        let rd = bs.refresh_delay.into();
        let dur = Duration::from_millis(250);
        let ratel = Duration::from_millis(bs.throttle_millisec.into());

        loop {
            let now = tokio::time::Instant::now();

            // Download threads.json / archive.json
            let mut fetched_threads: Option<Vec<u8>> = None;
            self.get_generic_thread(endpoint,
                                    &bs,
                                    &mut threads_last_modified,
                                    &mut fetched_threads,
                                    &mut local_threads_list,
                                    &mut init,
                                    &mut update_metadata,
                                    &mut has_archives,
                                    &statements)
                .await;

            // Display length of new fetched threads
            let threads_len = local_threads_list.len();
            if threads_len > 0 {
                info!("({})\t/{}/\tTotal new/modified threads: {}",
                      endpoint, current_board, threads_len);
            }

            // Download each thread
            let mut position = 1_u32;
            while let Some(thread) = local_threads_list.pop_front() {
                let now_thread = tokio::time::Instant::now();
                self.assign_to_thread(&bs, endpoint, thread, position, threads_len, &statements)
                    .await;
                position += 1;

                // Download thumbnails
                // if bs.download_thumbnails {
                // self.fetch_media(&bs, thread, false).await;
                // }

                // Send to download full media
                // if bs.download_media {
                // t.send(thread).unwrap();
                // tokio::spawn(async move {
                self.fetch_media(&bs, thread).await;
                // });
                // }

                if self.finished.load(Ordering::Relaxed) {
                    return Some(());
                }
                // ratelimit
                tokio::time::delay_until(now_thread + ratel).await;
            }
            // Update the cache at the end so that if the program was stopped while
            // processing threads, when it restarts it'll use the same
            // list of threads it was processing before + new ones.
            if threads_len > 0 && update_metadata {
                if let Some(ft) = &fetched_threads {
                    if let Err(e) =
                        self.query.update_metadata(&statements, endpoint, current_board, &ft).await
                    {
                        error!("Error executing update_metadata function! {}", e);
                    }
                    update_metadata = false;
                }
            }
            //  Board refresh delay ratelimit
            while now.elapsed().as_secs() < rd {
                if self.finished.load(Ordering::Relaxed) {
                    return Some(());
                }
                sleep(dur).await;
            }
            if self.finished.load(Ordering::Relaxed) {
                break;
            }
        }
        Some(())
    }

    // Download a single thread and its media
    async fn assign_to_thread(&self, board_settings: &BoardSettings, endpoint: YotsubaEndpoint,
                              thread: u32, position: u32, length: usize,
                              statements: &StatementStore)
    {
        let board = board_settings.board;
        for _ in 0..(board_settings.retry_attempts + 1) {
            match self.client
                      .get(&format!("{domain}/{board}/thread/{no}.json",
                                    domain = &self.config.settings.api_url,
                                    board = board,
                                    no = thread),
                           None)
                      .await
            {
                Ok((_, status, body)) => match status {
                    StatusCode::OK =>
                        if body.is_empty() {
                            error!("({})\t/{}/{}\t<{}> Body was found to be empty!",
                                   endpoint, board, thread, status);
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            if let Err(e) =
                                self.query.update_thread(&statements, endpoint, board, &body).await
                            {
                                error!("Error executing update_thread function! {}", e);
                            }
                            match self.query
                                      .update_deleteds(statements, endpoint, board, thread, &body)
                                      .await
                            {
                                Ok(_) => info!("({})\t/{}/{}\t[{}/{}]",
                                               endpoint, board, thread, position, length),
                                Err(e) => error!("Error running update_deleteds function! {}", e)
                            }
                            break;
                        },
                    StatusCode::NOT_FOUND => {
                        self.query.delete(statements, endpoint, board, thread).await;
                        warn!("({})\t/{}/{}\t[{}/{}]\t<DELETED>",
                              endpoint, board, thread, position, length);
                        break;
                    }
                    _e => {}
                },
                Err(e) => {
                    error!("({})\t/{}/{}\tError fetching thread {}", endpoint, board, thread, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    // Downloads any missing media from a thread
    async fn dl_media_post2(&self, row: &tokio_postgres::row::Row, bs: &BoardSettings,
                            thread: u32, thumb: bool)
                            -> Option<(u64, Option<Vec<u8>>, bool)>
    {
        let no: i64 = row.get("no");
        let tim: i64 = row.get("tim");
        let ext: String = row.get("ext");
        let path = self.get_path();

        let mut hashsum: Option<Vec<u8>> = None;
        let domain = &self.config.settings.media_url;
        let board = &bs.board;

        let url = format!("{}/{}/{}{}{}",
                          domain,
                          board,
                          tim,
                          if thumb { "s" } else { "" },
                          if thumb { ".jpg" } else { &ext });
        for _ in 0..(bs.retry_attempts + 1) {
            match self.client.get(&url, None).await {
                Ok((_, status, body)) => match status {
                    StatusCode::OK => {
                        if body.is_empty() {
                            error!("/{}/{}\t<{}> Body was found to be empty!",
                                   board, thread, status);
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            let mut hasher = Sha256::new();
                            hasher.input(&body);
                            let hash_bytes = hasher.result();
                            let temp_path = format!("{}/tmp/{}_{}{}", path, no, tim, ext);
                            hashsum = Some(hash_bytes.as_slice().to_vec());

                            // Clippy lint
                            // if (bs.keep_media && !thumb)
                            //     || (bs.keep_thumbnails && thumb)
                            //     || ((bs.keep_media && !thumb) && (bs.keep_thumbnails && thumb))
                            if (bs.keep_thumbnails || !thumb) && (thumb || bs.keep_media) {
                                if let Ok(mut dest) = std::fs::File::create(&temp_path) {
                                    if std::io::copy(&mut body.as_slice(), &mut dest).is_ok() {
                                        // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
                                        // /1/0a
                                        let hash_str = &format!("{:x}", hash_bytes); // &hash_text[2..];
                                        let basename =
                                            Path::new(&hash_str).file_stem()
                                                                .expect("err get basename")
                                                                .to_str()
                                                                .expect("err get basename end");
                                        let second =
                                            &basename[&basename.len() - 3..&basename.len() - 1];
                                        let first = &basename[&basename.len() - 1..];
                                        let final_dir_path =
                                            format!("{}/media/{}/{}", path, first, second);
                                        let final_path =
                                            format!("{}/{}{}", final_dir_path, hash_str, ext);

                                        let path_final = Path::new(&final_path);

                                        if path_final.exists() {
                                            warn!("Already exists: {}", final_path);
                                            if let Err(e) = std::fs::remove_file(&temp_path) {
                                                error!("Remove temp: {}", e);
                                            }
                                        } else {
                                            if let Err(e) = std::fs::create_dir_all(&final_dir_path)
                                            {
                                                error!("Create final dir: {}", e);
                                            }
                                            if let Err(e) = std::fs::rename(&temp_path, &final_path)
                                            {
                                                error!("Rename temp to final: {}", e);
                                            }
                                        }
                                    } else {
                                        error!("Error copying file to a temporary path");
                                    }
                                } else {
                                    error!("Error creating a temporary file path");
                                }
                            }
                            break;
                        }
                    }
                    StatusCode::NOT_FOUND => {
                        error!("/{}/{}\t<{}> {}", board, no, status, &url);
                        break;
                    }
                    _e => {
                        error!("/{}/{}\t<{}> {}", board, no, status, &url);
                        sleep(Duration::from_secs(1)).await;
                    }
                },
                Err(e) => {
                    error!("/{}/{}\tError fetching thread {}", board, thread, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Some((u64::try_from(no).unwrap(), hashsum, thumb))
    }
}
