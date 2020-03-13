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
use tokio::sync::Semaphore;
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

    // if config.settings.engine == Database::MySQL {
    //     info!("Connected with:\t{}", conn_url);
    //     let pool = mysql_async::Pool::new(&conn_url);
    //     let yb = YotsubaDatabase::new(pool);
    //     let conn = yb.get_conn().await.unwrap();
    //     let q = r#"UPDATE ? SET deleted = 1, timestamp_expired = unix_timestamp() WHERE num = ?
    // AND subnum = 0"#.to_string();

    //     let stmt = conn.prepare(&q).await.unwrap();
    //     stmt.execute(("3", 518668))
    //         .await
    //         .unwrap()/*
    //         .for_each(|row| {
    //             let z: u8 = row.get(0).unwrap();
    //             let x: u8 = row.get(1).unwrap();
    //             println!("{} {}", z, x);
    //         })
    //         .await
    //         .unwrap()*/;
    //     yb.0.disconnect().await.unwrap();
    //     return Ok(1);
    // }

    let (db_client, connection) = tokio_postgres::connect(&conn_url, tokio_postgres::NoTls)
        .await
        .context(format!("\nPlease check your settings. Connection url used: {}", conn_url))
        .expect("Connecting to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {}", e);
        }
    });
    info!("Connected with:\t{}", conn_url);

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

    sleep(Duration::from_millis(1100)).await;
    archiver.run().await;
    return Ok(0);
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

    async fn run(&self) {
        let mut fut = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(1));
        fut.push(self.compute(YotsubaEndpoint::Media,
                              &self.config.board_settings,
                              Some(&self.config),
                              semaphore.clone()));

        for board in self.config.boards.iter() {
            self.query.init_board(board.board, &self.config.settings.schema).await;
            self.query.init_views(board.board, &self.config.settings.schema).await;

            if board.download_archives {
                fut.push(self.compute(YotsubaEndpoint::Archive, board, None, semaphore.clone()));
            }

            fut.push(self.compute(YotsubaEndpoint::Threads, board, None, semaphore.clone()));
        }

        if semaphore.available_permits() < 1 {
            semaphore.add_permits(1);
        }
        while let Some(_) = fut.next().await {}
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

    async fn create_statements(&self, endpoint: YotsubaEndpoint, board: YotsubaBoard,
                               thumb: YotsubaStatement)
                               -> StatementStore
    {
        let mut statement_store: StatementStore = HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        if endpoint == YotsubaEndpoint::Media {
            statement_store.insert(YotsubaIdentifier { endpoint,
                                                       board,
                                                       statement: YotsubaStatement::Medias },
                                   self.query
                                       .prepare(self.sql.medias(board, thumb).as_str())
                                       .await
                                       .unwrap());
            statement_store.insert(YotsubaIdentifier { endpoint,
                                                       board,
                                                       statement: YotsubaStatement::DeleteMedia },
                                   self.query
                                       .prepare(self.sql.delete_media(board).as_str())
                                       .await
                                       .unwrap());

            statement_store.insert(
                                   YotsubaIdentifier { endpoint,
                                                       board,
                                                       statement:
                                                           YotsubaStatement::UpdateHashMedia },
                                   self.query
                                       .prepare(format!(
                r#"
                     UPDATE "{0}"
                     SET last_modified = extract(epoch from now())::bigint,
                             "sha256" = $2
                     WHERE
                     no = $1 AND "sha256" IS NULL;
                     "#,
                board
            ).as_str())
                                       .await
                                       .unwrap()
            );
            statement_store.insert(
                                   YotsubaIdentifier { endpoint,
                                                       board,
                                                       statement:
                                                           YotsubaStatement::UpdateHashThumbs },
                                   self.query
                                       .prepare(format!(
                r#"
                                UPDATE "{0}"
                                SET last_modified = extract(epoch from now())::bigint,
                                        "sha256t" = $2
                                WHERE
                                no = $1 AND "sha256t" IS NULL;
                                "#,
                board
            ).as_str())
                                       .await
                                       .unwrap()
            );
            return statement_store;
        }
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
                                       YotsubaStatement::UpdateThread =>
                                           self.query
                                               .prepare(self.sql
                                                            .update_thread(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       YotsubaStatement::Delete =>
                                           self.query
                                               .prepare(self.sql
                                                            .delete(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       YotsubaStatement::DeleteMedia => self.query
                                       .prepare(self.sql
                                                    .delete_media(board)
                                                    .as_str())
                                       .await
                                       .unwrap(),
                                       YotsubaStatement::UpdateDeleteds =>
                                           self.query
                                               .prepare(self.sql
                                                            .update_deleteds(self.schema(), board)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       YotsubaStatement::UpdateHashMedia => self.query
                                       .prepare(self.sql
                                                    .update_hash(board, YotsubaHash::Sha256, YotsubaStatement::UpdateHashMedia)
                                                    .as_str())
                                       .await
                                       .unwrap(),
                                       YotsubaStatement::UpdateHashThumbs => self.query
                                       .prepare(self.sql
                                                    .update_hash(board, YotsubaHash::Sha256, YotsubaStatement::UpdateHashThumbs)
                                                    .as_str())
                                       .await
                                       .unwrap(),
                                       YotsubaStatement::Medias =>
                                           self.query
                                               .prepare(self.sql.medias(board, YotsubaStatement::Medias).as_str())
                                               .await
                                               .unwrap(),
                                       YotsubaStatement::Threads =>
                                           self.query.prepare(self.sql.threads()).await.unwrap(),
                                       YotsubaStatement::ThreadsModified =>
                                           self.query
                                               .prepare(self.sql
                                                            .threads_modified(self.schema(),
                                                                              endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       YotsubaStatement::ThreadsCombined =>
                                           self.query
                                               .prepare(self.sql
                                                            .threads_combined(self.schema(),
                                                                              board,
                                                                              endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                       YotsubaStatement::Metadata =>
                                           self.query
                                               .prepare(self.sql
                                                            .metadata(self.schema(), endpoint)
                                                            .as_str())
                                               .await
                                               .unwrap(),
                                   });
        }
        statement_store
    }

    #[allow(unused_mut)]
    async fn compute(&self, endpoint: YotsubaEndpoint, info: &BoardSettings,
                     config: Option<&Config>, semaphore: Arc<Semaphore>)
    {
        match endpoint {
            YotsubaEndpoint::Archive | YotsubaEndpoint::Threads => {
                if self.fetch_board(endpoint, info, semaphore).await.is_some() {};
            }
            YotsubaEndpoint::Media => {
                sleep(Duration::from_secs(2)).await;

                // let rd = info.refresh_delay.into();
                let dur = Duration::from_millis(250);
                let board_settings = config.unwrap().boards.to_owned();
                loop {
                    let now = tokio::time::Instant::now();

                    // Sequential to prevent client congestion and errors
                    for i in board_settings.iter() {
                        if self.finished.load(Ordering::Relaxed) {
                            info!("({})\tStopping media fetching...", endpoint);
                            return;
                        }
                        let what = if i.download_media && i.download_thumbnails {
                            YotsubaStatement::Medias
                        } else if i.download_media {
                            YotsubaStatement::UpdateHashMedia
                        } else if i.download_thumbnails {
                            YotsubaStatement::UpdateHashThumbs
                        } else {
                            // No media downloading
                            continue;
                        };
                        if i.download_thumbnails || i.download_media {
                            let st = self.create_statements(endpoint, i.board, what).await;
                            self.fetch_media(i, &st, endpoint).await;
                        }
                    }

                    // Recheck after every n seconds has passed
                    while now.elapsed().as_secs() < 1 {
                        if self.finished.load(Ordering::Relaxed) {
                            info!("({})\tStopping media fetching...", endpoint);
                            return;
                        }
                        sleep(dur).await;
                    }

                    // If it somehow goes beyond the while loop
                    if self.finished.load(Ordering::Relaxed) {
                        info!("({})\tStopping media fetching...", endpoint);
                        return;
                    }
                }
            }
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
                        error!("({})\t/{}/\t\t<{}> An error has occurred getting the last_modified date",
                               endpoint, current_board, status);
                    } else if *last_modified != last_modified_new {
                        last_modified.clear();
                        last_modified.push_str(&last_modified_new);
                    }
                    match status {
                        StatusCode::NOT_MODIFIED =>
                            info!("({})\t/{}/\t\t<{}>", endpoint, current_board, status),
                        StatusCode::NOT_FOUND => {
                            error!("({})\t/{}/\t\t<{}> No {} found! {}",
                                   endpoint,
                                   current_board,
                                   status,
                                   endpoint,
                                   if retry_attempt == 0 {
                                       "".into()
                                   } else {
                                       format!("Attempt: #{}", retry_attempt)
                                   });
                            sleep(Duration::from_secs(1)).await;
                            if endpoint == YotsubaEndpoint::Archive {
                                *has_archives = false;
                            }
                            continue;
                        }
                        StatusCode::OK => {
                            if body.is_empty() {
                                error!("({})\t/{}/\t\t<{}> Fetched threads was found to be empty!",
                                       endpoint, current_board, status);
                            } else {
                                info!("({})\t/{}/\t\tReceived new threads",
                                      endpoint, current_board);
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
                                            info!("({})\t/{}/\t\tSeems like there was no modified threads at startup..",
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
                                                    info!("({})\t/{}/\t\tSeems like there was no modified threads..",
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
                                            warn!("({})\t/{}/\t\tSeems like there was no modified threads in the beginning?.. {}",
                                                  endpoint, current_board, e),
                                    }
                                }
                            }
                        }
                        _ => error!("({})\t/{}/\t\t<{}> An unforeseen event has occurred!",
                                    endpoint, current_board, status)
                    };
                }
                Err(e) => error!("({})\t/{}/\t\tFetching {}.json: {}",
                                 endpoint, current_board, endpoint, e)
            }
            if endpoint == YotsubaEndpoint::Archive {
                *has_archives = true;
            }
            break;
        }
    }

    /// Manages a single board
    async fn fetch_board(&self, endpoint: YotsubaEndpoint, bs: &BoardSettings,
                         semaphore: Arc<Semaphore>)
                         -> Option<()>
    {
        let current_board = bs.board;
        let mut threads_last_modified = String::new();
        let mut local_threads_list: VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut init = true;
        let mut init_semaphore = true;
        let mut has_archives = true;

        // Default statements
        let statements =
            self.create_statements(endpoint, current_board, YotsubaStatement::Medias).await;

        let dur = Duration::from_millis(250);
        let ratel = Duration::from_millis(bs.throttle_millisec.into());

        // This block is all to respect ratelimit by incrementing by 5 each time.
        let base: Vec<u16> = (bs.refresh_delay..).step_by(5).take(10).collect();
        let last = &base[base.len() - 1..];
        let repeat = last.iter().cycle();
        let original_ratelimit = base.iter().chain(repeat.clone());
        let mut ratelimit = base.iter().chain(repeat);

        loop {
            let now = tokio::time::Instant::now();

            // Semaphore START
            // 1 board acquires the semaphore. Since this function is run concurrently, the other
            // boards also try to acquire the semaphore but only 1 is allowed. This
            // board will release the semaphore after 1 or no thread is fetched, then the other
            // boards acquire the semaphore and do the same,
            if self.config.settings.strict_mode {
                if !self.finished.load(Ordering::Relaxed) {
                    semaphore.acquire().await.forget();
                } else {
                    if semaphore.available_permits() < 1 {
                        semaphore.add_permits(1);
                    }
                    return Some(());
                }
            }

            // Download threads.json / archive.json
            let mut fetched_threads: Option<Vec<u8>> = None;
            let now_endpoint = tokio::time::Instant::now();
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
                info!("({})\t/{}/\t\tTotal new/modified threads: {}",
                      endpoint, current_board, threads_len);
                ratelimit = original_ratelimit.clone();

                // Ratelimit after fetching endpoint
                // This delay is still run concurrently so all boards run this at the same time.
                // When threads are available and strictMode is enabled, this is run sequentially
                // because the above acquires the semaphore, prevent others to run their board, so
                // this delay appears to be sequentially.
                tokio::time::delay_until(now_endpoint + ratel).await;
            } else {
                // Semaphore END
                // If there was no threads, the while loop below won't run so we have to release the
                // semaphore
                if self.config.settings.strict_mode && semaphore.available_permits() < 1 {
                    semaphore.add_permits(1);
                }
            }

            // Download each thread
            let mut position = 1_u32;
            while let Some(thread) = local_threads_list.pop_front() {
                // Semaphore START
                if self.config.settings.strict_mode {
                    // Listen to CTRL-C
                    if self.finished.load(Ordering::Relaxed) {
                        // Reset semaphore when exiting to prevent hangs
                        if semaphore.available_permits() < 1 {
                            semaphore.add_permits(1);
                        }
                        return Some(());
                    } else {
                        // Dont acquire on init because it was already acquired before
                        if init_semaphore {
                            init_semaphore = false;
                        } else {
                            semaphore.acquire().await.forget();
                        }
                    }
                }

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
                // _t.send(thread).unwrap();
                // tokio::spawn(async move {
                // self.fetch_media(&bs, thread, &statements, endpoint).await;
                // });
                // }

                // Semaphore END
                if self.finished.load(Ordering::Relaxed) {
                    if self.config.settings.strict_mode && semaphore.available_permits() < 1 {
                        semaphore.add_permits(1);
                    }
                    return Some(());
                }
                // Ratelimit
                tokio::time::delay_until(now_thread + ratel).await;
                if self.config.settings.strict_mode && semaphore.available_permits() < 1 {
                    semaphore.add_permits(1);
                }
            }

            // Reset the semaphore
            if threads_len > 0 {
                init_semaphore = true;
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
            let newrt = (*ratelimit.next().unwrap()).into();
            while now.elapsed().as_secs() < newrt {
                if self.finished.load(Ordering::Relaxed) {
                    if self.config.settings.strict_mode && semaphore.available_permits() < 1 {
                        semaphore.add_permits(1);
                    }
                    break;
                }
                sleep(dur).await;
            }

            // If the while loop was somehow passed
            if self.finished.load(Ordering::Relaxed) {
                if self.config.settings.strict_mode && semaphore.available_permits() < 1 {
                    semaphore.add_permits(1);
                }
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
                        warn!("({})\t/{}/{}\t[{}/{}] <DELETED>",
                              endpoint, board, thread, position, length);
                        break;
                    }
                    _e => {}
                },
                Err(e) => {
                    error!("({})\t/{}/{}\tFetching thread: {}", endpoint, board, thread, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// FETCH MEDIA

    async fn fetch_media(&self, info: &BoardSettings, statements: &StatementStore,
                         endpoint: YotsubaEndpoint)
    {
        match self.query.medias(statements, endpoint, info.board).await {
            Err(e) =>
                error!("\t\t/{}/An error occurred getting missing media -> {}", info.board, e),
            Ok(media_list) => {
                let mut fut = FuturesUnordered::new();
                let mut has_media = false;
                let dur = Duration::from_millis(200);
                let len = media_list.len();
                if len > 0 {
                    info!("({})\t/{}/\t\tNew media :: {}", endpoint, info.board, len);
                }

                // Chunk to prevent client congestion and errors
                // That way the client doesn't have to run 1000+ requests all at the same time
                for chunks in media_list.as_slice().chunks(20) {
                    for row in chunks {
                        if self.finished.load(Ordering::Relaxed) {
                            return;
                        }
                        has_media = true;

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
                                fut.push(self.dl_media_post2(row, statements, info, false));
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
                                fut.push(self.dl_media_post2(row, statements, info, true));
                            }
                        }
                    }
                    if has_media {
                        while let Some(hh) = fut.next().await {
                            if let Some((no, hashsum, thumb)) = hh {
                                if let Some(hsum) = hashsum {
                                    // Media info
                                    // info!(
                                    //     "({})\t/{}/{}#{} Creating string hashsum{} {}",
                                    //     if thumb { "thumb" } else { "media" },
                                    //     &info.board,
                                    //     thread,
                                    //     no,
                                    //     if thumb { "t" } else { "" },
                                    //     &hsum
                                    // );

                                    self.query
                                        .update_hash(statements,
                                                     endpoint,
                                                     info.board,
                                                     no,
                                                     if thumb {
                                                         YotsubaStatement::UpdateHashThumbs
                                                     } else {
                                                         YotsubaStatement::UpdateHashMedia
                                                     },
                                                     hsum)
                                        .await;
                                    if self.finished.load(Ordering::Relaxed) {
                                        return;
                                    }
                                }
                            // This is usually due to 404. We are already notified of that.
                            // else {
                            //     error!("Error getting hashsum");
                            // }
                            } else {
                                error!("Error running hashsum function");
                            }
                        }
                    }
                    sleep(dur).await;
                    if self.finished.load(Ordering::Relaxed) {
                        return;
                    }
                }
            }
        }
    }

    // Downloads any missing media from a thread
    async fn dl_media_post2(&self, row: &tokio_postgres::row::Row, statements: &StatementStore,
                            info: &BoardSettings, thumb: bool)
                            -> Option<(u64, Option<Vec<u8>>, bool)>
    {
        let no: i64 = row.get("no");
        let tim: i64 = row.get("tim");
        let ext: String = row.get("ext");
        let resto: i64 = row.get("resto");
        let path = self.get_path();
        let thread = (if resto == 0 { no } else { resto }) as u32;

        let mut hashsum: Option<Vec<u8>> = None;
        let domain = &self.config.settings.media_url;
        let board = &info.board;

        let url = format!("{}/{}/{}{}{}",
                          domain,
                          board,
                          tim,
                          if thumb { "s" } else { "" },
                          if thumb { ".jpg" } else { &ext });
        // info!("(some)\t/{}/{}#{}\t Download {}", board, thread, no, &url);
        for ra in 0..(info.retry_attempts + 1) {
            match self.client.get(&url, None).await {
                Err(e) => {
                    error!("(media)\t/{}/{}\tFetching media: {} {}",
                           board,
                           thread,
                           e,
                           if ra > 0 { format!("Attempt #{}", ra) } else { "".into() });
                    sleep(Duration::from_secs(1)).await;
                }
                Ok((_, status, body)) => match status {
                    StatusCode::OK => {
                        if body.is_empty() {
                            error!("(media)\t/{}/{}\t<{}> Body was found to be empty!",
                                   board, thread, status);
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            // info!("(some)\t/{}/{}#{}\t HASHING", board, thread, no);
                            let mut hasher = Sha256::new();
                            hasher.input(&body);
                            let hash_bytes = hasher.result();
                            let temp_path = format!("{}/tmp/{}_{}{}", path, no, tim, ext);
                            hashsum = Some(hash_bytes.as_slice().to_vec());
                            // hashsum = Some(format!("{:x}", hash_bytes));

                            // Clippy lint
                            // if (info.keep_media && !thumb)
                            //     || (info.keep_thumbnails && thumb)
                            //     || ((info.keep_media && !thumb) && (info.keep_thumbnails &&
                            // thumb))
                            if (info.keep_thumbnails || !thumb) && (thumb || info.keep_media) {
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
                        error!("(media)\t/{}/{}\t<{}> {}", board, no, status, &url);
                        self.query
                            .delete_media(statements,
                                          YotsubaEndpoint::Media,
                                          board.to_owned(),
                                          no as u32)
                            .await;
                        break;
                    }
                    _e => {
                        error!("/{}/{}\t<{}> {}", board, no, status, &url);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
        Some((u64::try_from(no).unwrap(), hashsum, thumb))
    }
}
