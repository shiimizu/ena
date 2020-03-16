// use crate::ena::*;
use crate::{
    config::{self, BoardSettings, Config},
    enums::*,
    request,
    sql::*
};

use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom,
    path::Path,
    sync::{atomic::AtomicBool, Arc}
};

use tokio::time::{delay_for as sleep, Duration};

use anyhow::anyhow;
use core::sync::atomic::Ordering;
use enum_iterator::IntoEnumIterator;
use futures::stream::{FuturesUnordered, StreamExt as FutureStreamExt};
use log::*;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{self, StatusCode};
use sha2::{Digest, Sha256};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Semaphore
};

pub struct YotsubaArchiver<S, D: DatabaseTrait<S>, H: request::HttpClient> {
    _stmt:    S,
    query:    D,
    client:   H,
    config:   Config,
    finished: Arc<AtomicBool>
}

// The implementation of `YotsubaArchiver` that handles everything
impl<S, D, H> YotsubaArchiver<S, D, H>
where
    D: DatabaseTrait<S>,
    H: request::HttpClient
{
    pub async fn new(_stmt: S, db_client: D, http_client: H, config: Config) -> Self {
        Self {
            _stmt,
            query: db_client,
            client: http_client,
            config,
            finished: Arc::new(AtomicBool::new(false))
        }
    }

    pub async fn run(&self) {
        self.listen_to_exit();
        self.query.init_schema(&self.config.settings.schema).await;
        self.query.init_type().await;
        self.query.init_metadata().await;

        // Runs an archive, threads, and media thread concurrently
        let mut fut = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(1));
        let (tx, rx) = unbounded_channel();

        // Media background thread
        fut.push(self.compute(
            YotsubaEndpoint::Media,
            &self.config.board_settings,
            semaphore.clone(),
            None,
            Some(rx)
        ));

        for board in self.config.boards.iter() {
            self.query.init_board(board.board).await;
            self.query.init_views(board.board).await;

            if board.download_archives {
                fut.push(self.compute(
                    YotsubaEndpoint::Archive,
                    board,
                    semaphore.clone(),
                    Some(tx.clone()),
                    None
                ));
            }

            fut.push(self.compute(
                YotsubaEndpoint::Threads,
                board,
                semaphore.clone(),
                Some(tx.clone()),
                None
            ));
        }

        while let Some(_) = fut.next().await {}
    }

    fn listen_to_exit(&self) {
        let finished_clone = Arc::clone(&self.finished);
        ctrlc::set_handler(move || {
            finished_clone.compare_and_swap(false, true, Ordering::Relaxed);
        })
        .expect("Error setting Ctrl-C handler");
    }

    async fn create_statements(
        &self, endpoint: YotsubaEndpoint, board: YotsubaBoard, media_mode: YotsubaStatement
    ) -> HashMap<YotsubaIdentifier, S> {
        let mut statement_store = HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        let gen_id = |stmt: YotsubaStatement| -> YotsubaIdentifier {
            YotsubaIdentifier::new(endpoint, board, stmt)
        };
        statement_store.insert(
            gen_id(YotsubaStatement::Medias),
            self.query.prepare(&self.query.query_medias(board, media_mode)).await
        );
        if endpoint == YotsubaEndpoint::Media {
            statement_store.insert(
                gen_id(YotsubaStatement::Medias),
                self.query.prepare(&self.query.query_medias(board, media_mode)).await
            );

            statement_store.insert(
                gen_id(YotsubaStatement::UpdateHashMedia),
                self.query
                    .prepare(&self.query.query_update_hash(
                        board,
                        YotsubaHash::Sha256,
                        YotsubaStatement::UpdateHashMedia
                    ))
                    .await
            );
            statement_store.insert(
                gen_id(YotsubaStatement::UpdateHashThumbs),
                self.query
                    .prepare(&self.query.query_update_hash(
                        board,
                        YotsubaHash::Sha256,
                        YotsubaStatement::UpdateHashThumbs
                    ))
                    .await
            );
            return statement_store;
        }

        for statement in statements {
            statement_store.insert(gen_id(statement), match statement {
                YotsubaStatement::UpdateMetadata =>
                    self.query.prepare(&self.query.query_update_metadata(endpoint)).await,
                YotsubaStatement::UpdateThread =>
                    self.query.prepare(&self.query.query_update_thread(board)).await,
                YotsubaStatement::Delete =>
                    self.query.prepare(&self.query.query_delete(board)).await,
                YotsubaStatement::UpdateDeleteds =>
                    self.query.prepare(&self.query.query_update_deleteds(board)).await,
                YotsubaStatement::UpdateHashMedia =>
                    self.query
                        .prepare(&self.query.query_update_hash(
                            board,
                            YotsubaHash::Sha256,
                            YotsubaStatement::UpdateHashMedia
                        ))
                        .await,
                YotsubaStatement::UpdateHashThumbs =>
                    self.query
                        .prepare(&self.query.query_update_hash(
                            board,
                            YotsubaHash::Sha256,
                            YotsubaStatement::UpdateHashThumbs
                        ))
                        .await,
                YotsubaStatement::Medias =>
                    self.query
                        .prepare(&self.query.query_medias(board, YotsubaStatement::Medias))
                        .await,
                YotsubaStatement::Threads => self.query.prepare(&self.query.query_threads()).await,
                YotsubaStatement::ThreadsModified =>
                    self.query.prepare(&self.query.query_threads_modified(endpoint)).await,
                YotsubaStatement::ThreadsCombined =>
                    self.query.prepare(&self.query.query_threads_combined(board, endpoint)).await,
                YotsubaStatement::Metadata =>
                    self.query.prepare(&self.query.query_metadata(endpoint)).await,
            });
        }

        statement_store
    }

    async fn compute(
        &self, endpoint: YotsubaEndpoint, info: &BoardSettings, semaphore: Arc<Semaphore>,
        tx: Option<UnboundedSender<(BoardSettings, StatementStore<S>, u32)>>,
        rx: Option<UnboundedReceiver<(BoardSettings, StatementStore<S>, u32)>>
    )
    {
        match endpoint {
            YotsubaEndpoint::Archive | YotsubaEndpoint::Threads => {
                if self.fetch_board(endpoint, info, semaphore, tx, rx).await.is_some() {};
            }
            YotsubaEndpoint::Media => {
                sleep(Duration::from_secs(2)).await;

                let dur = Duration::from_millis(250);
                let mut r = rx.unwrap();
                let mut downloading = endpoint;

                // Use a custom poll rate instead of recv().await which polls at around 1s.
                loop {
                    // Sequential fetching to prevent client congestion and errors
                    while let Ok(received) = r.try_recv() {
                        if self.finished.load(Ordering::Relaxed)
                            && downloading == YotsubaEndpoint::Media
                        {
                            info!("({})\tStopping media fetching...", endpoint);
                            downloading = YotsubaEndpoint::Threads;
                        }
                        // Only leave here
                        // The signal to stop is a thread no of: 0
                        if received.2 == 0 {
                            break;
                        }
                        let media_info = received.0;
                        if media_info.download_thumbnails || media_info.download_media {
                            self.fetch_media(
                                &media_info,
                                &received.1,
                                endpoint,
                                received.2,
                                downloading
                            )
                            .await;
                        }
                    }

                    sleep(dur).await;
                    if self.finished.load(Ordering::Relaxed) {
                        break;
                    }
                }
                r.close();
                if self.finished.load(Ordering::Relaxed) {
                    return;
                }
            }
        }
    }

    /// Downloads the endpoint threads
    async fn get_generic_thread(
        &self, endpoint: YotsubaEndpoint, bs: &BoardSettings, last_modified: &mut String,
        fetched_threads: &mut Option<Vec<u8>>, local_threads_list: &mut VecDeque<u32>,
        init: &mut bool, update_metadata: &mut bool, has_archives: &mut bool,
        statements: &StatementStore<S>
    )
    {
        if endpoint == YotsubaEndpoint::Archive && !*has_archives {
            return;
        }

        let current_board = bs.board;
        for retry_attempt in 0..(bs.retry_attempts + 1) {
            match self
                .client
                .get(
                    &format!(
                        "{domain}/{board}/{endpoint}.json",
                        domain = &self.config.settings.api_url,
                        board = current_board,
                        endpoint = endpoint
                    ),
                    Some(last_modified)
                )
                .await
            {
                Ok((last_modified_recieved, status, body)) => {
                    if last_modified_recieved.is_empty() {
                        error!(
                            "({})\t/{}/\t\t<{}> An error has occurred getting the last_modified date",
                            endpoint, current_board, status
                        );
                    } else if *last_modified != last_modified_recieved {
                        last_modified.clear();
                        last_modified.push_str(&last_modified_recieved);
                    }
                    match status {
                        StatusCode::NOT_MODIFIED =>
                            info!("({})\t/{}/\t\t<{}>", endpoint, current_board, status),
                        StatusCode::NOT_FOUND => {
                            error!(
                                "({})\t/{}/\t\t<{}> No {} found! {}",
                                endpoint,
                                current_board,
                                status,
                                endpoint,
                                if retry_attempt == 0 {
                                    "".into()
                                } else {
                                    format!("Attempt: #{}", retry_attempt)
                                }
                            );
                            sleep(Duration::from_secs(1)).await;
                            if endpoint == YotsubaEndpoint::Archive {
                                *has_archives = false;
                            }
                            continue;
                        }
                        StatusCode::OK => {
                            if body.is_empty() {
                                error!(
                                    "({})\t/{}/\t\t<{}> Fetched threads was found to be empty!",
                                    endpoint, current_board, status
                                );
                            } else {
                                info!(
                                    "({})\t/{}/\t\tReceived new threads",
                                    endpoint, current_board
                                );
                                *fetched_threads = Some(body.to_owned());

                                // Check if there's an entry in the metadata
                                if self.query.metadata(&statements, endpoint, current_board).await {
                                    let ena_resume = config::ena_resume();

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
                                        if let Ok(mut list) = self
                                            .query
                                            .threads_combined(
                                                &statements,
                                                endpoint,
                                                current_board,
                                                &body
                                            )
                                            .await
                                        {
                                            local_threads_list.append(&mut list);
                                        } else {
                                            info!(
                                                "({})\t/{}/\t\tSeems like there was no modified threads at startup..",
                                                endpoint, current_board
                                            );
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
                                            endpoint,
                                            board: current_board,
                                            statement: YotsubaStatement::ThreadsModified
                                        };
                                        match &statements.get(&id) {
                                            Some(statement_recieved) => {
                                                if let Ok(mut list) = self
                                                    .query
                                                    .threads_modified(
                                                        current_board,
                                                        &body,
                                                        statement_recieved
                                                    )
                                                    .await
                                                {
                                                    local_threads_list.append(&mut list);
                                                } else {
                                                    info!(
                                                        "({})\t/{}/\t\tSeems like there was no modified threads..",
                                                        endpoint, current_board
                                                    )
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
                                    if let Err(e) = self
                                        .query
                                        .update_metadata(
                                            &statements,
                                            endpoint,
                                            current_board,
                                            &body
                                        )
                                        .await
                                    {
                                        error!("Error running update_metadata function! {}", e)
                                    }
                                    *update_metadata = false;
                                    *init = false;

                                    match if endpoint == YotsubaEndpoint::Threads {
                                        self.query
                                            .threads(&statements, endpoint, current_board, &body)
                                            .await
                                    } else {
                                        // Converting to anyhow
                                        match serde_json::from_slice::<VecDeque<u32>>(&body) {
                                            Ok(t) => Ok(t),
                                            Err(e) => Err(anyhow!(
                                                "Error converting body to VecDeque for query.threads() {}",
                                                e
                                            ))
                                        }
                                    } {
                                        Ok(mut list) => local_threads_list.append(&mut list),
                                        Err(e) => warn!(
                                            "({})\t/{}/\t\tSeems like there was no modified threads in the beginning?.. {}",
                                            endpoint, current_board, e
                                        )
                                    }
                                }
                            }
                        }
                        _ => error!(
                            "({})\t/{}/\t\t<{}> An unforeseen event has occurred!",
                            endpoint, current_board, status
                        )
                    };
                }
                Err(e) => error!(
                    "({})\t/{}/\t\tFetching {}.json: {}",
                    endpoint, current_board, endpoint, e
                )
            }
            if endpoint == YotsubaEndpoint::Archive {
                *has_archives = true;
            }
            break;
        }
    }

    /// Manages a single board
    async fn fetch_board(
        &self, endpoint: YotsubaEndpoint, bs: &BoardSettings, semaphore: Arc<Semaphore>,
        tx: Option<UnboundedSender<(BoardSettings, StatementStore<S>, u32)>>,
        _rx: Option<UnboundedReceiver<(BoardSettings, StatementStore<S>, u32)>>
    ) -> Option<()>
    {
        let current_board = bs.board;
        let mut threads_last_modified = String::new();
        let mut local_threads_list: VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut init = true;
        let mut has_archives = true;

        // Default statements
        let statements =
            self.create_statements(endpoint, current_board, YotsubaStatement::Medias).await;

        let file_setting = if bs.download_media && bs.download_thumbnails {
            YotsubaStatement::Medias
        } else if bs.download_media {
            YotsubaStatement::UpdateHashMedia
        } else if bs.download_thumbnails {
            YotsubaStatement::UpdateHashThumbs
        } else {
            // No media downloading at all
            // Set this to any. Before downloading media, the board settings is checked
            // So this is fine
            YotsubaStatement::Threads
        };

        let dur = Duration::from_millis(250);
        let ratel = Duration::from_millis(bs.throttle_millisec.into());

        // This mimics the thread refresh rate
        let original_ratelimit = config::refresh_rate(bs.refresh_delay, 5, 10);
        let mut ratelimit = original_ratelimit.clone();
        loop {
            let now = tokio::time::Instant::now();

            // Semaphore. When the result of `acquire` is dropped, the semaphore is released.
            // 1 board acquires the semaphore. Since this function is run concurrently, the other
            // boards also try to acquire the semaphore but only 1 is allowed. This
            // board will release the semaphore after 1 or no thread is fetched, then the other
            // boards acquire the semaphore and do the same,
            let mut _sem = None;
            if self.config.settings.strict_mode {
                _sem = Some(semaphore.acquire().await);
            }
            if self.finished.load(Ordering::Relaxed) {
                break;
            }

            // Download threads.json / archive.json
            let mut fetched_threads: Option<Vec<u8>> = None;
            let now_endpoint = tokio::time::Instant::now();
            self.get_generic_thread(
                endpoint,
                &bs,
                &mut threads_last_modified,
                &mut fetched_threads,
                &mut local_threads_list,
                &mut init,
                &mut update_metadata,
                &mut has_archives,
                &statements
            )
            .await;

            // Display length of new fetched threads
            let threads_len = local_threads_list.len();
            if threads_len > 0 {
                info!(
                    "({})\t/{}/\t\tTotal new/modified threads: {}",
                    endpoint, current_board, threads_len
                );
                ratelimit = original_ratelimit.clone();

                // Ratelimit after fetching endpoint
                // This delay is still run concurrently so all boards run this at the same time.
                // When threads are available and strictMode is enabled, this is run sequentially
                // because the above acquires the semaphore, prevent others to run their board, so
                // this delay appears to be sequentially.
                tokio::time::delay_until(now_endpoint + ratel).await;
            }

            drop(_sem);

            // Download each thread
            let mut position = 1_u32;
            let t = tx.clone().unwrap();
            while let Some(thread) = local_threads_list.pop_front() {
                // No `clone()` method for  the HashMap with the trait, so I have to put this here
                // in the loop
                let statements_media = self
                    .create_statements(YotsubaEndpoint::Media, current_board, file_setting)
                    .await;

                // Semaphore
                let mut _sem_thread = None;
                if self.config.settings.strict_mode {
                    _sem_thread = Some(semaphore.acquire().await);
                }
                if self.finished.load(Ordering::Relaxed) {
                    if bs.download_media || bs.download_thumbnails {
                        if let Err(_) = t.send((bs.clone(), statements_media, 0)) {
                            // Don't display an error if we're sending the exit code
                            // error!("(media)\t/{}/{}\t[{}/{}] {}", &bs.board, 0, 0, 0, e);
                        }
                    }
                    break;
                }

                let now_thread = tokio::time::Instant::now();
                self.assign_to_thread(&bs, endpoint, thread, position, threads_len, &statements)
                    .await;
                if bs.download_media || bs.download_thumbnails {
                    if let Err(e) = t.send((bs.clone(), statements_media, thread)) {
                        error!(
                            "(media)\t/{}/{}\t[{}/{}] {}",
                            &bs.board, thread, position, threads_len, e
                        );
                    }
                }
                position += 1;

                // Ratelimit
                tokio::time::delay_until(now_thread + ratel).await;
            }

            if self.finished.load(Ordering::Relaxed) {
                break;
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
            let newrt = (ratelimit.next().unwrap()).into();
            while now.elapsed().as_secs() < newrt {
                if self.finished.load(Ordering::Relaxed) {
                    break;
                }
                sleep(dur).await;
            }

            // If the while loop was somehow passed
            if self.finished.load(Ordering::Relaxed) {
                break;
            }
        }
        // channel.close();
        Some(())
    }

    // Download a single thread and its media
    async fn assign_to_thread(
        &self, board_settings: &BoardSettings, endpoint: YotsubaEndpoint, thread: u32,
        position: u32, length: usize, statements: &StatementStore<S>
    )
    {
        let board = board_settings.board;
        for _ in 0..(board_settings.retry_attempts + 1) {
            match self
                .client
                .get(
                    &format!(
                        "{domain}/{board}/thread/{no}.json",
                        domain = &self.config.settings.api_url,
                        board = board,
                        no = thread
                    ),
                    None
                )
                .await
            {
                Ok((_, status, body)) => match status {
                    StatusCode::OK =>
                        if body.is_empty() {
                            error!(
                                "({})\t/{}/{}\t<{}> Body was found to be empty!",
                                endpoint, board, thread, status
                            );
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            if let Err(e) =
                                self.query.update_thread(&statements, endpoint, board, &body).await
                            {
                                error!("Error executing update_thread function! {}", e);
                            }
                            match self
                                .query
                                .update_deleteds(&statements, endpoint, board, thread, &body)
                                .await
                            {
                                Ok(_) => info!(
                                    "({})\t/{}/{}\t[{}/{}]",
                                    endpoint, board, thread, position, length
                                ),
                                Err(e) => error!("Error running update_deleteds function! {}", e)
                            }
                            break;
                        },
                    StatusCode::NOT_FOUND => {
                        self.query.delete(&statements, endpoint, board, thread).await;
                        warn!(
                            "({})\t/{}/{}\t[{}/{}] <DELETED>",
                            endpoint, board, thread, position, length
                        );
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
    async fn fetch_media(
        &self, info: &BoardSettings, statements: &StatementStore<S>, endpoint: YotsubaEndpoint,
        no: u32, downloading: YotsubaEndpoint
    )
    {
        // All media for a particular thread should finish downloading to prevent missing media in
        // the database CTRL-C does not apply here

        match self.query.medias(statements, endpoint, info.board, no).await {
            Err(e) =>
                error!("\t\t/{}/An error occurred getting missing media -> {}", info.board, e),
            Ok(media_list) => {
                let mut pg = None;
                let mut _ms = None;
                match media_list {
                    Rows::PostgreSQL(p) => {
                        pg = Some(p);
                    }
                    Rows::MySQL(m) => {
                        _ms = Some(m);
                    }
                }
                let ml = pg.unwrap();
                let mut fut = FuturesUnordered::new();
                let mut has_media = false;
                let dur = Duration::from_millis(200);
                let len = ml.len();

                // Display info on whatever threads have media to download before exiting the
                // program
                if len > 0 && downloading == YotsubaEndpoint::Threads {
                    info!("({})\t/{}/{}\tNew media :: {}", endpoint, info.board, no, len);
                }

                // Chunk to prevent client congestion and errors
                // That way the client doesn't have to run 1000+ requests all at the same time
                for chunks in ml.as_slice().chunks(20) {
                    for row in chunks {
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
                                fut.push(self.dl_media_post2(row, info, false));
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
                                fut.push(self.dl_media_post2(row, info, true));
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
                                        .update_hash(
                                            statements,
                                            endpoint,
                                            info.board,
                                            no,
                                            if thumb {
                                                YotsubaStatement::UpdateHashThumbs
                                            } else {
                                                YotsubaStatement::UpdateHashMedia
                                            },
                                            hsum
                                        )
                                        .await;
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
                }
            }
        }
    }

    // Downloads any missing media from a thread
    async fn dl_media_post2(
        &self, row: &tokio_postgres::row::Row, info: &BoardSettings, thumb: bool
    ) -> Option<(u64, Option<Vec<u8>>, bool)> {
        let no: i64 = row.get("no");
        if info.board == YotsubaBoard::f && thumb {
            return Some((u64::try_from(no).unwrap(), None, thumb));
        }
        let tim: i64 = row.get("tim");
        let ext: String = row.get("ext");
        let resto: i64 = row.get("resto");
        let path: &str = &self.config.settings.path;
        let thread: u32 = (if resto == 0 { no } else { resto }) as u32;

        let mut hashsum: Option<Vec<u8>> = None;
        let domain = &self.config.settings.media_url;
        let board = &info.board;

        let url = if info.board == YotsubaBoard::f {
            // 4chan has HTML entities UNESCAPED in their filenames (and database) and THAT is then
            // encoded into an ascii url RATHER than an escaped html string then precent
            // encoded....
            let filename: String = row.try_get("filename").unwrap_or("<EMPTY>".into());
            let filename_encoded = utf8_percent_encode(&filename, FRAGMENT).to_string();
            format!("{}/{}/{}{}", domain, board, filename_encoded, &ext)
        } else {
            format!(
                "{}/{}/{}{}{}",
                domain,
                board,
                tim,
                if thumb { "s" } else { "" },
                if thumb { ".jpg" } else { &ext }
            )
        };
        // info!("(some)\t/{}/{}#{}\t Download {}", board, thread, no, &url);
        for ra in 0..(info.retry_attempts + 1) {
            match self.client.get(&url, None).await {
                Err(e) => {
                    error!(
                        "(media)\t/{}/{}\tFetching media: {} {}",
                        board,
                        thread,
                        e,
                        if ra > 0 { format!("Attempt #{}", ra) } else { "".into() }
                    );
                    sleep(Duration::from_secs(1)).await;
                }
                Ok((_, status, body)) => match status {
                    StatusCode::OK => {
                        if body.is_empty() {
                            error!(
                                "(media)\t/{}/{}\t<{}> Body was found to be empty!",
                                board, thread, status
                            );
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
                                        let basename = Path::new(&hash_str)
                                            .file_stem()
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

const FRAGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'{')
    .add(b'}')
    .add(b',')
    .add(b'&')
    .add(b'#')
    .add(b';');
