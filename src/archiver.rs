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

use ::core::sync::atomic::Ordering;
use anyhow::anyhow;
use enum_iterator::IntoEnumIterator;
use futures::stream::{FuturesUnordered, StreamExt as FutureStreamExt};
use log::*;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{self, StatusCode};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Semaphore
    },
    time::{delay_for as sleep, Duration}
};

pub struct YotsubaArchiver<S, R, D: DatabaseTrait<S, R>, H: request::HttpClient> {
    _stmt:    S,
    _row:     R,
    query:    D,
    client:   H,
    config:   Config,
    finished: Arc<AtomicBool>
}

// The implementation of `YotsubaArchiver` that handles everything
impl<S, R, D, H> YotsubaArchiver<S, R, D, H>
where
    R: RowTrait,
    D: DatabaseTrait<S, R>,
    H: request::HttpClient
{
    pub async fn new(_stmt: S, _row: R, db_client: D, http_client: H, config: Config) -> Self {
        Self {
            _stmt,
            _row,
            query: db_client,
            client: http_client,
            config,
            finished: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
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
    ) -> StatementStore<S> {
        let mut statement_store = HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        let gen_id = |stmt: YotsubaStatement| -> YotsubaIdentifier {
            YotsubaIdentifier::new(endpoint, board, stmt)
        };
        if endpoint == YotsubaEndpoint::Media {
            for statement in statements {
                match statement {
                    YotsubaStatement::Medias => {
                        statement_store.insert(
                            gen_id(statement),
                            self.query.prepare(&self.query.query_medias(board, media_mode)).await
                        );
                    }
                    YotsubaStatement::UpdateHashMedia | YotsubaStatement::UpdateHashThumbs => {
                        statement_store.insert(
                            gen_id(statement),
                            self.query
                                .prepare(&self.query.query_update_hash(
                                    board,
                                    YotsubaHash::Sha256,
                                    media_mode
                                ))
                                .await
                        );
                    }
                    _ => {}
                }
            }
            return statement_store;
        }

        for &statement in statements.iter().filter(|&&x| {
            x != YotsubaStatement::Medias
                || x != YotsubaStatement::UpdateHashMedia
                || x != YotsubaStatement::UpdateHashThumbs
        }) {
            statement_store.insert(gen_id(statement), match statement {
                YotsubaStatement::UpdateMetadata =>
                    self.query.prepare(&self.query.query_update_metadata(endpoint)).await,
                YotsubaStatement::UpdateThread =>
                    self.query.prepare(&self.query.query_update_thread(board)).await,
                YotsubaStatement::Delete =>
                    self.query.prepare(&self.query.query_delete(board)).await,
                YotsubaStatement::UpdateDeleteds =>
                    self.query.prepare(&self.query.query_update_deleteds(board)).await,
                YotsubaStatement::UpdateHashMedia | YotsubaStatement::UpdateHashThumbs =>
                    self.query
                        .prepare(&self.query.query_update_hash(
                            board,
                            YotsubaHash::Sha256,
                            media_mode
                        ))
                        .await,
                YotsubaStatement::Medias =>
                    self.query.prepare(&self.query.query_medias(board, media_mode)).await,
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
        tx: Option<UnboundedSender<(BoardSettings, Arc<StatementStore<S>>, u32)>>,
        rx: Option<UnboundedReceiver<(BoardSettings, Arc<StatementStore<S>>, u32)>>
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
                    if let Ok(received) = r.try_recv() {
                        if self.is_finished() && downloading == YotsubaEndpoint::Media {
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
                }
                r.close();
                if self.is_finished() {
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
                                        if let Ok(mut list) = self
                                            .query
                                            .threads_modified(
                                                endpoint,
                                                current_board,
                                                &body,
                                                statements
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
        tx: Option<UnboundedSender<(BoardSettings, Arc<StatementStore<S>>, u32)>>,
        _rx: Option<UnboundedReceiver<(BoardSettings, Arc<StatementStore<S>>, u32)>>
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

        // Media Statements
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
        let statements_media = Arc::new(
            self.create_statements(YotsubaEndpoint::Media, current_board, file_setting).await
        );

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
            if self.is_finished() {
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
                // Semaphore
                let mut _sem_thread = None;
                if self.config.settings.strict_mode {
                    _sem_thread = Some(semaphore.acquire().await);
                }
                if self.is_finished() {
                    if bs.download_media || bs.download_thumbnails {
                        if let Err(_) = t.send((bs.clone(), statements_media.clone(), 0)) {
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
                    if let Err(e) = t.send((bs.clone(), statements_media.clone(), thread)) {
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

            if self.is_finished() {
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
                if self.is_finished() {
                    break;
                }
                sleep(dur).await;
            }

            // If the while loop was somehow passed
            if self.is_finished() {
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
                                error!(
                                    "({})\t/{}/{}\t[{}/{}] |update_thread| {}",
                                    endpoint, board, thread, position, length, e
                                );
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
                                Err(e) => error!(
                                    "({})\t/{}/{}\t[{}/{}] |update_deleteds| {}",
                                    endpoint, board, thread, position, length, e
                                )
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
                let mut fut = FuturesUnordered::new();
                let mut has_media = false;
                let dur = Duration::from_millis(200);
                let len = media_list.len();

                // If somehow we passed the exit code and continued to fetch medias,
                // display info on whatever threads have media to download before exiting the
                // program
                if len > 0
                    && downloading == YotsubaEndpoint::Threads
                    && !(info.board == YotsubaBoard::f && info.download_thumbnails)
                {
                    info!(
                        "({})\t\t/{}/{}\tNew {} :: {}",
                        endpoint,
                        info.board,
                        no,
                        if info.download_media && info.download_thumbnails {
                            "media & thumbs"
                        } else if info.download_media {
                            "media"
                        } else if info.download_thumbnails {
                            "thumbs"
                        } else {
                            "media"
                        },
                        len
                    );
                }

                // Chunk to prevent client congestion and errors
                // Also have the media downloading on a single thread and run things sequentially
                // there That way the client doesn't have to run 1000+ requests all
                // at the same time
                for chunks in media_list.as_slice().chunks(20) {
                    for row in chunks {
                        has_media = true;

                        // Preliminary checks before downloading
                        let sha256: Option<Vec<u8>>;
                        let sha256t: Option<Vec<u8>>;

                        // This is for sha256 checks
                        // To skip or not to skip if present
                        let mut dl_media = false;
                        let mut dl_thumb = false;

                        if self.config.settings.asagi_mode {
                            sha256 = None;
                            sha256t = None;
                        } else {
                            sha256 = row.get::<&str, Option<Vec<u8>>>("sha256");
                            sha256t = row.get::<&str, Option<Vec<u8>>>("sha256t");
                        }
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
                                fut.push(self.dl_media_post2(
                                    row,
                                    info,
                                    YotsubaStatement::UpdateHashMedia
                                ));
                            }
                        }
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
                                fut.push(self.dl_media_post2(
                                    row,
                                    info,
                                    YotsubaStatement::UpdateHashThumbs
                                ));
                            }
                        }
                    }
                    if has_media {
                        while let Some(Some((no, hashsum, mode))) = fut.next().await {
                            // if let Some((no, hashsum, thumb)) = hh {
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

                                // Recieved hash. Proceed to upsert
                                if !self.config.settings.asagi_mode {
                                    self.query
                                        .update_hash(
                                            statements,
                                            endpoint,
                                            info.board,
                                            no,
                                            if mode.is_thumbs() {
                                                YotsubaStatement::UpdateHashThumbs
                                            } else {
                                                YotsubaStatement::UpdateHashMedia
                                            },
                                            hsum
                                        )
                                        .await;
                                }
                            }
                            // This is usually due to 404. We are already notified of that.
                            // else {
                            //     error!("Error getting hashsum");
                            // }
                            // } else {
                            //     error!("Error running hashsum function");
                            // }
                        }
                    }
                    sleep(dur).await;
                }
            }
        }
    }

    // Downloads any missing media from a thread
    // This method is volatile! Any misses in the db will
    // cause a panic! when calling `get()` to get a value.
    async fn dl_media_post2(
        &self, row: &R, info: &BoardSettings, mode: YotsubaStatement
    ) -> Option<(u64, Option<Vec<u8>>, YotsubaStatement)> {
        if info.board == YotsubaBoard::f && mode.is_thumbs() {
            return Some((u64::try_from(0).unwrap(), None, mode));
        }

        let asagi = self.config.settings.asagi_mode;
        let path: &str = &self.config.settings.path;

        let no: i64; // = row.get::<&str, i64>(if asagi { "num" } else { "no" } );

        //log::warn!("ext");
        let tim: i64; // = row.get::<&str, i64>(if asagi { "media_orig" } else { "tim" });
        let ext: String; // = row.get::<&str, String>("ext");
        let resto: i64; // = row.get::<&str, i64>("resto");
        let thread: u32; // = (if resto == 0 { no } else { resto }) as u32;

        if asagi {
            //log::warn!("num");
            no = row.get::<&str, i64>("num");

            //log::warn!("timestamp");
            // `tim` is actually from media_orig/preview_orig but we're not using `tim` here
            tim = row.get::<&str, i64>("timestamp");
            ext = "".into();
            //log::warn!("thread_num");
            resto = row.get::<&str, i64>("thread_num");
            thread = resto as u32; //(if resto == 0 { no } else { resto }) as u32;
        } else {
            no = row.get::<&str, i64>("no");
            tim = row.get::<&str, i64>("tim");
            ext = row.get::<&str, String>("ext");
            resto = row.get::<&str, i64>("resto");

            // For display purposes. Only show the thread no
            thread = (if resto == 0 { no } else { resto }) as u32;
        }

        let mut hashsum: Option<Vec<u8>> = None;
        let domain = &self.config.settings.media_url;
        let board = &info.board;

        // In Asagi there's no rehashing of the file.
        // So if it exists on disk just skip it.
        if asagi {
            let name = if mode.is_thumbs() {
                row.get::<&str, String>("preview_orig")
            } else {
                row.get::<&str, String>("media_orig")
            };
            let subdirs = (&name[..4], &name[4..6]);
            let final_path = format!(
                "{path}/{board}/{sub0}/{sub1}/{filename}",
                path = path,
                board = info.board.to_string(),
                sub0 = subdirs.0,
                sub1 = subdirs.1,
                filename = name
            );
            if Path::new(&final_path).exists() {
                warn!("EXISTS: {}", final_path);
                return Some((u64::try_from(no).unwrap(), None, mode));
            }
        }

        let url = if info.board == YotsubaBoard::f {
            // 4chan has HTML entities UNESCAPED in their filenames (and database) and THAT is then
            // encoded into an ascii url RATHER than an escaped html string and then precent
            // encoded....
            let filename: String;
            if asagi {
                filename =
                    row.get::<&str, Option<String>>("media_filename").unwrap_or("<EMPTY>".into());
                let filename_encoded = utf8_percent_encode(&filename, FRAGMENT).to_string();
                format!("{}/{}/{}", domain, board, filename_encoded)
            } else {
                filename = row.get::<&str, Option<String>>("filename").unwrap_or("<EMPTY>".into());
                let filename_encoded = utf8_percent_encode(&filename, FRAGMENT).to_string();
                format!("{}/{}/{}{}", domain, board, filename_encoded, &ext)
            }
        } else {
            if asagi {
                //log::warn!("preview_orig media_orig");
                format!(
                    "{}/{}/{}",
                    domain,
                    board,
                    if mode.is_thumbs() {
                        row.get::<&str, String>("preview_orig")
                    } else {
                        row.get::<&str, String>("media_orig")
                    }
                )
            } else {
                format!(
                    "{}/{}/{}{}",
                    domain,
                    board,
                    tim,
                    if mode.is_thumbs() { "s.jpg" } else { &ext }
                )
            }
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
                    StatusCode::NOT_FOUND => {
                        error!("(media)\t/{}/{}\t<{}> {}", board, no, status, &url);
                        break;
                    }
                    StatusCode::OK => {
                        if body.is_empty() {
                            error!(
                                "(media)\t/{}/{}\t<{}> Body was found to be empty!",
                                board, thread, status
                            );
                            sleep(Duration::from_secs(1)).await;
                        } else {
                            // Download to temp file. This is guaranteed be unique because the file
                            // name is based on `tim`.
                            let temp_path = if asagi {
                                format!(
                                    "{}/tmp/{}_{}",
                                    path,
                                    no,
                                    if mode.is_thumbs() {
                                        row.get::<&str, String>("preview_orig")
                                    } else {
                                        row.get::<&str, String>("media_orig")
                                    }
                                )
                            } else {
                                format!("{}/tmp/{}_{}{}", path, no, tim, ext)
                            };

                            // HASHING
                            let mut hash_bytes = None;
                            if !asagi {
                                let mut hasher = Sha256::new();
                                hasher.input(&body);
                                hash_bytes = Some(hasher.result());
                                hashsum = Some(hash_bytes.unwrap().as_slice().to_vec()); // Computed hash. Can send to db.
                                // hashsum = Some(format!("{:x}", hash_bytes));
                            }
                            // info!("INSIDE Recieved hash: {:x}", &hash_bytes);
                            // Clippy lint
                            // if (info.keep_media && !thumb)
                            //     || (info.keep_thumbnails && thumb)
                            //     || ((info.keep_media && !thumb) && (info.keep_thumbnails &&
                            // thumb))
                            //
                            // Only go here if keep media settings are enabled
                            // if (info.keep_thumbnails || mode.is_media())
                            //     && (mode.is_thumbs() || info.keep_media)
                            // {
                            if info.keep_thumbnails || info.keep_media {
                                if let Ok(mut dest) = std::fs::File::create(&temp_path) {
                                    if std::io::copy(&mut body.as_slice(), &mut dest).is_ok() {
                                        let final_path_dir;
                                        let final_path;
                                        if asagi {
                                            // Example:
                                            // 1540970147550
                                            // /1540/97
                                            let name = if mode.is_thumbs() {
                                                row.get::<&str, String>("preview_orig")
                                            } else {
                                                row.get::<&str, String>("media_orig")
                                            };
                                            let subdirs = (&name[..4], &name[4..6]);
                                            final_path_dir = format!(
                                                "{}/{}/{}/{}",
                                                path, info.board, subdirs.0, subdirs.1
                                            );
                                            final_path = format!("{}/{}", final_path_dir, name);
                                        } else {
                                            // Example:
                                            // 8e936b088be8d30dd09241a1aca658ff3d54d4098abd1f248e5dfbb003eed0a1
                                            // /1/0a
                                            let name = format!("{:x}", hash_bytes.unwrap());
                                            let len = name.len();
                                            let subdirs =
                                                (&name[len - 1..], &name[len - 3..len - 1]);
                                            final_path_dir = format!(
                                                "{}/media/{}/{}",
                                                path, subdirs.0, subdirs.1
                                            );
                                            final_path =
                                                format!("{}/{}{}", final_path_dir, name, ext);
                                        }

                                        if Path::new(&final_path).exists() {
                                            warn!("EXISTS: {}", final_path);
                                            if let Err(e) = std::fs::remove_file(&temp_path) {
                                                error!("Remove temp: {}", e);
                                            }
                                        } else {
                                            if let Err(e) = std::fs::create_dir_all(&final_path_dir)
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
                    _e => {
                        error!("(media)\t\t/{}/{}\t<{}> {}", board, no, status, &url);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        if self.config.settings.asagi_mode {
            Some((u64::try_from(no).unwrap(), None, mode))
        } else {
            Some((u64::try_from(no).unwrap(), hashsum, mode))
        }
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
