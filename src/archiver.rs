use crate::{
    config::{self, BoardSettings, Config},
    enums::*,
    request,
    sql::*
};

use ::core::sync::atomic::Ordering;
use anyhow::{anyhow, Result};
use enum_iterator::IntoEnumIterator;
use futures::stream::{FuturesUnordered, StreamExt as FutureStreamExt};
use log::*;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{self, StatusCode};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom,
    marker::PhantomData,
    path::Path,
    sync::{atomic::AtomicBool, Arc}
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Semaphore
    },
    time::{delay_for as sleep, Duration}
};

pub struct YotsubaArchiver<S, R, D: DatabaseTrait<S, R>, H: request::HttpClient> {
    // PhantomData
    // https://is.gd/CYXIJO
    // https://doc.rust-lang.org/std/marker/struct.PhantomData.html
    _stmt:    PhantomData<S>,
    _row:     PhantomData<R>,
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
    pub async fn new(db_client: D, http_client: H, config: Config) -> Self {
        Self {
            _stmt: PhantomData,
            _row: PhantomData,
            query: db_client,
            client: http_client,
            config,
            finished: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    pub async fn run(&self) -> Result<()> {
        self.listen_to_exit()?;
        self.query
            .init_schema(
                &self.config.settings.schema,
                self.config.settings.engine,
                &self.config.settings.charset
            )
            .await?;
        self.query.init_type().await?;
        self.query
            .init_metadata(self.config.settings.engine, &self.config.settings.charset)
            .await?;

        // Run archive, threads, and media concurrently
        let mut fut = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(1));
        let (tx, rx) = unbounded_channel();

        // Media background thread. On its own single thread.
        // Has to know which board to run on, that's where channels come in.
        fut.push(self.compute(
            YotsubaEndpoint::Media,
            &self.config.board_settings,
            semaphore.clone(),
            None,
            Some(rx)
        ));

        for board in self.config.boards.iter() {
            self.query
                .init_board(board.board, self.config.settings.engine, &self.config.settings.charset)
                .await?;
            self.query.init_views(board.board).await?;

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

        while let Some(Err(e)) = fut.next().await {
            error!("{}", e);
        }
        Ok(())
    }

    fn listen_to_exit(&self) -> Result<()> {
        let finished_clone = Arc::clone(&self.finished);
        Ok(ctrlc::set_handler(move || {
            finished_clone.compare_and_swap(false, true, Ordering::Relaxed);
        })?)
    }

    async fn create_statements(
        &self, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> StatementStore<S> {
        let mut statement_store = HashMap::new();
        let statements: Vec<_> = YotsubaStatement::into_enum_iter().collect();
        let gen_id = |stmt: YotsubaStatement| -> YotsubaIdentifier {
            YotsubaIdentifier::new(endpoint, board, stmt)
        };

        // Can't use statments for Asagi and MySQL due to the way [`mysql_async`] is implemented.
        // Connections don't hold a shared reference and statements can only be run once because it
        // ALSO moves itself out... wtf.. Therefore new connections need to be taken from
        // the pool, and statments are always constanly being remade for each query..
        // Here just give a placeholder statement.
        if self.config.settings.asagi_mode {
            statement_store.insert(
                gen_id(YotsubaStatement::Medias),
                self.query.prepare(&self.query.query_medias(board, YotsubaStatement::Medias)).await
            );
            return statement_store;
        }

        if endpoint == YotsubaEndpoint::Media {
            for statement in statements {
                match statement {
                    YotsubaStatement::Medias => {
                        statement_store.insert(
                            gen_id(statement),
                            self.query
                                .prepare(&self.query.query_medias(board, YotsubaStatement::Medias))
                                .await
                        );
                    }
                    YotsubaStatement::UpdateHashMedia | YotsubaStatement::UpdateHashThumbs => {
                        statement_store.insert(
                            gen_id(statement),
                            self.query
                                .prepare(&self.query.query_update_hash(
                                    board,
                                    YotsubaHash::Sha256,
                                    statement.is_thumbs_val()
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
                            statement.is_thumbs_val()
                        ))
                        .await,
                YotsubaStatement::Medias =>
                    self.query.prepare(&self.query.query_medias(board, statement)).await,
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
    ) -> Result<()>
    {
        match endpoint {
            YotsubaEndpoint::Archive | YotsubaEndpoint::Threads => {
                if let Err(e) = self.fetch_board(endpoint, info, semaphore, tx, rx).await {
                    error!("|fetch_board| An error has occurred {}", e);
                }
            }
            YotsubaEndpoint::Media => {
                sleep(Duration::from_secs(2)).await;

                let dur = Duration::from_millis(250);
                let mut r =
                    rx.ok_or_else(|| anyhow!("|compute::Media| UnboundedReceiver was empty"))?;
                let mut downloading = endpoint;
                let mut exit_code: u8 = 1;

                // Custom poll rate instead of recv().await which polls at around 1s
                // Sequential fetching to prevent client congestion and errors
                loop {
                    sleep(dur).await;
                    if let Ok((media_info, statments, thread)) = r.try_recv() {
                        if self.is_finished() && downloading == YotsubaEndpoint::Media {
                            if media_info.download_media || media_info.download_thumbnails {
                                info!("({})\tStopping media fetching...", endpoint);
                            }
                            downloading = YotsubaEndpoint::Threads;
                        }
                        // info!("({})\t\t/{}/{} DL", endpoint, media_info.board, received.2);

                        // Only leave here
                        // The signal to stop is a thread no of: 0
                        if thread == 0 {
                            exit_code = thread as u8;
                        }

                        // We need the current board info
                        // No Media at all for the current board
                        if !(media_info.download_media || media_info.download_thumbnails) {
                            // Continue because this media thread is a single thread.
                            // Don't return unless everyone is done.
                            continue;
                        }

                        // Exit code is so that the loop continues
                        // Exhausting the channel until no threads can be recieved
                        // Only one that go through this block are valid threads
                        if thread != 0
                            && (media_info.download_thumbnails || media_info.download_media)
                        {
                            if let Err(e) = self
                                .fetch_media(&media_info, &statments, endpoint, thread, downloading)
                                .await
                            {
                                error!("({})\t\t/{}/{} {}", endpoint, media_info.board, thread, e)
                            }
                        }
                    } else {
                        // This is our exit out of the loop.
                        if self.is_finished()
                            && downloading == YotsubaEndpoint::Threads
                            && exit_code == 0
                        {
                            break;
                        }
                    }
                }
                r.close();
            }
        }
        Ok(())
    }

    /// Downloads the endpoint threads
    async fn get_generic_thread(
        &self, endpoint: YotsubaEndpoint, bs: &BoardSettings, last_modified: &mut String,
        fetched_threads: &mut Option<Vec<u8>>, local_threads_list: &mut VecDeque<u32>,
        init: &mut bool, update_metadata: &mut bool, has_archives: &mut bool,
        statements: &StatementStore<S>
    )
    {
        if self.is_finished() || (endpoint == YotsubaEndpoint::Archive && !*has_archives) {
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
                Err(e) => error!(
                    "({})\t/{}/\t\tFetching {}.json: {}",
                    endpoint, current_board, endpoint, e
                ),
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
                                if self
                                    .query
                                    .metadata(&statements, endpoint, current_board)
                                    .await
                                    .unwrap_or(false)
                                {
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
                                        && (!ena_resume || endpoint == YotsubaEndpoint::Archive)
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
                                    // No cache found, use fetched_threads to update it
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
    ) -> Result<()>
    {
        let current_board = bs.board;
        let mut threads_last_modified = String::new();
        let mut local_threads_list: VecDeque<u32> = VecDeque::new();
        let mut update_metadata = false;
        let mut init = true;
        let mut has_archives = true;

        // Default statements
        let statements = self.create_statements(endpoint, current_board).await;

        // Media Statements
        let statements_media =
            Arc::new(self.create_statements(YotsubaEndpoint::Media, current_board).await);

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
            let t = tx.clone().ok_or_else(|| anyhow!("|fetch_board| UnboundedSender is empty"))?;
            while let Some(thread) = local_threads_list.pop_front() {
                // Semaphore
                let mut _sem_thread = None;
                if self.config.settings.strict_mode {
                    _sem_thread = Some(semaphore.acquire().await);
                }
                if self.is_finished() {
                    // if bs.download_media || bs.download_thumbnails {
                    if let Err(_) = t.send((bs.clone(), statements_media.clone(), 0)) {
                        // Don't display an error if we're sending the exit code
                        // error!("(media)\t/{}/{}\t[{}/{}] {}", &bs.board, 0, 0, 0, e);
                    }
                    sleep(Duration::from_millis(1500)).await;
                    // }
                    break;
                }

                let now_thread = tokio::time::Instant::now();
                self.assign_to_thread(&bs, endpoint, thread, position, threads_len, &statements)
                    .await;
                // if bs.download_media || bs.download_thumbnails {
                if let Err(e) = t.send((bs.clone(), statements_media.clone(), thread)) {
                    error!(
                        "(media)\t\t/{}/{}\t[{}/{}] {}",
                        &bs.board, thread, position, threads_len, e
                    );
                }
                // }
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
            let newrt = ratelimit.next().unwrap_or(bs.refresh_delay).into();
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
        Ok(())
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
                        if let Err(e) =
                            self.query.delete(&statements, endpoint, board, thread).await
                        {
                            error!(
                                "({})\t/{}/{}\t[{}/{}] |delete| {}",
                                endpoint, board, thread, position, length, e
                            );
                        }
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
    ) -> Result<bool>
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
                        let sha256 = row.get::<&str, Option<Vec<u8>>>("sha256")?;
                        let sha256t = row.get::<&str, Option<Vec<u8>>>("sha256t")?;
                        [
                            (sha256, YotsubaStatement::UpdateHashMedia),
                            (sha256t, YotsubaStatement::UpdateHashThumbs)
                        ]
                        .iter()
                        .for_each(|(current_hash, mode)| {
                            let hash_exists =
                                matches!(current_hash, Some(hash) if hash.len() >= (65 / 2));
                            if !hash_exists
                                && ((info.download_media
                                    && matches!(mode, YotsubaStatement::UpdateHashMedia))
                                    || (info.download_thumbnails
                                        && matches!(mode, YotsubaStatement::UpdateHashThumbs)))
                            {
                                has_media = true;
                                fut.push(self.dl_media_post2(row, info, *mode));
                            }
                        });
                    }

                    if !has_media {
                        continue;
                    }

                    while let Some(Ok((no, hashsum, mode))) = fut.next().await {
                        if let Some(hsum) = hashsum {
                            if !self.config.settings.asagi_mode {
                                if let Err(e) = self
                                    .query
                                    .update_hash(
                                        statements,
                                        endpoint,
                                        info.board,
                                        no,
                                        mode.is_thumbs_val(),
                                        hsum
                                    )
                                    .await
                                {
                                    error!(
                                        "({})\t\t/{}/{}\t|update_hash| {}",
                                        endpoint, info.board, no, e
                                    )
                                }
                            }
                        }
                    }
                    sleep(dur).await;
                }
            }
        }
        Ok(true)
    }

    // Downloads any missing media from a thread
    // This method is volatile! Any misses in the db will
    // cause a panic! when calling `get()` to get a value.
    async fn dl_media_post2(
        &self, row: &R, info: &BoardSettings, mode: YotsubaStatement
    ) -> Result<(u64, Option<Vec<u8>>, YotsubaStatement)> {
        if info.board == YotsubaBoard::f && mode.is_thumbs() {
            return Ok((0, None, mode));
        }

        let asagi = self.config.settings.asagi_mode;
        let path: &str = &self.config.settings.path;
        let no: i64;
        let tim: i64;
        let ext: String;
        let resto: i64;
        let thread: u32;

        if asagi {
            //log::warn!("num");
            no = row.get::<&str, i64>("num")?;

            //log::warn!("timestamp");
            // `tim` is actually from media_orig/preview_orig but we're not using `tim` here
            tim = row.get::<&str, i64>("timestamp")?;
            ext = "".into();
            //log::warn!("thread_num");
            resto = row.get::<&str, i64>("thread_num")?;
            thread = resto as u32;
        } else {
            no = row.get::<&str, i64>("no")?;
            tim = row.get::<&str, i64>("tim")?;
            ext = row.get::<&str, String>("ext")?;
            resto = row.get::<&str, i64>("resto")?;

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
                row.get::<&str, String>("preview_orig")?
            } else {
                row.get::<&str, String>("media_orig")?
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
                return Ok((u64::try_from(no)?, None, mode));
            }
        }

        let url = if info.board == YotsubaBoard::f {
            // 4chan has HTML entities UNESCAPED in their filenames (and database) and THAT is then
            // encoded into an ascii url RATHER than an escaped html string and then precent
            // encoded....
            let filename: String;
            if asagi {
                filename =
                    row.get::<&str, Option<String>>("media_filename")?.unwrap_or("<EMPTY>".into());
                let filename_encoded = utf8_percent_encode(&filename, FRAGMENT).to_string();
                format!("{}/{}/{}", domain, board, filename_encoded)
            } else {
                filename = row.get::<&str, Option<String>>("filename")?.unwrap_or("<EMPTY>".into());
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
                        row.get::<&str, String>("preview_orig")?
                    } else {
                        row.get::<&str, String>("media_orig")?
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

        debug!("(media)\t\t/{}/{}#{}\t {}", board, thread, no, &url);
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
                                        row.get::<&str, String>("preview_orig")?
                                    } else {
                                        row.get::<&str, String>("media_orig")?
                                    }
                                )
                            } else {
                                format!("{}/tmp/{}_{}{}", path, no, tim, ext)
                            };

                            // Hashing
                            let mut hash_bytes = None;
                            if !asagi {
                                let mut hasher = Sha256::new();
                                hasher.input(&body);
                                hash_bytes = Some(hasher.result());
                                hashsum = Some(hash_bytes.unwrap().as_slice().to_vec());
                                // hashsum = Some(format!("{:x}", hash_bytes));
                            }

                            if !(info.keep_thumbnails || info.keep_media) {
                                break;
                            }

                            if let Ok(mut dest) = std::fs::File::create(&temp_path) {
                                if std::io::copy(&mut body.as_slice(), &mut dest).is_ok() {
                                    let final_path_dir;
                                    let final_path;
                                    if asagi {
                                        // Example:
                                        // 1540970147550
                                        // /1540/97
                                        let name = if mode.is_thumbs() {
                                            row.get::<&str, String>("preview_orig")?
                                        } else {
                                            row.get::<&str, String>("media_orig")?
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
                                        let subdirs = (&name[len - 1..], &name[len - 3..len - 1]);
                                        final_path_dir =
                                            format!("{}/media/{}/{}", path, subdirs.0, subdirs.1);
                                        final_path = format!("{}/{}{}", final_path_dir, name, ext);
                                    }

                                    if Path::new(&final_path).exists() {
                                        warn!("EXISTS: {}", final_path);
                                        if let Err(e) = std::fs::remove_file(&temp_path) {
                                            error!("Remove temp: {}", e);
                                        }
                                    } else {
                                        if let Err(e) = std::fs::create_dir_all(&final_path_dir) {
                                            error!("Create final dir: {}", e);
                                        }
                                        if let Err(e) = std::fs::rename(&temp_path, &final_path) {
                                            error!("Rename temp to final: {}", e);
                                        }
                                    }
                                } else {
                                    error!("Error copying file to a temporary path");
                                }
                            } else {
                                error!("Error creating a temporary file path");
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
            Ok((u64::try_from(no)?, None, mode))
        } else {
            Ok((u64::try_from(no)?, hashsum, mode))
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
