#![forbid(unsafe_code)]
#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(irrefutable_let_patterns)]
#![allow(clippy::range_plus_one)]

use ena::*;

use crate::sql::*;
use serde::{self, Deserialize, Serialize};
use tokio::{
    runtime::Builder,
    time::{delay_for as sleep, Duration}
};

use anyhow::{Context, Result};
use chrono::Local;

use log::*;
#[derive(Deserialize, Serialize, Debug, Clone)]
struct Thread {
    posts: Vec<ena::pgsql::core::Post>
}
fn main() {
    config::check_version();
    config::display();

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

    info!(
        "\nStarted on:\t{}\nFinished on:\t{}",
        start_time.to_rfc2822(),
        Local::now().to_rfc2822()
    );
}

async fn async_main() -> Result<u64> {
    let config = config::read_config("ena_config.json");
    let boards_len = config.boards.len();
    let asagi_mode = config.settings.asagi_mode;

    if config.boards.is_empty() {
        panic!("No boards specified");
    }

    // Find duplicate boards
    for info in &config.boards {
        let count = &config.boards.iter().filter(|&n| n.board == info.board).count();
        if *count > 1 {
            panic!("Multiple occurrences of `{}` found :: `{}`", info.board, count);
        }
    }

    if config.settings.asagi_mode && config.settings.engine.base() != Database::MySQL {
        unimplemented!("Asagi mode outside of MySQL. Found: `{}`", &config.settings.engine)
    }

    if !config.settings.asagi_mode && config.settings.engine.base() == Database::MySQL {
        unimplemented!("Only the Asagi schema is implemented for MySQL")
    }

    if config.settings.asagi_mode && !config.settings.strict_mode {
        unimplemented!(
            "The Asagi schema can only be used in strict mode because of MySQL's lack of concurrency"
        )
    }

    let http_client = reqwest::ClientBuilder::new()
        .default_headers(config::default_headers(&config.settings.user_agent).unwrap())
        .build()
        .expect("Err building the HTTP Client");

    let archiver;

    // Determine which engine is being used
    if config.settings.engine.base() == Database::PostgreSQL {
        let (db_client, connection) =
            tokio_postgres::connect(&config.settings.db_url, tokio_postgres::NoTls)
                .await
                .context(format!(
                    "\nPlease check your settings. Connection url used: {}",
                    config.settings.db_url
                ))
                .expect("Connecting to database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });
        info!("Connected with:\t\t{}", config.settings.db_url);
        archiver = MuhArchiver::new(Box::new(
            archiver::YotsubaArchiver::new(db_client, http_client, config).await
        ));
    } else {
        let pool = mysql_async::Pool::new(&config.settings.db_url);
        info!("Connected with:\t\t{}", config.settings.db_url);

        archiver = MuhArchiver::new(Box::new(
            archiver::YotsubaArchiver::new(pool, http_client, config).await
        ));
    }

    // Enjoy muh ASCII art!
    if boards_len < 10 && !asagi_mode {
        sleep(Duration::from_millis(1100)).await;
    }
    archiver.run().await;
    Ok(0)
}
