#![forbid(unsafe_code)]
#![deny(unsafe_code)]
#![allow(unreachable_code)]
#![allow(irrefutable_let_patterns)]
#![allow(clippy::range_plus_one)]

use ena::*;

use crate::sql::*;
// use serde::{self, Deserialize, Deserializer, Serialize};
// use serde_json::json;
// use std::collections::HashSet;

use tokio::{
    runtime::Builder,
    time::{delay_for as sleep, Duration}
};

use anyhow::{anyhow, Result};
use chrono::Local;
use log::*;
use std::io::Read;

fn main() {
    let start_time = Local::now();
    pretty_env_logger::try_init_timed_custom_env("ENA_LOG").unwrap();

    let ret = match Builder::new().enable_all().threaded_scheduler().build() {
        Ok(mut runtime) => runtime.block_on(async {
            match async_main().await {
                Ok(o) => o,
                Err(e) => {
                    error!("{}", e);
                    1
                }
            }
        }),
        Err(e) => {
            error!("{}", e);
            1
        }
    };

    if ret != 0 && ((Local::now().timestamp() - start_time.timestamp()) > 1) {
        info!(
            "\n Started on:\t{}\n Finished on:\t{}",
            start_time.to_rfc2822(),
            Local::now().to_rfc2822()
        );
    }
}

async fn async_main() -> Result<u64> {
    let mut args = std::env::args().skip(1).peekable();
    let mut config: Result<config::Config> = Err(anyhow!("Empty config file"));
    while let Some(arg) = args.next() {
        match arg.as_ref() {
            "--help" | "-h" => {
                config::display_help();
                return Ok(0);
            }
            "--version" | "-V" => {
                config::display_version();
                return Ok(0);
            }
            "--config" | "-c" =>
                if let Some(filename) = args.peek() {
                    if filename == "-" {
                        let mut file = String::new();
                        std::io::stdin().read_to_string(&mut file)?;
                        config::CONFIG_CONTENTS.set(file.clone()).unwrap();
                        let cfg = serde_json::from_str(&file)?;
                        config = Ok(config::read_config(cfg));
                    } else {
                        config::CONFIG_CONTENTS.set(std::fs::read_to_string(filename)?).unwrap();
                        match config::read_json_try(filename) {
                            Ok(cfg) => {
                                config = Ok(config::read_config(cfg));
                            }
                            Err(e) => return Err(anyhow!(e))
                        }
                    }
                    break;
                },
            unknown_arg => {
                return Err(anyhow!("Unknown argument: {}", unknown_arg))
            }
        }
    }
    if config.is_err() {
        let filename = "ena_config.json";
        config::CONFIG_CONTENTS
            .set(std::fs::read_to_string(filename).unwrap_or({
                if !std::path::Path::new(filename).exists() {
                    error!("`{}` not found! Using default built-in config file", filename);
                }
                serde_json::to_string(&config::ConfigInner::default())?
            }))
            .unwrap();
        let cfg: config::Config = serde_json::from_str::<config::Config>(
            &config::CONFIG_CONTENTS.get().expect("Empty config")
        )?;
        config = Ok(config::read_config(cfg));
    }
    let config = config?;
    let boards_len = config.boards.len();
    let asagi_mode = config.settings.asagi_mode;

    if config.boards.is_empty() {
        return Err(anyhow!("No boards specified"));
    }

    // Find duplicate boards
    for info in &config.boards {
        let count = &config.boards.iter().filter(|&n| n.board == info.board).count();
        if *count > 1 {
            return Err(anyhow!("Multiple occurrences of `{}` found :: `{}`", info.board, count));
        }
    }

    if config.settings.asagi_mode && config.settings.engine.base() != Database::MySQL {
        return Err(anyhow!("Asagi mode outside of MySQL. Found: `{}`", &config.settings.engine));
    }

    if !config.settings.asagi_mode && config.settings.engine.base() == Database::MySQL {
        return Err(anyhow!("Only the Asagi schema is implemented for MySQL"));
    }

    let http_client = reqwest::ClientBuilder::new()
        .default_headers(config::default_headers(&config.settings.user_agent).unwrap())
        .build()
        .expect("Err building the HTTP Client");

    let archiver;
    // Determine which engine is being used
    if config.settings.engine.base() == Database::PostgreSQL {
        let res = tokio_postgres::connect(&config.settings.db_url, tokio_postgres::NoTls).await;
        match res {
            Ok((db_client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        error!("Connection error: {}", e);
                    }
                });
                config::display();
                info!("Connected with:\t\t{}", config.settings.db_url);

                archiver = MuhArchiver::new(Box::new(
                    archiver::YotsubaArchiver::new(db_client, http_client, config).await
                ));
            }
            Err(e) => {
                error!("Please check your configuration");
                error!("Connection URL used: {}", config.settings.db_url);
                return Err(anyhow!(e));
            }
        }
    } else {
        let pool = mysql_async::Pool::new(&config.settings.db_url);
        config::display();
        info!(
            "Connected with:\t\t{}",
            format!(
                "{engine}://{username}:{password}@{host}:{port}/{database}",
                engine = &config.settings.engine.base().to_string().to_lowercase(),
                username = "*********",
                password = "***",
                host = &config.settings.host,
                port = &config.settings.port,
                database = &config.settings.database
            )
        );

        archiver = MuhArchiver::new(Box::new(
            archiver::YotsubaArchiver::new(pool, http_client, config).await
        ));
    }

    if boards_len < 10 && !asagi_mode {
        sleep(Duration::from_millis(1100)).await;
    }
    archiver.run().await;
    Ok(1)
}
