use crate::config::Opt;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
#[allow(unused_imports)]
use fomat_macros::{epintln, fomat, pintln};
use reqwest::{
    header::{IF_MODIFIED_SINCE, LAST_MODIFIED},
    Client, ClientBuilder, IntoUrl, StatusCode,
};
use std::fmt::Debug;

#[async_trait]
pub trait HttpClient: Sync + Send {
    async fn gett<U: IntoUrl + Send + Debug + Clone>(
        &self,
        url: U,
        last_modified: Option<&String>,
    ) -> Result<(StatusCode, String, Vec<u8>)>;
}

/// Implementation of `HttpClient` for `reqwest`.
#[async_trait]
impl HttpClient for Client {
    async fn gett<U: IntoUrl + Send + Debug + Clone>(
        &self,
        url: U,
        last_modified: Option<&String>,
    ) -> Result<(StatusCode, String, Vec<u8>)> {
        // let url: &str = url.into();
        // let _url = url.clone();
        let res = {
            if let Some(lm) = last_modified {
                if lm.is_empty() {
                    self.get(url)
                } else {
                    self.get(url).header(IF_MODIFIED_SINCE, lm)
                }
            } else {
                self.get(url)
            }
            .send()
            .await
        }?;

        // Last-Modified Should never be empty where status is OK
        // Which we always check after this method is called
        let status = res.status();
        let last_modified = res
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|r| r.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = res.bytes().await.map(|b| b.to_vec()).unwrap_or_default();

        Ok((status, last_modified, body))
    }
}

#[rustfmt::skip]
pub async fn create_client(origin: &str, opt: &Opt) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Connection", "keep-alive".parse()?);
    headers.insert("Origin", origin.parse()?);
    let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0";
    let mut proxies_list = vec![];

    let create_client_builder =
        |headers: reqwest::header::HeaderMap, ua: &str| -> ClientBuilder {
            reqwest::Client::builder()
                    .default_headers(headers)
                    .user_agent(ua)
                    .use_rustls_tls()
                    .gzip(true)
                    .brotli(true)
        };

    if let Some(proxies) = &opt.proxies {
        // Test each proxy with retries
        for proxy in proxies {
            let proxy_url = &proxy.url;
            if proxy_url.is_empty() {
                continue;
            }
            let username = proxy.username.as_ref();
            let password = proxy.password.as_ref();
            let mut _proxy = reqwest::Proxy::https(proxy_url.as_str())?;
            // Add credentials. A password may be empty but a username must be something.
            if let Some(user) = username {
                if !user.is_empty() {
                    let pass = password.map(|s| s.as_str()).unwrap_or_default();
                    _proxy = _proxy.basic_auth(user, pass);
                }
            }
            proxies_list.push(_proxy.clone());
            for retry in 0..=3u8 {
                let mut cb = create_client_builder(headers.clone(), ua);
                let mut cb = cb.proxy(_proxy.clone()).build()?;
                pintln!("Testing proxy: "(proxy_url));
                match cb.head("https://a.4cdn.org/po/catalog.json").send().await {
                    Err(e) => epintln!("Error HEAD request: "(e)),
                    Ok(resp) => {
                        let status = resp.status();
                        if status == StatusCode::OK {
                            break;
                        } else {
                            if retry == 3 {
                                epintln!("Proxy: " (proxy_url) " ["(status)"]");
                                proxies_list.pop();
                            }
                        }
                    }
                }
            }
        }
        
        if proxies_list.len() > 0 {
            let mut cb = create_client_builder(headers, ua);
            for proxy in proxies_list {
                cb = cb.proxy(proxy);
            }
            cb.build().map_err(|e| anyhow!(e))
        } else {
            create_client_builder(headers, ua).no_proxy().build().map_err(|e| anyhow!(e))
        }
    } else {
        // same as above
        create_client_builder(headers, ua).no_proxy().build().map_err(|e| anyhow!(e))
    }
}
