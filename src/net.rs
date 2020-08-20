use async_trait::async_trait;
use color_eyre::eyre::Result;
use reqwest::{
    self,
    header::{IF_MODIFIED_SINCE, LAST_MODIFIED},
    IntoUrl, StatusCode,
};
use std::fmt::Debug;

#[async_trait]
pub trait HttpClient: Sync + Send {
    async fn gett<U: IntoUrl + Send + Debug + Clone>(&self, url: U, last_modified: &Option<String>) -> Result<(StatusCode, String, Vec<u8>)>;
}

/// Implementation of `HttpClient` for `reqwest`.
#[async_trait]
impl HttpClient for reqwest::Client {
    async fn gett<U: IntoUrl + Send + Debug + Clone>(&self, url: U, last_modified: &Option<String>) -> Result<(StatusCode, String, Vec<u8>)> {
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

        // Last-Modified Should never be empty is status is OK
        // Which we always check after this method is called
        let lm = res.headers().get(LAST_MODIFIED).map(|r| r.to_str().ok()).flatten().unwrap_or("");

        Ok((res.status(), lm.into(), res.bytes().await.map(|b| b.to_vec()).unwrap_or(vec![])))
    }
}
