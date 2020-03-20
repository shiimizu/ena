//! HTTP clients and networking.
use async_trait::async_trait;
use reqwest::{
    self,
    header::{IF_MODIFIED_SINCE, LAST_MODIFIED},
    IntoUrl, StatusCode
};
use std::ops::Deref;

/// Implementation of the default HTTP client.  
/// A wrapper for [`reqwest`]
pub struct Client<T>(T);

#[allow(dead_code)]
impl<T> Client<T> {
    fn new(x: T) -> Client<T> {
        Client(x)
    }
}

impl<T> Deref for Client<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// Implementation of `HttpClient` for `reqwest`.
#[async_trait]
impl HttpClient for Client<reqwest::Client> {
    async fn get<U: IntoUrl + Send>(
        &self, url: U, last_modified: Option<&str>
    ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error> {
        // let url: &str = &url.into();
        match if let Some(lm) = last_modified {
            if lm.is_empty() {
                self.0.get(url)
            } else {
                self.0.get(url).header(IF_MODIFIED_SINCE, lm)
            }
        } else {
            self.0.get(url)
        }
        .send()
        .await
        {
            Ok(res) => {
                let lm = res
                    .headers()
                    .get(LAST_MODIFIED)
                    .map(|r| r.to_str().ok())
                    .flatten()
                    .unwrap_or("");

                Ok((lm.into(), res.status(), res.bytes().await.map(|b| b.to_vec())?))
            }
            Err(e) => Err(e)
        }
    }
}

#[async_trait]
pub trait HttpClient: Sync + Send {
    async fn get<U: IntoUrl + Send>(
        &self, url: U, last_modified: Option<&str>
    ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error>;
}

/// Implementation of `HttpClient` for `reqwest`.
#[async_trait]
impl HttpClient for reqwest::Client {
    async fn get<U: IntoUrl + Send>(
        &self, url: U, last_modified: Option<&str>
    ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error> {
        // let url: &str = &url.into();
        match if let Some(lm) = last_modified {
            if lm.is_empty() { self.get(url) } else { self.get(url).header(IF_MODIFIED_SINCE, lm) }
        } else {
            self.get(url)
        }
        .send()
        .await
        {
            Ok(res) => {
                let lm = res
                    .headers()
                    .get(LAST_MODIFIED)
                    .map(|r| r.to_str().ok())
                    .flatten()
                    .unwrap_or("");

                Ok((lm.into(), res.status(), res.bytes().await.map(|b| b.to_vec())?))
            }
            Err(e) => Err(e)
        }
    }
}

#[cfg(test)]
mod test {
    #[allow(unused_imports)]
    use super::*;
    #[test]
    fn get_http_improper_body() {}

    #[test]
    fn get_http_proper_body() {}
}
