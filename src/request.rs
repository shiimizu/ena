use async_trait::async_trait;
use reqwest::{
    self,
    header::{IF_MODIFIED_SINCE, LAST_MODIFIED},
    IntoUrl, StatusCode
};

#[async_trait]
pub trait HttpClient {
    async fn cget<U: IntoUrl + Send>(
        &self,
        url: U,
        last_modified: Option<&str>
    ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error>;
}

/// Http Client wrapper to  promote modularity and extensibility.
///
/// Just add your desired http client with the `HttpClient` trait and implement
/// `cget`.
pub struct YotsubaHttpClient<T: HttpClient> {
    // pub components: Vec<T>,
    impl_client: T
}

impl<T> YotsubaHttpClient<T>
where T: HttpClient
{
    pub fn new(new_client: T) -> Self {
        Self {
            // components: vec![],
            impl_client: new_client
        }
    }

    /// Get the stuff
    pub async fn get<U: IntoUrl + Send>(
        &self,
        url: U,
        last_modified: Option<&str>
    ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error>
    {
        self.impl_client.cget(url, last_modified).await
    }

    // pub fn run(&self) {
    //     for component in self.components.iter() {
    //         component.get();
    //     }
    // }
}

/// Implementation for `reqwest`.
#[async_trait]
impl HttpClient for reqwest::Client {
    async fn cget<U: IntoUrl + Send>(
        &self,
        url: U,
        last_modified: Option<&str>
    ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error>
    {
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

// pub async fn cget<'a, S>(
//     client: &reqwest::Client,
//     url: S,
//     last_modified: Option<&str>
// ) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error>
// where
//     S: Into<std::borrow::Cow<'a, str>>
// {
//     let url: &str = &url.into();
//     match if let Some(lm) = last_modified {
//         if lm.is_empty() { client.get(url) } else {
// client.get(url).header(IF_MODIFIED_SINCE, lm) }     } else {
//         client.get(url)
//     }
//     .send()
//     .await
//     {
//         Ok(res) => {
//             let lm =
//                 res.headers().get(LAST_MODIFIED).map(|r|
// r.to_str().ok()).flatten().unwrap_or("");

//             Ok((lm.into(), res.status(), res.bytes().await.map(|b|
// b.to_vec())?))         }
//         Err(e) => Err(e)
//     }
// }
