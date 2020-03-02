use reqwest::{
    self,
    header::{IF_MODIFIED_SINCE, LAST_MODIFIED},
    StatusCode
};

pub async fn cget<'a, S>(
    client: &reqwest::Client,
    url: S,
    last_modified: Option<&str>,
    _retry_attempts: u16,
    _throttle_millisec: u32
) -> Result<(String, StatusCode, Vec<u8>), reqwest::Error>
where
    S: Into<std::borrow::Cow<'a, str>>
{
    let url: &str = &url.into();
    match if let Some(lm) = last_modified {
        if !lm.is_empty() { client.get(url).header(IF_MODIFIED_SINCE, lm) } else { client.get(url) }
    } else {
        client.get(url)
    }
    .send()
    .await
    {
        Ok(res) => {
            let lm =
                res.headers().get(LAST_MODIFIED).map(|r| r.to_str().ok()).flatten().unwrap_or("");

            Ok((lm.into(), res.status(), res.bytes().await.map(|b| b.to_vec())?))
        }
        Err(e) => Err(e)
    }
}
