use crate::state::TodoGroup;
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

const BALLCHASING_API_ROOT: &str = "https://ballchasing.com/api";

#[derive(Serialize, Deserialize, Debug)]
pub struct GroupSummary {
    pub id: String,
    pub name: String,
    pub direct_replays: Option<i64>,
    pub indirect_replays: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct GroupListing {
    list: Vec<GroupSummary>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReplaySummary {
    pub id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created: OffsetDateTime,
}

#[derive(Serialize, Deserialize, Debug)]
struct ReplayListing {
    list: Vec<ReplaySummary>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PingResponse {
    pub steam_id: String,
}

pub struct Fetcher {
    client: reqwest::Client,
    auth_token: String,
    rate_limiter: governor::DefaultDirectRateLimiter,
}

fn api_url(rel_path: &str) -> String {
    format!("{BALLCHASING_API_ROOT}/{rel_path}")
}

impl Fetcher {
    pub fn new(auth_token: String) -> Self {
        Self {
            auth_token,
            client: reqwest::Client::new(),
            rate_limiter: governor::RateLimiter::direct(
                governor::Quota::with_period(std::time::Duration::from_millis(500)).unwrap(),
            ),
        }
    }
    /// GETs the api root to test authentication and return the `steam_id` of the caller.
    pub async fn ping_server(&self) -> anyhow::Result<PingResponse> {
        self.fetch_json(api_url(""), Option::<&'_ [(&str, &str)]>::None)
            .await
    }

    pub async fn fetch_replay(&self, replay_id: &str) -> anyhow::Result<Value> {
        self.fetch_json(
            api_url(&format!("replays/{replay_id}")),
            Option::<&'_ [(&str, &str)]>::None,
        )
        .await
        .context("fetching replay")
    }

    pub async fn fetch_replay_ids(&self, parent_id: &str) -> anyhow::Result<Vec<ReplaySummary>> {
        let list: ReplayListing = self
            .fetch_json(api_url("replays"), Some(&[("group", parent_id)]))
            .await
            .context("listing replays")?;
        Ok(list.list)
    }
    pub async fn fetch_child_groups(&self, parent_id: &str) -> anyhow::Result<Vec<TodoGroup>> {
        let list: GroupListing = self
            .fetch_json(api_url("groups"), Some(&[("group", parent_id)]))
            .await?;

        let groups = list
            .list
            .into_iter()
            .map(TodoGroup::from)
            .filter(|tg| !tg.is_done())
            .collect();
        Ok(groups)
    }

    pub async fn fetch_creator_groups(&self, creator_id: &str) -> anyhow::Result<Vec<TodoGroup>> {
        let list: GroupListing = self
            .fetch_json(api_url("groups"), Some(&[("creator", creator_id)]))
            .await?;

        let groups = list
            .list
            .into_iter()
            .map(TodoGroup::from)
            .filter(|tg| !tg.is_done())
            .collect();
        Ok(groups)
    }

    #[tracing::instrument(level = "debug", skip(self, query))]
    async fn fetch_json<Q: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        url: String,
        query: Option<&Q>,
    ) -> anyhow::Result<T> {
        let Fetcher {
            client,
            auth_token,
            rate_limiter,
        } = self;

        loop {
            // Do our own rate limiting, so that we can avoid 429 responses in the common case
            rate_limiter.until_ready().await;

            let builder = client
                .get(url.as_str())
                .header("Authorization", auth_token.as_str());

            let builder = if let Some(q) = query {
                builder.query(q)
            } else {
                builder
            };

            let resp = builder.send().await.context("fetching url")?;
            let s = resp.status();
            if s == reqwest::StatusCode::OK {
                let body = resp
                    .json::<T>()
                    .await
                    .context("deserializing response body")?;
                return Ok(body);
            } else if s == reqwest::StatusCode::TOO_MANY_REQUESTS {
                // We'll just loop around and try again
                tracing::warn!("delaying in response to 429 status");
            } else {
                let body = resp.text().await;
                return Err(anyhow::anyhow!("response error {s:?}, body: {body:?}"));
            }
        }
    }
}
