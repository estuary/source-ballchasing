pub mod fetcher;
pub mod pull;
pub mod state;
pub mod transactor;

use std::mem;

use self::fetcher::Fetcher;
use anyhow::Context;

use proto_flow::capture::{
    request::validate::Binding as ValidateBinding,
    response::validated::Binding as ValidatedBinding,
    response::{discovered::Binding as DiscoveredBinding, Applied},
    response::{Discovered, Spec, Validated},
    Request, Response,
};
use schemars::{schema::RootSchema, JsonSchema};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt};

#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct EndpointConfig {
    /// Authentication token for the ballchasing api.
    ///
    /// If you don't have one, get one by visiting:
    /// https://ballchasing.com/login
    auth_token: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConfig {
    /// The creator id to filter replays in ballchasing.
    /// Only replays in groups for this creator will be ingested.
    creator_id: String,
}

fn schema_for<T: JsonSchema>() -> RootSchema {
    schemars::gen::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}

pub async fn run_connector(
    mut stdin: io::BufReader<io::Stdin>,
    stdout: io::Stdout,
) -> Result<(), anyhow::Error> {
    let req = read_capture_request(&mut stdin)
        .await
        .context("reading request")?;
    let Request {
        spec,
        discover,
        validate,
        apply,
        open,
        ..
    } = req;
    if let Some(_) = spec {
        return do_spec(stdout).await;
    }
    if let Some(mut discover_req) = discover {
        return do_discover(mem::take(&mut discover_req.config_json), stdout).await;
    }
    if let Some(mut validate_req) = validate {
        return do_validate(
            mem::take(&mut validate_req.config_json),
            mem::take(&mut validate_req.bindings),
            stdout,
        )
        .await;
    }
    if let Some(_) = apply {
        return do_apply(stdout).await;
    }
    if let Some(open_req) = open {
        return pull::do_pull(open_req, stdin, stdout).await;
    }
    Err(anyhow::anyhow!(
        "invalid request, expected spec|discover|validate|apply|open"
    ))
}

async fn do_apply(mut stdout: io::Stdout) -> anyhow::Result<()> {
    // There's nothing to apply
    write_capture_response(
        Response {
            applied: Some(Applied {
                action_description: String::new(),
            }),
            ..Default::default()
        },
        &mut stdout,
    )
    .await
}

async fn do_spec(mut stdout: io::Stdout) -> anyhow::Result<()> {
    let config_schema_json = serde_json::to_string(&schema_for::<EndpointConfig>())?;
    let resource_config_schema_json = serde_json::to_string(&schema_for::<ResourceConfig>())?;
    let response = Response {
        spec: Some(Spec {
            protocol: 3032023,
            config_schema_json,
            resource_config_schema_json,
            documentation_url: "https://go.estuary.dev/placeholder".to_string(),
            oauth2: None,
            resource_path_pointers: vec!["/creatorId".to_string()],
        }),
        ..Default::default()
    };
    write_capture_response(response, &mut stdout).await
}

async fn do_discover(config: String, mut stdout: io::Stdout) -> anyhow::Result<()> {
    // make sure we can parse the config, just as an extra sanity check
    let endpoint_config =
        serde_json::from_str::<EndpointConfig>(&config).context("parsing endpoint config")?;

    let fetcher = Fetcher::new(endpoint_config.auth_token);
    let ping_response = fetcher
        .ping_server()
        .await
        .context("failed to connect to ballchasing api")?;

    let bindings = vec![discovered_collection(ping_response.steam_id)];
    let response = Response {
        discovered: Some(Discovered { bindings }),
        ..Default::default()
    };
    write_capture_response(response, &mut stdout).await
}

async fn do_validate(
    config: String,
    bindings: Vec<ValidateBinding>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    let endpoint_config =
        serde_json::from_str::<EndpointConfig>(&config).context("deserializing endpoint config")?;
    let fetcher = Fetcher::new(endpoint_config.auth_token);
    let ping_response = fetcher
        .ping_server()
        .await
        .context("failed to connect to ballchasing api")?;
    tracing::info!(?ping_response, "successfully pinged the ballchasing API");
    let mut output = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let resource_config = serde_json::from_str::<ResourceConfig>(&binding.resource_config_json)
            .context("deserializing resource config")?;

        let groups = fetcher
            .fetch_creator_groups(&resource_config.creator_id)
            .await
            .context("fetching groups for creator_id")?;
        tracing::info!(num_groups = %groups.len(), creator_id = %resource_config.creator_id, "fetched groups for creator");

        let resource_path = vec![resource_config.creator_id];

        output.push(ValidatedBinding {
            resource_path: resource_path.clone(),
        });
    }

    let response = Response {
        validated: Some(Validated { bindings: output }),
        ..Default::default()
    };
    write_capture_response(response, &mut stdout).await
}

pub async fn read_capture_request(stdin: &mut io::BufReader<io::Stdin>) -> anyhow::Result<Request> {
    let mut buf = String::with_capacity(4096);
    stdin
        .read_line(&mut buf)
        .await
        .context("reading next request line")?;
    if buf.trim().is_empty() {
        anyhow::bail!("unexpected EOF reading request from stdin");
    }
    let deser = serde_json::from_str(&buf).context("deserializing request")?;
    Ok(deser)
}

/// Writes the response to stdout, and waits to a flush to complete. The flush ensures that the complete
/// response will be written, even if the runtime is immediately shutdown after this call returns.
pub async fn write_capture_response(
    response: Response,
    stdout: &mut io::Stdout,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let resp = serde_json::to_vec(&response).context("serializing response")?;
    stdout.write_all(&resp).await.context("writing response")?;
    stdout
        .write_u8(b'\n')
        .await
        .context("writing response newline")?;
    stdout.flush().await?;
    Ok(())
}

fn discovered_collection(steam_id: String) -> DiscoveredBinding {
    DiscoveredBinding {
        disable: false,
        recommended_name: "replays".to_string(),
        resource_config_json: serde_json::to_string(&ResourceConfig {
            creator_id: steam_id,
        })
        .unwrap(),
        document_schema_json: serde_json::to_string(&serde_json::json!({
            "type": "object",
            "x-infer-schema": true,
            "properties": {
                "_meta": {
                    "type": "object",
                    "properties": {
                        "parent_groups": {

                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": { "type": "string" },
                                    "name": { "type": "string" }
                                },
                                "required": ["name", "id"]
                            }
                        }
                    }
                },
                "id": { "type": "string" }
            },
            "required": ["_meta", "id"]
        }))
        .unwrap(),
        key: vec!["/id".to_string()],
        resource_path: Vec::new(), // resource_path is deprecated and unused
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn endpoint_config_schema() {
        let schema = schema_for::<EndpointConfig>();
        insta::assert_json_snapshot!(schema);
    }

    #[test]
    fn resource_config_schema() {
        let schema = schema_for::<ResourceConfig>();
        insta::assert_json_snapshot!(schema);
    }
}
