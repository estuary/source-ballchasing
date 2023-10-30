use crate::{
    fetcher::{Fetcher, ReplaySummary, Visibility},
    state::{BindingState, State, TodoGroup},
    write_capture_response, EndpointConfig, ResourceConfig,
};
use std::collections::BTreeMap;

use crate::transactor::Emitter;
use anyhow::Context;
use proto_flow::{
    capture::{request::Open, response::Opened, Response},
    flow::CaptureSpec,
};
use serde::{Deserialize, Serialize};

use time::OffsetDateTime;
use tokio::io;

#[derive(Serialize, Deserialize, Debug)]
pub struct ParentGroup {
    id: String,
    name: String,
}

pub async fn do_pull(
    Open {
        capture,
        state_json,
        ..
    }: Open,
    _stdin: io::BufReader<io::Stdin>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    tracing::info!("starting to pull");
    let Some(CaptureSpec { config_json, bindings, .. }) = capture else {
        anyhow::bail!("open request is missing capture spec");
    };

    let config = serde_json::from_str::<EndpointConfig>(&config_json)
        .context("deserializing endpoint config")?;

    let fetcher = Fetcher::new(config.auth_token);

    let mut state: State = if state_json.trim().is_empty() {
        State::default()
    } else {
        serde_json::from_str(&state_json).context("deserializing state checkpoint")?
    };

    let mut binding_indices = BTreeMap::new();
    for (i, binding) in bindings.iter().enumerate() {
        let collection_name = binding
            .collection
            .as_ref()
            .map(|spec| spec.name.as_str())
            .expect("binding must have collection name");
        let resource_config: ResourceConfig = serde_json::from_str(&binding.resource_config_json)
            .context("deserializing resource config")?;

        let binding_key = format!("{};{}", resource_config.creator_id, collection_name);
        if !state.bindings.contains_key(&binding_key) {
            tracing::info!(%binding_key, "initializing new empty state for binding");
            state.bindings.insert(
                binding_key.clone(),
                BindingState::new(collection_name, &resource_config.creator_id),
            );
        }
        binding_indices.insert(binding_key, i as u32);
    }

    state.bindings.retain(|k, v| {
        if binding_indices.contains_key(k) {
            true
        } else {
            tracing::info!(binding_key = %k, state = ?v, "clearing out unused state");
            false
        }
    });

    let resp = Response {
        opened: Some(Opened {
            explicit_acknowledgements: true,
        }),
        ..Default::default()
    };
    write_capture_response(resp, &mut stdout).await?;

    let mut emitter = Emitter(stdout);

    run_sweep(binding_indices, &mut state, &fetcher, &mut emitter).await
}

async fn run_sweep(
    binding_indices: BTreeMap<String, u32>,
    state: &mut State,
    fetcher: &Fetcher,
    emitter: &mut Emitter,
) -> anyhow::Result<()> {
    let ping_response = fetcher
        .ping_server()
        .await
        .context("failed to ping server")?;
    let caller_steam_id = ping_response.steam_id;

    // Is there an in-progress sweep? If not, then we'll start one.
    for binding_state in state.bindings.values_mut() {
        if binding_state.sweep_start.is_none() {
            binding_state.start_sweep(fetcher).await?;
        }
    }

    tracing::debug!("runnning sweep");

    while state.bindings.values().any(|b| !b.is_sweep_complete()) {
        for (binding_key, binding_state) in state.bindings.iter_mut() {
            if binding_state.is_sweep_complete() {
                continue;
            }
            tracing::debug!(%binding_key, ?binding_state, todo_groups = binding_state.todo_groups.len(), "checking for next replays");
            if let Some((lineage, replays)) =
                next_replays(binding_state, fetcher, &caller_steam_id).await?
            {
                tracing::debug!(%binding_key, ?lineage, num_replays = replays.len(), "found replays to fetch");
                let binding_idx = binding_indices.get(binding_key).unwrap();
                ingest_replays(lineage, *binding_idx, &replays, fetcher, emitter)
                    .await
                    .context("ingesting replays")?;
                tracing::debug!(%binding_key, num_replays = replays.len(), "finished processing replays");
            } else {
                tracing::debug!("no replays found under group");
            }
        }
        tracing::debug!("persisting state");
        emitter.commit(&*state, false).await?;
    }
    tracing::debug!("sweep complete, pending state update");
    for binding_state in state.bindings.values_mut() {
        binding_state.last_completed_sweep = binding_state.sweep_start.take();
    }

    emitter.commit(&*state, false).await?;

    Ok(())
}

fn lineage_info(grp: &TodoGroup) -> ParentGroup {
    ParentGroup {
        id: grp.id.clone(),
        name: grp.name.clone(),
    }
}

fn should_ingest(
    last_completed_sweep: Option<OffsetDateTime>,
    replay: &ReplaySummary,
    caller_steam_id: &str,
) -> bool {
    // Filter out replays that we've already captured
    if !last_completed_sweep
        .map(|sc| replay.created > sc)
        .unwrap_or(true)
    {
        return false;
    }

    // Filter out replays that we don't have permission to download
    if replay.visibility.unwrap_or(Visibility::Public) == Visibility::Public {
        true
    } else if replay.uploader.steam_id == caller_steam_id {
        true
    } else {
        tracing::warn!(?replay, %caller_steam_id, "skipping replay because it is not public and does not belong to the caller");
        false
    }
}

/// Does a depth-first search of the graph of groups. Does not use recursion
/// because async rust does not yet allow it
#[tracing::instrument(skip(fetcher), level = "debug")]
async fn next_replays(
    state: &mut BindingState,
    fetcher: &Fetcher,
    caller_steam_id: &str,
) -> anyhow::Result<Option<(Vec<ParentGroup>, Vec<ReplaySummary>)>> {
    let BindingState {
        last_completed_sweep,
        todo_groups,
        ..
    } = state;

    todo_groups.retain(|g| !g.is_done());

    let Some(grp) =  todo_groups.front_mut() else {
        return Ok(None);
    };

    let mut next_group: &mut TodoGroup = grp;
    let mut lineage = vec![lineage_info(&*next_group)];

    loop {
        // Does this group contain direct replays?
        if next_group.must_fetch_replays {
            next_group.must_fetch_replays = false;
            let mut replays = fetcher.fetch_replay_ids(&next_group.id).await?;
            replays.retain(|rp| should_ingest(*last_completed_sweep, rp, caller_steam_id));
            if !replays.is_empty() {
                return Ok(Some((lineage, replays)));
            }
        }
        // Does this group maybe have any children?
        if next_group.must_fetch_children {
            let children = fetcher
                .fetch_child_groups(&next_group.id)
                .await
                .context("fetching child groups")?;
            next_group.must_fetch_children = false;
            next_group.children.extend(children);
        }

        next_group.children.retain(|g| !g.is_done());
        if next_group.children.is_empty() {
            return Ok(None);
        } else {
            // Descend into DFS of the child
            next_group = next_group.children.front_mut().unwrap();
            lineage.push(lineage_info(&*next_group));
        }
    }
}

async fn ingest_replays(
    lineage: Vec<ParentGroup>,
    binding: u32,
    replays: &[ReplaySummary],
    fetcher: &Fetcher,
    emitter: &mut Emitter,
) -> anyhow::Result<()> {
    let meta = serde_json::json!({ "parent_groups": lineage });

    for replay in replays {
        let mut replay_json = match fetcher.fetch_replay(&replay.id).await {
            Ok(rp) => rp,
            Err(err) => {
                tracing::warn!(?lineage, ?binding, ?replay, error = ?err, "failed to fetch replay");
                return Err(err);
            }
        };
        replay_json
            .as_object_mut()
            .expect("replay must be an object")
            .insert("_meta".to_string(), meta.clone());
        emitter.emit_doc(binding, &replay_json).await?;
    }
    Ok(())
}
