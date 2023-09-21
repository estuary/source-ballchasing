use std::collections::{BTreeMap, VecDeque};

use crate::fetcher::{Fetcher, GroupSummary};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Debug)]
pub struct BindingState {
    pub collection_name: String,
    pub creator_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sweep_start: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_sweep: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    pub todo_groups: VecDeque<TodoGroup>,
}

impl BindingState {
    pub fn new(collection: impl Into<String>, creator_id: impl Into<String>) -> BindingState {
        BindingState {
            collection_name: collection.into(),
            creator_id: creator_id.into(),
            sweep_start: None,
            last_completed_sweep: None,
            todo_groups: VecDeque::new(),
        }
    }

    pub async fn start_sweep(&mut self, fetcher: &Fetcher) -> anyhow::Result<()> {
        tracing::info!(creator_id = %self.creator_id, "starting sweep");
        self.sweep_start = Some(OffsetDateTime::now_utc());
        let groups = fetcher.fetch_creator_groups(&self.creator_id).await?;
        tracing::info!(creator_id = %self.creator_id, group_count = %groups.len(), "fetched top-level groups for creator");
        self.todo_groups.extend(groups);
        Ok(())
    }

    pub fn is_sweep_complete(&self) -> bool {
        self.todo_groups.is_empty()
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct State {
    /// The state is keyed on the combination of the creator id and the Flow
    /// collection name, so that we can easily throw away the state and start
    /// over if either of those things changes.
    #[serde(default)]
    pub bindings: BTreeMap<String, BindingState>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TodoGroup {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub must_fetch_children: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub must_fetch_replays: bool,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    pub children: VecDeque<TodoGroup>,
}

impl TodoGroup {
    pub fn is_done(&self) -> bool {
        !self.must_fetch_children && !self.must_fetch_replays && self.children.is_empty()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ParentGroup {
    pub id: String,
    pub name: String,
}

impl From<GroupSummary> for TodoGroup {
    fn from(gs: GroupSummary) -> TodoGroup {
        let must_fetch_children = gs.indirect_replays.is_some_and(|n| n > 0);
        let must_fetch_replays = gs.direct_replays.is_some_and(|n| n > 0);
        TodoGroup {
            id: gs.id,
            name: gs.name,
            must_fetch_children,
            must_fetch_replays,
            children: VecDeque::new(),
        }
    }
}
