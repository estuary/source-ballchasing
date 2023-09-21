use crate::{read_capture_request, write_capture_response};
use anyhow::Context;
use proto_flow::{
    capture::{
        response::{Captured, Checkpoint},
        Response,
    },
    flow::ConnectorState,
};
use serde::Serialize;
use tokio::io;

pub struct Acknowledgements(io::BufReader<io::Stdin>);
impl Acknowledgements {
    pub async fn next_ack(&mut self) -> anyhow::Result<anyhow::Result<()>> {
        let req = read_capture_request(&mut self.0).await?;
        if req.acknowledge.is_none() {
            anyhow::bail!("expected Acknowledge message, got: {:?}", req);
        };
        Ok(Ok(()))
    }
}

pub struct Emitter(pub io::Stdout);

impl Emitter {
    pub async fn emit_doc(&mut self, binding: u32, doc: &impl Serialize) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;

        // We intentionally don't use `write_capture_response` here because we don't want to wait
        // for the flush call to complete.
        let doc_json = serde_json::to_string(doc).context("serializing output document")?;

        let resp = Response {
            captured: Some(Captured { binding, doc_json }),
            ..Default::default()
        };
        let resp = serde_json::to_vec(&resp).context("serializing response")?;
        self.0.write_all(&resp).await.context("writing response")?;
        self.0
            .write_u8(b'\n')
            .await
            .context("writing response newline")?;
        Ok(())
    }

    pub async fn commit(&mut self, cp: &impl Serialize, merge_patch: bool) -> anyhow::Result<()> {
        let updated_json = serde_json::to_string(cp).context("serializing driver checkpoint")?;
        let resp = Response {
            checkpoint: Some(Checkpoint {
                state: Some(ConnectorState {
                    updated_json,
                    merge_patch,
                }),
            }),
            ..Default::default()
        };
        write_capture_response(resp, &mut self.0)
            .await
            .context("writing checkpoint")
    }
}
