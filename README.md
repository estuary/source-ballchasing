# Rocket League stats

This is an Flow capture connector that pulls Rocket League game stats from the [Ballchasing API](https://ballchasing.com/).

The connector captures all game stats for a given `creatorId`, which can be either a Steam user id, or the special value `me`.
The connector will periodically "sweep" the ballchasing API, recursively listing all Groups and fetching the Replays under them.


## Tutorial

**Prerequisites:**

- A [Ballchasing API key](https://ballchasing.com/login): Follow the linked instructions and copy the API key
- A free account at [Estuary](https://dashboard.estuary.dev/), take note of the tenant name you chose when signing up
- The `flowctl` CLI: [installation instructions](https://docs.estuary.dev/getting-started/installation/#get-started-with-the-flow-cli)
    - Alternatively, you can use [Gitpod with `flowctl` pre-installed](https://gitpod.io/new#https://github.com/estuary/flow-gitpod-base)
	- Run `flowctl auth login` to authenticate the CLI with Estuary

**Note:** This connector is currently only usable via the `flowctl` CLI. It cannot be setup using the UI.

**Usage:**

Create a file called `flow.yaml` with the following contents, replacing the tenant name and `authToken` with your own values.

```yaml
captures:
  <my-tenant>/rl-stats/source-ballchasing:
    endpoint:
      connector:
        image: source-ballchasing:local
        config:
          authToken: '<Your-ballchasing-api-key-here>'
    bindings:
      - target: '<my-tenant>/rl-stats/games'
        resource:
          creatorId: 'me'

collections:
  <my-tenant>/rl-stats/games:
    writeSchema:
      type: object
      properties:
        id: { type: string }
      required: [id]
    readSchema:
      allOf:
        - $ref: flow://write-schema
        - $ref: flow://inferred-schema
    key: [/id]
```

If you want to ingest replays from another user, then change the `creatorId` under `bindings` to be that users steam id.

Once you've got all the values replaced, open up a terminal and run `flowctl catalog publish --source flow.yaml`.

This will create two things. You'll get a Flow Collection, which you can view under [collections](https://dashboard.estuary.dev/collections) in the UI. You'll be able to "materialize" this collection into any number of destination systems like databases, spreadsheets, etc. You'll also get a Capture task, which periodically queries the ballchasing API and ingests replay stats into the collection. You can see the capture task under [sources](https://dashboard.estuary.dev/captures) in the UI, though you won't be able to edit it there.

## Implementation details

The connector persists the starting timestamp of each sweep, and uses it to filter replays on subsequent sweeps. This minimizes the chances of ingesting the same replay twice, though it does not guarantee that it won't happen. All replays are automatically deduplicated by `id`, so this is really just for efficiency. "Exactly-once" ingestion of replays is likely possible, though it was considered unnecessary given the ability to easily deduplicate by replay id.

**Build:** `docker build --platform linux/amd64 .`
