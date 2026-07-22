# custom-config-go

Demonstrates `durable.Config` customization: a non-default
`CheckpointStrategy`, a custom `LoggerConfig`, and a custom
`types.Serdes` applied to a single step's checkpointed result.

## What it demonstrates

`main.go`'s `durable.Config` literal sets three fields most examples
in this repo leave at their defaults:

1. **`CheckpointStrategy: durable.CheckpointStrategyBatched`** — shown
   for API-surface completeness only. **This is currently a
   documented no-op**: `durable.go`'s own doc comment on
   `CheckpointStrategy` states plainly that "the current runtime
   implementation always batches eagerly via `checkpoint.Manager`'s
   drain loop ... accepted here for API compatibility with the design
   surface but not yet wired to distinct runtime behavior." This
   example's own comments repeat that honestly rather than implying
   checkpoint batching actually changes as a result of setting this
   field today.
2. **`LoggerConfig: &types.LoggerConfig{ModeAware: true}`** — a
   custom logger configuration (see `examples/logging-go` for a
   dedicated deep dive into what `ModeAware` actually does; this
   example just shows it as one of several `Config` fields a caller
   might set together).
3. **A custom `types.Serdes`** (`screamingSnakeCaseSerdes` in
   `handler.go`), applied to one step via `operations.WithStepSerdes`
   — no other example in this repo demonstrates a custom `Serdes`
   (confirmed by grepping the `examples/` tree before writing this).
   It re-keys the step's checkpointed JSON from the struct's normal
   `snake_case` tags to `SCREAMING_SNAKE_CASE` on the wire and reverses
   the transform on deserialize — a simple, self-contained
   demonstration of the `types.Serdes` extensibility point that needs
   no additional infrastructure (unlike the officially-documented
   `FileSystem SerDes` large-payload-offload pattern — see
   `pkg/durable/operations/errors.go`'s research notes on
   `docs/remaining-work.md` §6 task 16 for why that's a separate,
   larger feature this example does not attempt).

## Local testing

```bash
go test ./...
```

- `TestHandler_SnapshotsInventory` — the ordinary result/operation
  assertions plus an event-history golden file.
- `TestHandler_ChecksSnapshotUsesCustomSerdes` — asserts on the RAW
  checkpointed payload string, confirming it genuinely contains
  `SCREAMING_SNAKE_CASE` keys (`"ITEM_COUNT"`, `"SNAPSHOTTED_AT"`) and
  not the original `snake_case` ones — proving the custom `Serdes`
  actually ran, since a silently-ignored `WithStepSerdes` option would
  still produce a correctly round-tripping Go value via the default
  JSON `Serdes` (both tolerate this struct's tags equally well), making
  the deserialized value alone insufficient proof on its own.

This is the local half of the completion criteria described in
`docs/remaining-work.md`; a cloud-runner counterpart does not exist
yet (see that document's task 17a).

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
