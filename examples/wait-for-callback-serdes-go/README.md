# wait-for-callback-serdes-go

Demonstrates `operations.WithWaitForCallbackSerdes`: a custom
serializer/deserializer for the callback's own result. Mirrors the JS
reference SDK's own `wait-for-callback/serdes` example, simplified to
avoid that example's own `Date`-prototype-loss motivation (Go's
`encoding/json` already round-trips `time.Time` correctly by default,
unlike JS's `JSON.stringify`/`parse`) — this example's own custom
serdes instead demonstrates a genuinely different, verifiable wire
format (uppercase JSON keys) to have a real reason to exist.

## What it demonstrates

A custom `upperKeySerdes` serializes/deserializes the callback result
using UPPERCASE JSON keys (`MESSAGE`/`COMPLETEDAT`) instead of the
SDK's default lowercase-key JSON encoding. The test confirms the
checkpointed `CALLBACK` operation's own payload is genuinely in this
custom format, not the default one.

## A real local-testing limitation

`testing.Operation.SendCallbackSuccess` always marshals its own
argument via plain `encoding/json` — there is no way to inject an
already-serialized raw wire-format *string* the way a genuinely
external system (using the matching custom `Serialize`) would send.
See `handler_test.go`'s own top-level doc comment for how this test
still genuinely exercises `upperKeySerdes.Deserialize` despite that
gap (by passing `SendCallbackSuccess` a value whose own JSON tags
already match the custom wire shape).

## Local testing

```bash
go test ./...
```

## Deploying

Build and deploy exactly like `examples/simple-step-go` (see that
example's README for the full container-image deployment steps) —
this example was built with local-runner verification as the primary
goal and has not yet been deployed to a real AWS account.
