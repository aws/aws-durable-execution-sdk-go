# durablelint

`durablelint` is a static analyzer for handlers written against the AWS
Durable Execution SDK for Go. It reports code that breaks replay before the
code is deployed. It is built on the standard Go analysis framework
(`golang.org/x/tools/go/analysis`), so it runs as a standalone binary, under
`go vet -vettool`, or inside golangci-lint.

## Installation

```console
go install github.com/aws/aws-durable-execution-sdk-go/analysis/cmd/durablelint@latest
```

## Usage

Run it over the packages of a module, from the module root:

```console
durablelint ./...
```

Or as a vet tool:

```console
go vet -vettool=$(which durablelint) ./...
```

Each rule is a flag. To switch a rule off:

```console
durablelint -durablenondeterminism=false ./...
```

`durablelint help` lists every rule with its full description.
`durablelint -json ./...` prints diagnostics as JSON.

## Rules

The rules recognise the SDK's operations by import path, so aliased imports
are handled. A *durable operation* below is any SDK function that claims an
operation ID on a `Context`: `Step`, `StepAsync`, `Wait`, `WaitAsync`,
`Invoke`, `InvokeAsync`, `RunInChildContext`, `RunInChildContextAsync`, `Go`,
`WaitForCondition`, `CreateCallback`, `WaitForCallback`, `Map`, `Parallel`,
`Select`, `Retry`, `All`, `AllSettled`, `Any`, `Race`, and `Join`. A *step
body* is the function passed to `Step` or `StepAsync`, the check function of
`WaitForCondition`, or the submitter of `WaitForCallback`.

### durablegoroutine

Reports a durable operation that runs on a goroutine the SDK did not start.
A `Context` is owned by the goroutine the SDK created it on. An operation
invoked from another goroutine claims its ID in a scheduling-dependent order
and fails replay.

Reported:

- a durable operation in a `go` statement, either directly
  (`go durable.Wait(ctx, ...)`) or inside an immediately invoked function
  literal (`go func() { ... }()`);
- a durable operation in a function literal passed to
  `(*errgroup.Group).Go` or `TryGo`, including a group returned by
  `errgroup.WithContext`;
- a durable operation in a function literal passed to `(*sync.WaitGroup).Go`.

Fix: use `durable.Go`. It starts the goroutine and gives it a `Context` of
its own.

```go
// Reported.
go func() {
	durable.Step(ctx, "work", doWork)
}()

// Correct.
fut := durable.Go(ctx, "work", func(child durable.Context) (string, error) {
	return durable.Step(child, "work", doWork)
})
```

Known limitations:

- A function literal assigned to a variable and started later
  (`f := func() { ... }; go f()`), and a named function started with `go`,
  are not analysed.
- A `go` statement inside a `durable.Go` body is still reported. That
  goroutine does not own the child `Context` either.

### durablenestedop

Reports a durable operation created inside a step body. A step is one atomic
unit of work: its body runs once and its result is checkpointed. An
operation created inside the body is skipped when the step replays from its
checkpoint, so the operation log differs between runs.

Fix: group operations with `durable.RunInChildContext` or `durable.Go`.

Known limitations:

- A step body that calls a named helper which creates durable operations is
  not analysed.

### durablenondeterminism

Reports nondeterministic constructs in orchestration code. Orchestration
code is any function that has a `durable.Context` parameter, excluding step
bodies. Between checkpoints that code must be a pure function of the handler
input and of the checkpointed results it has consumed.

Reported:

- `time.Now`, `time.Since`, `time.Until`;
- the package-level functions of `math/rand`, `math/rand/v2` and
  `crypto/rand`, and the constructors of `github.com/google/uuid`;
- `Context.RequestID`, which changes on every invocation of one execution;
- a `range` over a map whose body creates a durable operation. Map
  iteration order is randomized, so the operations are created in a
  different order on replay.

Fix: move the call into a `durable.Step` so its value is checkpointed. Use
`durable.ExecutionStartTime` when a stable timestamp is enough. Sort map keys
into a slice before iterating.

Known limitations:

- Only functions that receive a `durable.Context` are analysed. A helper
  without a `Context` parameter that reads the clock is not reported, even
  when the handler calls it between operations.
- Methods on a `*rand.Rand` are not reported, because a seeded generator is
  deterministic.
- Nondeterminism reached through an interface or a function value is not
  reported.
- Reading a database or an environment variable between operations is also
  nondeterministic but is not reported.

### durablechildctx

Reports a durable operation inside a function that receives its own child
`Context` when the operation uses a `Context` captured from an enclosing
scope instead. The function passed to `RunInChildContext`,
`RunInChildContextAsync`, `Go`, `Map`, `Retry`, or a `Parallel` or `Select`
branch receives a child `Context`. An operation on the captured outer
`Context` claims an ID on the parent from a goroutine that does not own it.

```go
// Reported: uses ctx where child was intended.
durable.Go(ctx, "work", func(child durable.Context) (string, error) {
	return durable.Step(ctx, "work", doWork)
})
```

Known limitations:

- The `Context` argument must be a plain identifier. A `Context` stored in a
  struct field or returned by a function is not checked.
- A `Context` copied into a local variable inside the function is assumed to
  be the child.
- Only the function literal passed directly to the SDK is a callback. A
  closure that is assigned to a variable first, or a named function, is not
  analysed.

### durableclosure

Reports a write to a captured variable inside a step body or a child
context function. The SDK checkpoints the result of those functions and
returns the stored result on replay without running the function again.
Every side effect is then skipped. A variable declared outside the function
and written inside it therefore changes on the first run and keeps its old
value on replay, and the code after the operation sees different state on
each run.

Reported:

- an assignment (`x = v`, `x, err = f()`), a compound assignment (`x += v`),
  an increment or decrement (`x++`), or a `range` clause with `=`, whose
  target is a variable declared outside the innermost enclosing step body or
  child context function;
- the same write made from a helper closure, a `defer`, or a `go` statement
  inside that function.

Reading a captured variable is allowed.

Fix: return the value from the function and assign it outside. The return
value is checkpointed, so it is the same on every run.

```go
// Reported.
count := 0
_, err := durable.Step(ctx, "count", func(sc durable.StepContext) (durable.Void, error) {
	count = len(fetchItems())
	return durable.Void{}, nil
})

// Correct.
count, err := durable.Step(ctx, "count", func(sc durable.StepContext) (int, error) {
	return len(fetchItems()), nil
})
```

Known limitations:

- Only writes to a plain identifier are reported. A write through a pointer
  (`*p = v`), to a struct field (`s.f = v`), to a slice element or a map
  entry (`m[k] = v`) of a captured variable, and a method call that mutates
  a captured value, are not reported.
- A function that is assigned to a variable and then passed to the SDK is
  not analysed.

## Suppression

Suppress a diagnostic with a comment on the reported line, or on the line
directly above it. A comment that follows code on its line covers that line
only. A comment on a line of its own covers the next line only. The comment
starts with `//durable:ignore`, with no space after `//`. An optional
comma-separated rule list restricts it to those rules, and text after ` -- `
is a free-form reason. Two directives that cover the same line combine.

```go
//durable:ignore
durable.Step(ctx, "a", work)

durable.Step(ctx, "b", work) //durable:ignore durablegoroutine -- joined before the next checkpoint
```

For a multi-line call, put the directive on the line where the call starts.

Exclude a whole file from every rule, or from the listed rules, with a
comment anywhere in the file:

```go
//durable:ignore-file
//durable:ignore-file durablenondeterminism -- generated fixture
```

Under golangci-lint, `//nolint:durablelint` also applies.

## golangci-lint integration

golangci-lint v2 loads external analyzers as a module plugin. Add a
`.custom-gcl.yml` next to `.golangci.yml`:

```yaml
version: v2.12.2
plugins:
  - module: github.com/aws/aws-durable-execution-sdk-go/analysis
    import: github.com/aws/aws-durable-execution-sdk-go/analysis/gclplugin
    version: latest
```

Build the extended binary once with `golangci-lint custom`; it writes
`./custom-gcl`. Then enable the linter in `.golangci.yml`:

```yaml
version: "2"
linters:
  enable:
    - durablelint
  settings:
    custom:
      durablelint:
        type: module
        description: determinism rules for AWS Durable Execution handlers
```

Run `./custom-gcl run ./...` in place of `golangci-lint run ./...`. The
rules keep their own names in diagnostics, so `-durablenestedop=false` style
selection is not available under golangci-lint; use `//nolint:durablelint`
or the `//durable:ignore` directives instead.

## False-positive policy

The rules are written to be quiet enough for CI. Each one analyses lexical
structure and resolved types only, never data flow across function
boundaries, and the sections above list what each rule therefore misses. The
repository's own CI runs `durablelint` over the `examples` and `conformance`
modules and requires an empty report. The examples that demonstrate an
ownership error on purpose carry a `//durable:ignore` directive with a
reason.

## Development

```console
cd analysis
go test ./...
```

Each rule has a table-driven test under `durablelint/testdata/src/<rule>/`
using the `analysistest` conventions: every expected diagnostic is marked
with a `// want` comment on the reported line, and every other line must be
clean. The `testdata/src` tree carries signature-only stand-ins for the SDK,
`errgroup`, and `uuid` packages.
