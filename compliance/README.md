# Conformance Test Handlers

Per-requirement Go handlers for the
[AWS Durable Execution Conformance Tests](https://github.com/aws/aws-durable-execution-conformance-tests)
suite. Each handler implements one or more requirements from the 9 suites
(step, wait, invoke, child, callback, wait_for_callback, wait_for_condition,
map, parallel — 148 requirements total, numbered N-M).

Handlers live at `<operation>/<handler>/main.go`, one main package per handler,
compiled to `publish/<handler>/bootstrap` for the `provided.al2023` Lambda
runtime.

## Building

```sh
./build_examples.sh [operation...]
```

The `go.mod` uses a relative `replace` directive (`=> ../`) to resolve the
unpublished SDK from the parent directory. No external arguments or generated
workspace files needed.

## Running the Conformance Validator

1. Clone the public conformance test suite:

   ```sh
   git clone https://github.com/aws/aws-durable-execution-conformance-tests.git
   cd aws-durable-execution-conformance-tests
   ```

2. Run a specific suite (requires AWS credentials with Lambda + CloudFormation
   permissions):

   ```sh
   hatch run validate \
     --template /absolute/path/to/compliance/template_wait.yaml \
     --language go \
     --region us-west-2 \
     --suite wait
   ```

   The validator runs `sam build`, deploys a CloudFormation stack, invokes each
   handler, retrieves execution history, and asserts against the requirement
   YAMLs bundled in the conformance package.

## Requirement Numbering

Requirements follow the `N-M` scheme where N is the suite number (1=step,
2=wait, 3=child, 4=callback, 5=invoke, 6=wait_for_condition,
7=wait_for_callback, 8=parallel, 9=map) and M is the requirement index within
that suite.
