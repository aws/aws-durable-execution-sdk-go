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

You need Go, the
[AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html),
Python, AWS credentials for an account you can deploy to, and the ARN of a
Lambda execution role in that account. The repository ships a one-time
CloudFormation template for the role; deploy it once with IAM-capable
credentials, and pass its `RoleArn` output as `ExecutionRoleArn` below. The
test stacks themselves never create IAM resources.

```sh
aws cloudformation deploy \
    --template-file ../scripts/test-execution-role.yaml \
    --stack-name durable-sdk-test-execution-role \
    --capabilities CAPABILITY_IAM
```

Install the runner from `main`, the same as CI does. The released version does
not accept `--parameter-overrides`.

```sh
pip install "git+https://github.com/aws/aws-durable-execution-conformance-tests.git@main#subdirectory=packages/aws-durable-execution-conformance-tests"
```

Build the handlers, then run one suite. Each suite deploys its own
CloudFormation stack, runs every handler, and deletes the stack when done. The
runner writes its history and report files, and a `build/` scratch directory,
into the current directory, so run it from somewhere outside the repository.

```sh
./build_examples.sh wait        # build one suite; omit the argument to build all

mkdir -p /tmp/conformance-run && cd /tmp/conformance-run
python -m aws_durable_execution_conformance_tests.app \
    --template /path/to/conformance/template_wait.yaml \
    --language go \
    --suite wait \
    --name conf-go-wait-local \
    --region us-west-2 \
    --parameter-overrides ExecutionRoleArn=arn:aws:iam::111122223333:role/your-lambda-execution-role \
    --history-dir history-wait \
    --report console junit \
    --report-file report-wait
```

`--name` becomes part of the CloudFormation stack name. Use a distinct name for
every run: the runner deletes its stack without waiting, so a second run with
the same name can fail on `CreateStack` while the first delete is still in
progress. The nine suites are independent and can run concurrently, each from
its own directory; a full run of all suites takes about two minutes after the
handlers are built.

## Requirement Numbering

Requirements follow the `N-M` scheme where N is the suite number (1=step,
2=wait, 3=child, 4=callback, 5=invoke, 6=wait_for_condition,
7=wait_for_callback, 8=parallel, 9=map) and M is the requirement index within
that suite.
