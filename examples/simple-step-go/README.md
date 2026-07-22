# simple-step-go

A minimal AWS Lambda durable function using the AWS Durable Execution SDK
for Go, deployed as a container image.

This example exists to answer two questions empirically, by deploying to
a real AWS account and observing actual behavior rather than inferring it
from documentation (see `docs/checkpoint-replay-design.md`'s "Real-world
verification" section for the full account of what was found):

1. Does a Go binary work at all as a Lambda durable function container
   image? (Durable functions are documented as available for Node.js,
   Python, and Java - not Go. See `docs/go-sdk-design.md`.)
2. What does the real `DurableExecutionInvocationInput` wire payload
   actually look like?

Both were answered: yes, Go works; and the payload shape is now reflected
in `pkg/durable/types/wire.go`, corrected in several ways from what the
public API documentation alone would suggest.

## What this example does

`main.go` implements the Lambda Runtime API directly against the standard
library (no `aws-lambda-go` dependency - see the file's doc comment for
why), decodes the invocation into a `types.DurableExecutionInvocationInput`,
and runs a single `operations.Step` call via this SDK's
`durable.WithDurableExecution`.

## Checkpoint client caveat

This example uses `pkg/durable/awscli.Client`, which shells out to the AWS
CLI rather than using the AWS SDK for Go v2 directly. This is a stopgap
(see that package's doc comment) required because the development
environment this was built in has no outbound network access to the Go
module proxy. **Do not use this in production** - it requires the `aws`
CLI installed in the container image (see the `Dockerfile`) and adds a
process-spawn per checkpoint call.

## Building and deploying

```sh
finch build --platform linux/arm64 -t simple-step-go:latest -f examples/simple-step-go/Dockerfile .
finch tag simple-step-go:latest <account>.dkr.ecr.<region>.amazonaws.com/<repo>:latest
finch push <account>.dkr.ecr.<region>.amazonaws.com/<repo>:latest

aws lambda create-function \
  --function-name simple-step-go-example \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/<repo>:latest \
  --role <execution-role-arn> \
  --architectures arm64 \
  --durable-config '{"RetentionPeriodInDays":1,"ExecutionTimeout":120}'

aws lambda publish-version --function-name simple-step-go-example

# Durable functions require a qualified ARN (a published version or
# alias) - $LATEST is rejected.
aws lambda invoke \
  --function-name simple-step-go-example:1 \
  --invocation-type Event \
  --payload '{"orderId":"abc","message":"hello"}' \
  response.json
```
