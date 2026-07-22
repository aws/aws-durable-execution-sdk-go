# Multi-stage build: compile the Go binary in a standard Go image, then
# copy the static binary into the Lambda provided.al2023 base image.
#
# provided.al2023 expects a single executable at
# ${LAMBDA_TASK_ROOT}/bootstrap that implements the Lambda Runtime API
# directly - exactly what examples/simple-step-go/main.go does by hand
# (see that file's doc comment for why it's hand-rolled rather than using
# aws-lambda-go).
#
# This example now uses pkg/durable/awssdk (the real AWS SDK for Go v2
# Lambda client) as its checkpoint.Client, so - unlike an earlier version
# of this file - no AWS CLI installation is needed in the final image;
# credentials are resolved from the Lambda execution role via the SDK's
# own standard credential chain, exactly like every other AWS SDK-backed
# Lambda function.

FROM golang:1.24 AS build
WORKDIR /src

# Same GOPROXY=direct GOSUMDB=off workaround docs/subagent-briefing.md
# documents for this repo's development environment: proxy.golang.org is
# network-blocked here (confirmed - a plain `go build` inside this build
# stage times out resolving it), but direct git/HTTPS access to
# github.com works fine, so GOPROXY=direct bypasses the module proxy
# entirely rather than going through it. Added here (this Dockerfile
# didn't have it before, unlike every other awssdk-based example's
# Dockerfile - e.g. examples/completion-config-go/Dockerfile - unearthed
# while rebuilding this image to add the handler-error scenario: a fresh
# `finch build` in this session hit the identical proxy.golang.org
# timeout every other example's Dockerfile already works around, so this
# was a real, if latent, gap in this one Dockerfile specifically) - see
# docs/remaining-work.md §10 task 22's writeup on why this is a property
# of THIS sandbox, not a real project requirement.
ENV GOPROXY=direct
ENV GOSUMDB=off

COPY . /repo
WORKDIR /repo/examples/simple-step-go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /bootstrap .

FROM public.ecr.aws/lambda/provided:al2023-arm64

COPY --from=build /bootstrap /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap
CMD ["ignored-but-required-by-provided-runtime"]
