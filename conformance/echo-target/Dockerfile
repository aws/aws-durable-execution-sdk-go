# Multi-stage build for echo-target: a MINIMAL, DURABLE echo target
# Lambda function used as the real invoke target for the Go SDK
# conformance test harness's Invoke suite (see main.go's doc comment for
# the full rationale, including why this target is now durable - it
# previously was not).
#
# Uses this repo's own pkg/durable, hand-rolling the Lambda Runtime API
# loop directly against the standard library, exactly like every other
# DURABLE function in this repo (see conformance/Dockerfile's own sibling
# main.go doc comment for why: the durable-execution-aware invocation
# lifecycle needs direct control over the raw request/response cycle that
# a higher-level Runtime API library's own abstraction doesn't expose).
# GOPROXY=direct/GOSUMDB=off is required for this target's build to
# succeed, since proxy.golang.org is network-blocked inside the Finch VM
# this repo builds container images with (see
# docs/subagent-briefing.md), but direct git/HTTPS module fetches work
# fine - the same workaround every other Dockerfile in this repo already
# uses.
#
# Still deployed on the same public.ecr.aws/lambda/provided:al2023-arm64
# base as every other function in this repo (a bare custom-runtime image,
# not one of the language-specific go:1.x base images).

FROM golang:1.24 AS build
WORKDIR /src

ENV GOPROXY=direct
ENV GOSUMDB=off

COPY . /repo
WORKDIR /repo/conformance/echo-target

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /bootstrap .

FROM public.ecr.aws/lambda/provided:al2023-arm64

COPY --from=build /bootstrap /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap
CMD ["ignored-but-required-by-provided-runtime"]
