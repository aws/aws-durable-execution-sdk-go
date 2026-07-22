# Multi-stage build for inventory-check-target: a MINIMAL, PLAIN
# (non-durable) Lambda function used as the real invoke target for
# examples/chained-invoke-go's RetryingInventoryCheckHandler real-cloud
# verification (see main.go's doc comment for the full rationale).
#
# Unlike every other Dockerfile in this repo, this target has NO
# dependency on this repo's own SDK (pkg/durable/*) at all - it is a
# plain, stdlib-only Lambda function, so its build stage does not need
# the GOPROXY=direct/GOSUMDB=off workaround the SDK-dependent examples
# need (no module fetch of aws-sdk-go-v2 or this repo's own module
# happens here) - but the lines are kept anyway, harmlessly, for
# consistency with every other Dockerfile in this repo and in case a
# future change to this target adds a real dependency.

FROM golang:1.24 AS build
WORKDIR /src

ENV GOPROXY=direct
ENV GOSUMDB=off

COPY . /repo
WORKDIR /repo/examples/chained-invoke-go/inventory-check-target

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /bootstrap .

FROM public.ecr.aws/lambda/provided:al2023-arm64

COPY --from=build /bootstrap /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap
CMD ["ignored-but-required-by-provided-runtime"]
