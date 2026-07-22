# Multi-stage build: compile the Go binary in a standard Go image, then
# copy the static binary into the Lambda provided.al2023 base image.
#
# provided.al2023 expects a single executable at
# ${LAMBDA_TASK_ROOT}/bootstrap that implements the Lambda Runtime API
# directly - exactly what examples/map-parallel-go/main.go does by hand
# (see that file's doc comment for why it's hand-rolled rather than using
# aws-lambda-go).
#
# This example uses pkg/durable/awssdk (the real AWS SDK for Go v2 Lambda
# client) as its checkpoint.Client, so no AWS CLI installation is needed
# in the final image; credentials are resolved from the Lambda execution
# role via the SDK's own standard credential chain - mirroring
# examples/simple-step-go/Dockerfile and examples/completion-config-go/Dockerfile
# verbatim, with only the module path adjusted.

FROM golang:1.24 AS build
WORKDIR /src

# Same GOPROXY=direct GOSUMDB=off workaround documented in
# examples/completion-config-go/Dockerfile: proxy.golang.org is
# network-blocked inside this build environment, but direct git/HTTPS
# access to github.com works fine, so GOPROXY=direct bypasses the module
# proxy entirely. Not needed for a real GitHub Actions runner or a
# production build environment with normal internet access.
ENV GOPROXY=direct
ENV GOSUMDB=off

COPY . /repo
WORKDIR /repo/examples/map-parallel-go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /bootstrap .

FROM public.ecr.aws/lambda/provided:al2023-arm64

COPY --from=build /bootstrap /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap
CMD ["ignored-but-required-by-provided-runtime"]
