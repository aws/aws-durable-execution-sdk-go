FROM golang:1.24 AS build
WORKDIR /src

# Same GOPROXY=direct GOSUMDB=off workaround every other AWS-SDK-backed
# example's Dockerfile in this repo already documents (see e.g.
# examples/completion-config-go/Dockerfile's own comment for the full
# rationale): proxy.golang.org is network-blocked inside the Finch VM
# this repo builds container images with, but direct git/HTTPS module
# fetches work fine, so GOPROXY=direct bypasses the proxy entirely.
ENV GOPROXY=direct
ENV GOSUMDB=off

COPY . /repo
WORKDIR /repo/conformance

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /bootstrap .

FROM public.ecr.aws/lambda/provided:al2023-arm64

COPY --from=build /bootstrap /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap
CMD ["ignored-but-required-by-provided-runtime"]
