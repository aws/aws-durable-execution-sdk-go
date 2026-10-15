package durable

// Version is the semantic version of the aws-durable-execution-sdk-go module.
const Version = "0.1.0"

// userAgentKey names the SDK in the User-Agent header of every Lambda API
// call the default client makes. The header carries the token
// userAgentKey/Version, as aws-durable-execution-sdk-go/1.0.0, the format
// the durable execution SDKs for the other languages send.
const userAgentKey = "aws-durable-execution-sdk-go"
