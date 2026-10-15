package durable

// Version is the version of the github.com/aws/aws-durable-execution-sdk-go
// module, without the leading v of its release tag. The release tool sets
// it in the commit that prepares a release, so between releases it names
// the most recent release. See RELEASING.md.
const Version = "1.0.0"

// userAgentKey names the SDK in the User-Agent header of every Lambda API
// call the default client makes. The header carries the token
// userAgentKey/Version, as aws-durable-execution-sdk-go/1.0.0, the format
// the durable execution SDKs for the other languages send.
const userAgentKey = "aws-durable-execution-sdk-go"
