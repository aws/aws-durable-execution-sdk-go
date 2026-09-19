module github.com/aws/aws-durable-execution-sdk-go/examples

go 1.24

require (
	github.com/aws/aws-durable-execution-sdk-go v0.1.0
	github.com/aws/aws-lambda-go v1.54.0
	github.com/aws/aws-sdk-go-v2 v1.42.1
	github.com/aws/aws-sdk-go-v2/config v1.32.30
	github.com/aws/aws-sdk-go-v2/service/lambda v1.99.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.14 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.29 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.31 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.79.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.44.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.47.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/firehose v1.45.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.30 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.31 // indirect
	github.com/aws/aws-sdk-go-v2/service/rdsdata v1.25.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/redshiftdata v1.37.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.105.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.45.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.32.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.37.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.44.1 // indirect
	github.com/aws/smithy-go v1.27.3 // indirect
)

require github.com/aws/aws-durable-execution-sdk-go/insight v0.0.0

replace github.com/aws/aws-durable-execution-sdk-go => ../

replace github.com/aws/aws-durable-execution-sdk-go/insight => ../insight
