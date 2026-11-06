package telemetry

import "testing"

// TestParseFunctionARN_Partitions verifies that function metadata is
// extracted from ARNs in every partition, not only the aws partition.
func TestParseFunctionARN_Partitions(t *testing.T) {
	cases := []struct {
		arn          string
		region       string
		accountID    string
		functionName string
		qualifier    string
	}{
		{
			arn:          "arn:aws:lambda:us-east-1:123456789012:function:my-fn:5",
			region:       "us-east-1",
			accountID:    "123456789012",
			functionName: "my-fn",
			qualifier:    "5",
		},
		{
			arn:          "arn:aws-cn:lambda:cn-north-1:123456789012:function:my-fn",
			region:       "cn-north-1",
			accountID:    "123456789012",
			functionName: "my-fn",
		},
		{
			arn:          "arn:aws-us-gov:lambda:us-gov-west-1:123456789012:function:my-fn:PROD",
			region:       "us-gov-west-1",
			accountID:    "123456789012",
			functionName: "my-fn",
			qualifier:    "PROD",
		},
	}

	for _, tc := range cases {
		region, accountID, functionName, qualifier := parseFunctionARN(tc.arn)
		if region != tc.region || accountID != tc.accountID || functionName != tc.functionName || qualifier != tc.qualifier {
			t.Errorf("parseFunctionARN(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
				tc.arn, region, accountID, functionName, qualifier,
				tc.region, tc.accountID, tc.functionName, tc.qualifier)
		}
	}
}
