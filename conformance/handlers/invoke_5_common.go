// Package handlers' invoke_5_common.go holds the one thing every
// Invoke5N* handler (invoke_5_1.go through invoke_5_16.go) needs in
// common: the real, deployed echo-target function's ARN.
//
// # Why hardcoded, not templated
//
// conformance/echo-target is deployed as its own AWS::Lambda::Function
// resource in conformance/template.yaml (EchoTarget), which SAM/
// CloudFormation only resolves to a real ARN at deploy time. Every
// Invoke5N* function needs this ARN baked into ITS OWN container image
// (this same, single, shared conformance/ image every other requirement
// function also uses - see registry.go's doc comment) as a Go constant,
// not as a template-injected environment variable, because:
//
//   - This template is NOT tied to one fixed AWS account/region the way
//     a real user's hand-deployed function might be (see
//     examples/chained-invoke-go/handler.go's own InventoryCheckFunctionARN
//     for exactly this same precedent: a hardcoded ARN with a real
//     account ID/region baked in, not a templated one, chosen there for
//     the identical reason: "note that one might be hardcoded
//     post-deployment rather than templated" per this session's own
//     briefing).
//   - EchoTarget's LogicalId is stable across every `sam deploy` of this
//     template (it is not re-created per deploy, only updated in place),
//     so its ARN is deterministic and stable for the lifetime of this
//     stack, and every Invoke5N* function is a SEPARATE resource in the
//     SAME template as EchoTarget - so Fn::GetAtt EchoTarget.Arn (set
//     directly on each Invoke5N* function's own Environment.Variables,
//     not hardcoded in Go source) is the cleaner, fully-templated
//     mechanism available here. See template.yaml's own EchoTarget/
//     Invoke5N* resources: each Invoke5N* function reads the real,
//     deployed ARN from its own CONFORMANCE_ECHO_TARGET_ARN environment
//     variable (set via Fn::GetAtt EchoTarget.Arn), not from a Go
//     constant at all - this file exists only to centralize the ONE
//     environment-variable-read helper every invoke_5_*.go file calls,
//     so that boilerplate isn't repeated 16 times.
package handlers

import (
	"fmt"
	"os"
)

// echoTargetFunctionARN reads the real, deployed echo-target function's
// ARN from CONFORMANCE_ECHO_TARGET_ARN - set on every Invoke5N* function
// resource in template.yaml via Fn::GetAtt EchoTarget.Arn, so this
// container image never needs a hardcoded, account/region-specific ARN
// baked into its own binary (contrast
// examples/chained-invoke-go/handler.go's InventoryCheckFunctionARN,
// which IS a hardcoded constant - appropriate there because that example
// is hand-deployed outside of any SAM template, per that file's own doc
// comment, but not the cleanest choice for THIS fully-SAM-templated
// conformance harness, which can just as easily pass its sibling
// resource's real ARN through at deploy time instead).
//
// Panics if unset - a real configuration error in template.yaml (every
// Invoke5N* function resource must set this), not a recoverable runtime
// condition, exactly like registry.go's own Register panicking on a
// duplicate id.
func echoTargetFunctionARN() string {
	arn := os.Getenv("CONFORMANCE_ECHO_TARGET_ARN")
	if arn == "" {
		panic(fmt.Sprintf("handlers: CONFORMANCE_ECHO_TARGET_ARN is not set - every Invoke5N* function must set this via template.yaml's Fn::GetAtt EchoTarget.Arn"))
	}
	return arn
}
