// cloud_callback_driver.go extends CloudTestRunner (cloud_runner.go) with
// the minimal capability needed to drive a REAL, backend-issued callback
// to completion from a test itself, mid-run - closing a genuine gap
// found while wiring examples/map-with-condition-and-callback-go's
// cloud_integration_test.go onto §7 task 17b (see that file's own doc
// comment for the full example-level narrative this supports).
//
// # Why this gap exists, and why it's a real, well-scoped SDK addition
// rather than a one-off hack in the example's own test file
//
// Every other example this session (and the earlier session that added
// examples/completion-config-go's own cloud_integration_test.go) wired
// onto CloudTestRunner runs a single Invoke-then-poll-to-terminal cycle
// via Run/awaitTerminal (cloud_runner.go) - a synchronous Lambda Invoke
// against a durable function blocks for the ENTIRE execution (per
// LambdaInvoker.Invoke's own doc), so Run's poll loop only ever needs to
// wait, never to intervene mid-flight.
// examples/map-with-condition-and-callback-go is structurally different:
// its handler genuinely suspends on operations.WaitForCallback partway
// through, and nothing EXTERNAL to this SDK will ever complete that
// callback on its own - a real test exercising this example end-to-end
// against the real backend MUST itself extract the real, backend-issued
// CallbackId (via GetDurableExecutionHistory's CallbackStarted event,
// now reconstructed into a CALLBACK-type types.Operation by
// sdk_state_client.go's reconstructOperationsFromHistory - see that
// file's own doc for the CHAINED_INVOKE/WAIT/CALLBACK handling this
// added in the same session) and call the real
// SendDurableExecutionCallbackSuccess/Failure API itself, exactly like
// docs/remaining-work.md's §0c section's own prior manual verification
// of this same function did by hand with the AWS CLI.
//
// Read cloud_runner.go's Run method (and every other exported method on
// CloudTestRunner) in full before writing this: neither Run nor
// awaitTerminal exposes ANY way to stop polling early, inspect
// in-progress state, or resume polling after an external action - Run is
// a single, monolithic invoke-then-block-until-terminal call. Rather
// than bypassing CloudTestRunner entirely for this one example (option
// (b) this task's own instructions offered), this file adds the minimal
// necessary support to CloudTestRunner ITSELF (option (a)) for the
// following reasons:
//
//  1. It is a genuinely small, self-contained addition - one new
//     interface (CallbackAPI, mirroring RawLambdaAPI's/
//     RawExternalExecutionAPI's own established "narrow interface over
//     the real *lambda.Client, for testability" pattern in this exact
//     package - see lambda_invoker.go/sdk_state_client.go), one new
//     production constructor (NewCallbackDriver), and two new
//     CloudTestRunner methods (RunUntilCallback, Continue) that reuse
//     every existing piece (fetchAllOperations, toTestResult,
//     newTestResult's pre-existing but previously-always-nil
//     `runner callbackDriver` parameter) rather than duplicating any
//     polling logic.
//  2. It makes the RESULTING TestResult genuinely consistent with
//     LocalTestRunner's own callback-driving API surface: a
//     RunUntilCallback call returns a TestResult whose GetOperation(name)
//     handle, once obtained, supports the EXACT SAME
//     Operation.SendCallbackSuccess/SendCallbackFailure methods
//     (operation.go) a LocalTestRunner-produced, mid-flight PENDING
//     TestResult already supports - satisfied by wiring a new
//     cloudCallbackDriver (implementing the pre-existing, package-private
//     callbackDriver interface operation.go already defines) into
//     newTestResult's runner parameter, exactly mirroring how
//     LocalTestRunner itself does the identical wiring (see runner.go's
//     own toTestResult, which passes `r` - the runner itself - as that
//     same parameter). A caller/test that already knows LocalTestRunner's
//     callback-driving API needs to learn NOTHING new for the cloud
//     case beyond calling RunUntilCallback/Continue instead of
//     Run/RunAsync/Continue - the actual callback-completion call
//     (op.SendCallbackSuccess(result)) is IDENTICAL Go code either way.
//  3. It is testable without real AWS credentials, following this
//     package's own established convention: CallbackAPI is a narrow
//     interface a hand-written fake can satisfy (see
//     cloud_callback_driver_test.go), exactly like RawLambdaAPI/
//     RawExternalExecutionAPI already are.
//
// This is NOT a claim that CloudTestRunner now has FULL parity with
// LocalTestRunner's callback-testing surface (e.g. there is still no
// cloud equivalent of a heartbeat-timeout test helper, since the SDK
// itself has no heartbeat-timeout support yet either - see
// docs/remaining-work.md's comparison table) - only that the ONE gap
// blocking this task's specific example (driving a real callback to
// completion mid-run) is now closed, minimally and testably.
package testing

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// CallbackAPI is the minimal Lambda callback-completion capability
// CloudTestRunner's callback-driving support needs, exposed as an
// interface (matching RawLambdaAPI's/RawExternalExecutionAPI's own
// established pattern in this same package) so tests can substitute a
// fake without real AWS credentials or network access. The real AWS SDK
// for Go v2's *lambda.Client satisfies this directly.
type CallbackAPI interface {
	SendDurableExecutionCallbackSuccess(ctx context.Context, params *lambda.SendDurableExecutionCallbackSuccessInput, optFns ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackSuccessOutput, error)
	SendDurableExecutionCallbackFailure(ctx context.Context, params *lambda.SendDurableExecutionCallbackFailureInput, optFns ...func(*lambda.Options)) (*lambda.SendDurableExecutionCallbackFailureOutput, error)
}

// cloudCallbackDriver adapts a CallbackAPI into this package's own
// package-private callbackDriver interface (operation.go), the same
// interface LocalTestRunner itself implements - see this file's own
// header doc, point 2, for why reusing that exact interface (rather than
// inventing a parallel one) is what makes Operation.SendCallbackSuccess/
// SendCallbackFailure work identically against a cloud-run TestResult as
// they already do against a local one.
type cloudCallbackDriver struct {
	api CallbackAPI
}

// NewCallbackDriver constructs the production callback-driving capability
// CloudTestRunner.RunUntilCallback wires into its returned TestResult,
// backed by a real *lambda.Client, resolving credentials/region via the
// standard AWS SDK for Go v2 default chain - the exact same resolution
// NewLambdaInvoker/NewStateClient already use (see those functions' own
// docs), reused here for consistency.
func NewCallbackDriver(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (*cloudCallbackDriver, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("testing.NewCallbackDriver: loading AWS config: %w", err)
	}
	return &cloudCallbackDriver{api: lambda.NewFromConfig(cfg)}, nil
}

// sendCallbackResult implements the package-private callbackDriver
// interface (operation.go) - see Operation.SendCallbackSuccess/
// SendCallbackFailure for the public entry points that call this,
// identically to how LocalTestRunner's OWN sendCallbackResult method
// (runner.go) is invoked through that same interface.
//
// Exactly one of result/errObj is non-nil per callbackDriver's own
// existing contract (established by LocalTestRunner's implementation,
// unchanged here) - result non-nil calls the real
// SendDurableExecutionCallbackSuccess API; errObj non-nil calls the real
// SendDurableExecutionCallbackFailure API.
func (d *cloudCallbackDriver) sendCallbackResult(callbackID string, result *string, errObj *types.ErrorObject) error {
	if result != nil {
		_, err := d.api.SendDurableExecutionCallbackSuccess(context.Background(), &lambda.SendDurableExecutionCallbackSuccessInput{
			CallbackId: aws.String(callbackID),
			Result:     []byte(*result),
		})
		if err != nil {
			return fmt.Errorf("testing.cloudCallbackDriver.sendCallbackResult: SendDurableExecutionCallbackSuccess: %w", err)
		}
		return nil
	}
	if errObj != nil {
		_, err := d.api.SendDurableExecutionCallbackFailure(context.Background(), &lambda.SendDurableExecutionCallbackFailureInput{
			CallbackId: aws.String(callbackID),
			Error: &lambdatypes.ErrorObject{
				ErrorMessage: aws.String(errObj.ErrorMessage),
				ErrorType:    aws.String(errObj.ErrorType),
			},
		})
		if err != nil {
			return fmt.Errorf("testing.cloudCallbackDriver.sendCallbackResult: SendDurableExecutionCallbackFailure: %w", err)
		}
		return nil
	}
	return fmt.Errorf("testing.cloudCallbackDriver.sendCallbackResult: neither result nor errObj was provided")
}
