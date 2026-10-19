// Command release prepares and checks the releases of the Go modules in
// this repository. RELEASING.md describes the release procedure.
//
// Run it from the repository root:
//
//	go run -C .github/scripts/release . <command> [arguments]
//
// The commands are:
//
//	prepare vX.Y.Z
//	    Set durable.Version, and the SDK version that each nested module
//	    requires, for the SDK release vX.Y.Z. Commit the changes and merge
//	    them to main before creating the release. prepare refuses a
//	    version below durable.Version, such as v1.1.0 or v1.2.0-rc.1 after
//	    1.2.0.
//
//	check [-branch ref] [tag]
//	    Check that each nested module requires the SDK version that
//	    durable.Version names. With a tag, also check that HEAD is a
//	    commit that the release can tag: the working tree has no changes,
//	    HEAD is on the branch, origin/main by default, and the versions
//	    match the tag. For a telemetry tag, this also builds telemetry from
//	    this checkout against the released SDK version that telemetry
//	    requires, which the go command fetches as it is configured to.
//	    check changes nothing.
//
//	consumer [dir...]
//	    Build a new module that imports every package of each published
//	    module, taking the modules from this checkout. The directories
//	    default to all published modules: . telemetry analysis.
//
//	verify [-proxy url] [-branch ref] [-timeout d] [-interval d] tag
//	    Check a published release. The Release workflow runs this command
//	    when a GitHub Release is published.
//
// Versions and tags carry the leading v that the go command requires, as
// in v1.2.0, telemetry/v0.1.0, and analysis/v0.1.0. durable.Version holds the
// SDK version without it, as 1.2.0. That is the form the default client
// sends in the User-Agent header.
package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	// versionFile declares durable.Version. The path is relative to the
	// repository root.
	versionFile = "durable/version.go"

	// defaultProxy is the module proxy that the go command uses by default.
	defaultProxy = "https://proxy.golang.org"

	// defaultBranch is the remote-tracking branch that releases come from.
	defaultBranch = "origin/main"
)

// publishedModules are the modules that users add with go get, by
// directory relative to the repository root. The directory "." holds the
// SDK. A nested module's tags carry its directory as a prefix, as in
// telemetry/v0.1.0.
var publishedModules = []string{".", "telemetry", "analysis"}

// internalModules are used only inside the repository, so they are never
// released. prepare still sets the SDK version they require. examples
// requires both the SDK and telemetry, so its go.mod must list the SDK
// version that telemetry requires.
var internalModules = []string{"conformance", "examples"}

// versionPattern matches the versions this repository releases. The
// numbers have no leading zeros, because the go command ignores a tag such
// as v1.02.0. releaseVersionPattern, in durable/version_test.go, is the
// same pattern without the leading v. Change the two patterns together.
var versionPattern = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|rc)(\.(0|[1-9][0-9]*))?)?$`)

const versionForm = "vX.Y.Z, optionally followed by -alpha, -beta, or -rc and an optional .N"

// versionLine matches the line of versionFile that declares Version.
var versionLine = regexp.MustCompile(`(?m)^const Version = "([^"]*)"$`)

const usage = `usage: go run -C .github/scripts/release . [-repo dir] <command> [arguments]

commands:
  prepare vX.Y.Z      set the SDK version for an SDK release
  check [tag]         check the versions, and that HEAD is ready for the tag
  consumer [dir...]   build a new module against each published module
  verify tag          check a published release

The package documentation and RELEASING.md describe each command.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run runs the command line args and returns the exit status.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("release", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() { _, _ = fmt.Fprint(stderr, usage) }
	repo := flags.String("repo", "", "repository root (default: the root of the Git checkout that holds the current directory)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	a := &app{env: os.Environ(), out: stdout}
	dir := *repo
	if dir == "" {
		root, err := a.command(a.env, ".", "git", "rev-parse", "--show-toplevel")
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "release: cannot find the repository root. Run the tool inside the checkout, or pass -repo.\n%v\n", err)
			return 1
		}
		dir = strings.TrimSpace(root)
	}
	// The tool runs commands in the repository, and it passes them paths
	// inside the repository. A relative path would resolve against a
	// different directory in each of those two uses.
	dir, err := filepath.Abs(dir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "release: %v\n", err)
		return 1
	}
	a.repo = dir
	err = a.run(flags.Args())
	var u usageError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &u):
		_, _ = fmt.Fprintf(stderr, "release: %v\n\n%s", err, usage)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "release: %v\n", err)
		return 1
	}
}

// usageError reports a command line that the tool cannot run.
type usageError string

func (e usageError) Error() string { return string(e) }

// app runs the commands against one checkout.
type app struct {
	repo string    // root of the checkout
	env  []string  // environment of the go and git commands
	out  io.Writer // where the commands report what they did
}

func (a *app) run(args []string) error {
	if len(args) == 0 {
		return usageError("no command given")
	}
	switch args[0] {
	case "prepare":
		return a.prepare(args[1:])
	case "check":
		return a.check(args[1:])
	case "consumer":
		return a.consumer(args[1:])
	case "verify":
		return a.verify(args[1:])
	}
	return usageError(fmt.Sprintf("unknown command %q", args[0]))
}

func (a *app) logf(format string, args ...any) {
	_, _ = fmt.Fprintf(a.out, format+"\n", args...)
}

// A release is one version of one published module.
type release struct {
	dir     string // directory of the module; "." for the SDK
	version string // the version with its leading v, as v1.2.0
}

// tag returns the Git tag of the release, as v1.2.0 or telemetry/v0.1.0.
func (r release) tag() string {
	if r.dir == "." {
		return r.version
	}
	return r.dir + "/" + r.version
}

// parseTag parses the tag of a release. It returns an error unless the
// repository releases that module at that version.
func parseTag(tag string) (release, error) {
	r := release{dir: ".", version: tag}
	if i := strings.LastIndex(tag, "/"); i >= 0 {
		r.dir, r.version = tag[:i], tag[i+1:]
		if r.dir == "." || !slices.Contains(publishedModules, r.dir) {
			return release{}, fmt.Errorf("tag %s: %q is not a published module. The SDK's tags are vX.Y.Z, and the nested modules' tags are telemetry/vX.Y.Z and analysis/vX.Y.Z", tag, r.dir)
		}
	}
	major, err := parseVersion(r.version)
	if err == nil {
		err = majorRule(r.dir, major)
	}
	if err != nil {
		return release{}, fmt.Errorf("tag %s: %w", tag, err)
	}
	return r, nil
}

// parseVersion returns the major version number of v. It returns an error
// unless v matches versionPattern.
func parseVersion(v string) (int, error) {
	m := versionPattern.FindStringSubmatch(v)
	if m == nil {
		return 0, fmt.Errorf("version %q does not have the form %s", v, versionForm)
	}
	return strconv.Atoi(m[1])
}

// compareVersions returns -1, 0, or +1 as the version a is lower than,
// equal to, or higher than the version b, by the precedence rules of
// semantic versioning. Both versions must match versionPattern.
func compareVersions(a, b string) int {
	// The submatches are the major, minor, and patch numbers, then the
	// whole pre-release, its name, a dot, and its number.
	x, y := versionPattern.FindStringSubmatch(a), versionPattern.FindStringSubmatch(b)
	for i := 1; i <= 3; i++ {
		if c := compareNumbers(x[i], y[i]); c != 0 {
			return c
		}
	}
	// A version with a pre-release is lower than the same version without.
	switch {
	case x[4] == y[4]:
		return 0
	case x[4] == "":
		return 1
	case y[4] == "":
		return -1
	}
	// The names alpha, beta, and rc compare in ASCII order. A pre-release
	// with a number is higher than the same pre-release without one.
	if c := strings.Compare(x[5], y[5]); c != 0 {
		return c
	}
	switch {
	case x[7] == "":
		return -1
	case y[7] == "":
		return 1
	}
	return compareNumbers(x[7], y[7])
}

// compareNumbers compares two decimal numbers that have no leading zeros.
// The longer number is the higher one, and numbers of the same length
// compare digit by digit, so no number can overflow.
func compareNumbers(a, b string) int {
	if c := cmp.Compare(len(a), len(b)); c != 0 {
		return c
	}
	return strings.Compare(a, b)
}

// majorRule returns an error unless the repository releases the module in
// dir at major version major. The SDK has v0 and v1 versions. A v2 SDK
// would need a new module path, which this tool does not set up. The
// nested modules stay below v1.
func majorRule(dir string, major int) error {
	switch {
	case dir == "." && major > 1:
		return errors.New("a v2 or later release of the SDK needs a new module path, which this tool does not set up")
	case dir != "." && major > 0:
		return fmt.Errorf("%s stays below v1", dir)
	}
	return nil
}

// modulePath returns the path of the module in dir, given the SDK's
// module path.
func modulePath(sdk, dir string) string {
	if dir == "." {
		return sdk
	}
	return sdk + "/" + dir
}

func nestedModules() []string {
	return append(slices.Clone(publishedModules[1:]), internalModules...)
}

// versions holds the versions that a checkout names.
type versions struct {
	sdk      string            // module path of the SDK
	version  string            // durable.Version with a leading v added
	pins     map[string]string // the SDK version that each nested module requires, by directory
	pinned   []string          // the nested modules that require the SDK, in a fixed order
	unpinned []string          // the nested modules that do not require the SDK
}

func (a *app) readVersions() (*versions, error) {
	root, err := a.readModFile(".")
	if err != nil {
		return nil, err
	}
	v := &versions{sdk: root.Module.Path, pins: map[string]string{}}
	data, err := os.ReadFile(filepath.Join(a.repo, versionFile))
	if err != nil {
		return nil, err
	}
	m := versionLine.FindAllSubmatch(data, -1)
	if len(m) != 1 {
		return nil, fmt.Errorf(`%s must declare Version on exactly one line, as: const Version = "1.2.0"`, versionFile)
	}
	v.version = "v" + string(m[0][1])
	for _, dir := range nestedModules() {
		mf, err := a.readModFile(dir)
		if err != nil {
			return nil, err
		}
		if want := modulePath(v.sdk, dir); slices.Contains(publishedModules, dir) && mf.Module.Path != want {
			return nil, fmt.Errorf("%s/go.mod declares module %s, but the tags %s/vX.Y.Z belong to module %s", dir, mf.Module.Path, dir, want)
		}
		i := slices.IndexFunc(mf.Require, func(r requirement) bool { return r.Path == v.sdk })
		if i < 0 {
			v.unpinned = append(v.unpinned, dir)
			continue
		}
		v.pins[dir] = mf.Require[i].Version
		v.pinned = append(v.pinned, dir)
	}
	return v, nil
}

// check returns an error unless durable.Version is a version that the SDK
// can be released at and every nested module requires that version.
func (v *versions) check() error {
	plain := strings.TrimPrefix(v.version, "v")
	if strings.HasPrefix(plain, "v") {
		return fmt.Errorf("durable.Version is %q. Write it without the leading v, as %q", plain, strings.TrimPrefix(plain, "v"))
	}
	major, err := parseVersion(v.version)
	if err != nil {
		return fmt.Errorf("durable.Version is %q, but it must have the form %s, without the leading v", plain, versionForm)
	}
	if err := majorRule(".", major); err != nil {
		return fmt.Errorf("durable.Version is %q: %w", plain, err)
	}
	var wrong []string
	for _, dir := range v.pinned {
		if v.pins[dir] != v.version {
			wrong = append(wrong, fmt.Sprintf("%s/go.mod requires %s", dir, v.pins[dir]))
		}
	}
	if len(wrong) > 0 {
		return fmt.Errorf("durable.Version is %s, so each nested module must require %s %s. But %s. Run prepare %s to set them", plain, v.sdk, v.version, strings.Join(wrong, ", and "), v.version)
	}
	return nil
}

// prepare sets the version of an SDK release in the checkout.
func (a *app) prepare(args []string) error {
	if len(args) != 1 {
		return usageError("prepare takes the SDK version, as in: prepare v1.2.0")
	}
	r, err := parseTag(args[0])
	if err != nil {
		return err
	}
	if r.dir != "." {
		return fmt.Errorf("%s: only an SDK release needs preparing. Create the GitHub Release %s on main, as RELEASING.md describes", r.tag(), r.tag())
	}
	v, err := a.readVersions()
	if err != nil {
		return err
	}
	// A release never lowers the version. A lower version, such as v1.1.0
	// or v1.2.0-rc.1 after 1.2.0, would set durable.Version and the SDK
	// requirements back, and its tag could never be withdrawn. So prepare
	// refuses it. A durable.Version that does not parse is replaced without
	// the comparison.
	if _, err := parseVersion(v.version); err == nil && compareVersions(r.version, v.version) < 0 {
		return fmt.Errorf("%s is lower than durable.Version, %s. A release cannot lower the version", r.version, strings.TrimPrefix(v.version, "v"))
	}

	plain := strings.TrimPrefix(r.version, "v")
	if v.version == r.version {
		a.logf("%s: Version is already %q", versionFile, plain)
	} else {
		file := filepath.Join(a.repo, versionFile)
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		data = versionLine.ReplaceAllLiteral(data, []byte(`const Version = "`+plain+`"`))
		if err := os.WriteFile(file, data, 0o644); err != nil {
			return err
		}
		a.logf("%s: Version = %q (was %q)", versionFile, plain, strings.TrimPrefix(v.version, "v"))
	}

	for _, dir := range v.pinned {
		if v.pins[dir] == r.version {
			a.logf("%s/go.mod: already requires %s %s", dir, v.sdk, r.version)
			continue
		}
		if _, err := a.command(a.goEnv(), a.repo, "go", "mod", "edit", "-require="+v.sdk+"@"+r.version, filepath.Join(a.repo, dir, "go.mod")); err != nil {
			return err
		}
		a.logf("%s/go.mod: require %s %s (was %s)", dir, v.sdk, r.version, v.pins[dir])
	}
	for _, dir := range v.unpinned {
		a.logf("%s/go.mod: does not require the SDK", dir)
	}
	a.logf("\nCommit the changes as \"chore(release): %s\" and merge them to main.", r.version)
	a.logf("Then create the GitHub Release %s on main, as RELEASING.md describes.", r.version)
	return nil
}

// check checks the versions in the checkout. With a tag, it also checks
// that HEAD is a commit that the release can tag.
func (a *app) check(args []string) error {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	branch := flags.String("branch", defaultBranch, "")
	if err := flags.Parse(args); err != nil {
		return usageError("check: " + err.Error())
	}
	if flags.NArg() > 1 {
		return usageError("check takes at most one tag, as in: check v1.2.0")
	}
	var (
		r    release
		head string
	)
	if flags.NArg() == 1 {
		var err error
		if r, err = parseTag(flags.Arg(0)); err != nil {
			return err
		}
		// The release tags a commit on the branch, but the checks below
		// read the files of the working tree. Those are the files of HEAD
		// only when the tree has no changes.
		if head, err = a.git("rev-parse", "HEAD"); err != nil {
			return err
		}
		if err := a.requireCleanTree(); err != nil {
			return err
		}
		onBranch, err := a.isAncestor(head, *branch)
		if err != nil {
			return err
		}
		if !onBranch {
			return fmt.Errorf("HEAD is %s, which is not on %s. Check out the commit to release. If %s is out of date, fetch it first", head, *branch, *branch)
		}
		a.logf("HEAD is %s, which is on %s.", head, *branch)
	}

	v, err := a.readVersions()
	if err != nil {
		return err
	}
	if err := v.check(); err != nil {
		return err
	}
	plain := strings.TrimPrefix(v.version, "v")
	if len(v.pinned) == 0 {
		a.logf("durable.Version is %s, and no nested module requires the SDK.", plain)
	} else {
		a.logf("durable.Version is %s, and %s require %s %s.", plain, strings.Join(v.pinned, ", "), v.sdk, v.version)
	}
	if flags.NArg() == 0 {
		return nil
	}

	if r.dir == "." && r.version != v.version {
		return fmt.Errorf("durable.Version is %s, not %s. Run prepare %s, and merge the change to main before the release", plain, strings.TrimPrefix(r.version, "v"), r.version)
	}
	// A user of a nested module gets the SDK version that the module
	// requires from its release. So build a new module that takes the
	// nested module from this checkout and fetches that SDK version as the
	// go command is configured to: from GOPROXY, or from the module cache.
	if pin, ok := v.pins[r.dir]; ok {
		path := modulePath(v.sdk, r.dir)
		replace := []string{"-replace=" + path + "=" + filepath.Join(a.repo, r.dir)}
		if _, err := a.buildConsumer(a.goEnv(), path, "v0.0.0", replace); err != nil {
			return fmt.Errorf("%s requires %s %s. A new module that takes %s from this checkout and fetches that SDK version does not build. If %s is not released yet, release it first: %w", r.dir, v.sdk, pin, r.dir, pin, err)
		}
		a.logf("%s from this checkout builds with the released %s %s.", r.dir, v.sdk, pin)
	}
	a.logf("Commit %s is ready for the release %s. Create the release with this commit as its target.", head, r.tag())
	return nil
}

// consumer builds a new module against each published module in dirs,
// taking the modules from the checkout.
func (a *app) consumer(dirs []string) error {
	if len(dirs) == 0 {
		dirs = publishedModules
	}
	for _, dir := range dirs {
		if !slices.Contains(publishedModules, dir) {
			return usageError(fmt.Sprintf("%q is not a published module. The published modules are: %s", dir, strings.Join(publishedModules, " ")))
		}
	}
	root, err := a.readModFile(".")
	if err != nil {
		return err
	}
	// A replace directive without a version applies to every version that
	// a requirement names. So each published module resolves to this
	// checkout, and the check needs no released version.
	var replace []string
	for _, dir := range publishedModules {
		replace = append(replace, "-replace="+modulePath(root.Module.Path, dir)+"="+filepath.Join(a.repo, dir))
	}
	for _, dir := range dirs {
		path := modulePath(root.Module.Path, dir)
		n, err := a.buildConsumer(a.goEnv(), path, "v0.0.0", replace)
		if err != nil {
			return fmt.Errorf("a new module that imports %s from this checkout does not build: %w", path, err)
		}
		a.logf("%s: a new module that imports its %s from this checkout builds.", path, plural(n, "package"))
	}
	return nil
}

// verify checks a published release.
func (a *app) verify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	proxy := flags.String("proxy", defaultProxy, "")
	branch := flags.String("branch", defaultBranch, "")
	timeout := flags.Duration("timeout", 35*time.Minute, "")
	interval := flags.Duration("interval", 30*time.Second, "")
	if err := flags.Parse(args); err != nil {
		return usageError("verify: " + err.Error())
	}
	if flags.NArg() != 1 {
		return usageError("verify takes the tag of the release, as in: verify v1.2.0")
	}
	r, err := parseTag(flags.Arg(0))
	if err != nil {
		return err
	}

	// The checks of the files below must read the files of the tagged
	// commit. So HEAD must be that commit, with no changes.
	commit, err := a.git("rev-parse", "--verify", "--quiet", "refs/tags/"+r.tag()+"^{commit}")
	if err != nil {
		return fmt.Errorf("the checkout has no tag %s", r.tag())
	}
	head, err := a.git("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if head != commit {
		return fmt.Errorf("HEAD is %s, but %s points at %s. Check out the tag first", head, r.tag(), commit)
	}
	if err := a.requireCleanTree(); err != nil {
		return err
	}
	onBranch, err := a.isAncestor(commit, *branch)
	if err != nil {
		return err
	}
	if !onBranch {
		return fmt.Errorf("%s points at %s, which is not on %s. Release the next version from %s", r.tag(), commit, *branch, *branch)
	}
	a.logf("%s points at %s, which is on %s.", r.tag(), commit, *branch)

	v, err := a.readVersions()
	if err != nil {
		return err
	}
	if err := v.check(); err != nil {
		return err
	}
	if r.dir == "." && v.version != r.version {
		return fmt.Errorf("the tagged commit sets durable.Version to %s, not %s", strings.TrimPrefix(v.version, "v"), strings.TrimPrefix(r.version, "v"))
	}

	// The go commands below fetch modules from the proxy alone, into an
	// empty module cache. So they see what a new user of the release sees.
	cache, err := os.MkdirTemp("", "release-modcache-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(cache) }()
	env := proxyEnv(a.env, *proxy, cache)
	path := modulePath(v.sdk, r.dir)
	info, err := a.waitForModule(env, path, r.version, *timeout, *interval)
	if err != nil {
		return err
	}
	switch {
	case info.Origin == nil || info.Origin.Hash == "":
		a.logf("The module proxy serves %s@%s. It did not report the commit it fetched, so that commit was not compared with the tag.", path, r.version)
	case info.Origin.Hash != commit:
		return fmt.Errorf("the module proxy serves %s@%s from commit %s, but %s points at %s. A published version cannot change, so release the next version", path, r.version, info.Origin.Hash, r.tag(), commit)
	default:
		a.logf("The module proxy serves %s@%s from commit %s.", path, r.version, commit)
	}

	n, err := a.buildConsumer(env, path, r.version, nil)
	if err != nil {
		return fmt.Errorf("a new module that requires %s@%s does not build: %w", path, r.version, err)
	}
	a.logf("A new module that imports the %s of %s@%s builds.", plural(n, "package"), path, r.version)
	a.logf("%s is released.", r.tag())
	return nil
}

// waitForModule asks the module proxy for path@version until the proxy
// serves it or the timeout passes. A new version can take a while to
// appear. If someone asked for the version before its tag existed, the
// proxy can keep answering that the version does not exist for up to 30
// minutes.
func (a *app) waitForModule(env []string, path, version string, timeout, interval time.Duration) (*moduleInfo, error) {
	deadline := time.Now().Add(timeout)
	for attempt := 1; ; attempt++ {
		info, err := a.listModule(env, path, version)
		if err == nil {
			return info, nil
		}
		if time.Now().Add(interval).After(deadline) {
			return nil, fmt.Errorf("the module proxy does not serve %s@%s after %s: %w", path, version, plural(attempt, "attempt"), err)
		}
		a.logf("Attempt %d: the module proxy does not serve %s@%s yet. Retrying in %s.\n%v", attempt, path, version, interval, err)
		time.Sleep(interval)
	}
}

// moduleInfo is the part of the output of go list -m -json that the tool
// reads.
type moduleInfo struct {
	Version string
	Origin  *struct {
		Hash string
	}
}

// listModule asks the go command for path@version, outside any module.
func (a *app) listModule(env []string, path, version string) (*moduleInfo, error) {
	dir, err := os.MkdirTemp("", "release-list-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	out, err := a.command(env, dir, "go", "list", "-m", "-json", path+"@"+version)
	if err != nil {
		return nil, err
	}
	var info moduleInfo
	if err := json.Unmarshal([]byte(out), &info); err != nil {
		return nil, fmt.Errorf("go list -m -json %s@%s: %w", path, version, err)
	}
	return &info, nil
}

// buildConsumer creates a module in a temporary directory, as a user of
// path would. The module requires path at version and imports every
// package of path except commands and internal packages. Then
// buildConsumer builds the module. edits are go mod edit flags that it
// applies to the new module first. It returns how many packages the
// module imports.
func (a *app) buildConsumer(env []string, path, version string, edits []string) (int, error) {
	dir, err := os.MkdirTemp("", "release-consumer-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	goCmd := func(args ...string) (string, error) { return a.command(env, dir, "go", args...) }

	if _, err := goCmd("mod", "init", "example.com/consumer"); err != nil {
		return 0, err
	}
	edits = append(slices.Clone(edits), "-require="+path+"@"+version)
	if _, err := goCmd(append([]string{"mod", "edit"}, edits...)...); err != nil {
		return 0, err
	}
	// The pattern path/... matches the packages of the modules that the new
	// module requires. -mod=mod in GOFLAGS lets go list download the module
	// and record its checksum.
	out, err := goCmd("list", "-f", `{{if ne .Name "main"}}{{.ImportPath}}{{end}}`, path+"/...")
	if err != nil {
		return 0, err
	}
	var src strings.Builder
	src.WriteString("package main\n\nimport (\n")
	n := 0
	for _, pkg := range strings.Fields(out) {
		if strings.Contains(pkg+"/", "/internal/") {
			continue
		}
		fmt.Fprintf(&src, "\t_ %q\n", pkg)
		n++
	}
	src.WriteString(")\n\nfunc main() {}\n")
	if n == 0 {
		return 0, fmt.Errorf("%s@%s has no package that another module can import", path, version)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src.String()), 0o644); err != nil {
		return 0, err
	}
	if _, err := goCmd("mod", "tidy"); err != nil {
		return 0, err
	}
	if _, err := goCmd("build", "./..."); err != nil {
		return 0, err
	}
	return n, nil
}

// modFile is the part of the output of go mod edit -json that the tool
// reads.
type modFile struct {
	Module struct {
		Path string
	}
	Require []requirement
}

type requirement struct {
	Path    string
	Version string
}

// readModFile reads the go.mod file of the module in dir.
func (a *app) readModFile(dir string) (modFile, error) {
	var mf modFile
	out, err := a.command(a.goEnv(), a.repo, "go", "mod", "edit", "-json", filepath.Join(a.repo, dir, "go.mod"))
	if err != nil {
		return mf, err
	}
	if err := json.Unmarshal([]byte(out), &mf); err != nil {
		return mf, fmt.Errorf("%s/go.mod: %w", dir, err)
	}
	return mf, nil
}

// goEnv returns the environment of go commands that read the checkout.
// GO111MODULE=on makes the go command use module mode in every directory.
// Some CI runners set GO111MODULE=auto, and under auto the go command uses
// module mode only inside a module.
func (a *app) goEnv() []string {
	return withEnv(a.env, "GO111MODULE=on", "GOWORK=off", "GOFLAGS=-mod=mod")
}

// proxyEnv returns the environment of go commands that must fetch modules
// from proxy alone, into the empty module cache cache. GO111MODULE=on
// forces module mode, as in goEnv. The go command fetches a module that
// GOPRIVATE or GONOPROXY matches from its origin instead. GONOPROXY=none
// matches no module, and it overrides GOPRIVATE, so every module comes
// from the proxy. GOPROXY names no fallback, so a version that the proxy
// does not serve is an error. -modcacherw keeps the module cache
// removable.
func proxyEnv(env []string, proxy, cache string) []string {
	return withEnv(env,
		"GO111MODULE=on",
		"GOPROXY="+proxy,
		"GONOPROXY=none",
		"GOWORK=off",
		"GOFLAGS=-mod=mod -modcacherw",
		"GOMODCACHE="+cache,
	)
}

// withEnv returns env with the KEY=value entries of set added. exec.Cmd
// uses the last entry of a key, so set overrides the entries in env.
func withEnv(env []string, set ...string) []string {
	return append(slices.Clone(env), set...)
}

// git runs git in the checkout and returns its output without surrounding
// space.
func (a *app) git(args ...string) (string, error) {
	out, err := a.command(a.env, a.repo, "git", args...)
	return strings.TrimSpace(out), err
}

// requireCleanTree returns an error if the working tree has changes,
// including files that Git neither tracks nor ignores. check and verify
// read the files of the working tree, and those are the files of HEAD only
// when the tree has no changes.
func (a *app) requireCleanTree() error {
	status, err := a.command(a.env, a.repo, "git", "status", "--porcelain")
	if err != nil {
		return err
	}
	if status = strings.TrimRight(status, "\n"); status != "" {
		return fmt.Errorf("the working tree has changes. Commit them, or check out a commit without them:\n%s", status)
	}
	return nil
}

// isAncestor reports whether commit is reachable from ref.
func (a *app) isAncestor(commit, ref string) (bool, error) {
	_, err := a.git("merge-base", "--is-ancestor", commit, ref)
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return err == nil, err
}

// command runs name with args in dir and returns its standard output. When
// the command fails, the error includes its standard error.
func (a *app) command(env []string, dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}
