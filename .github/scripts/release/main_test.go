package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a repository with the module layout of the real one. The
// SDK, example.com/sdk, is at version 0.1.0, and each nested module that
// requires it requires v0.1.0.
var fixture = map[string]string{
	"go.mod":                              "module example.com/sdk\n\ngo 1.24\n",
	"durable/version.go":                  "package durable\n\n// Version is the SDK version.\nconst Version = \"0.1.0\"\n",
	"durable/durabletest/durabletest.go":  "package durabletest\n\nimport _ \"example.com/sdk/durable\"\n",
	"durable/internal/wire/wire.go":       "package wire\n",
	"insight/go.mod":                      "module example.com/sdk/insight\n\ngo 1.24\n\nrequire example.com/sdk v0.1.0\n\nreplace example.com/sdk => ..\n",
	"insight/insight.go":                  "package insight\n\nimport _ \"example.com/sdk/durable\"\n",
	"analysis/go.mod":                     "module example.com/sdk/analysis\n\ngo 1.24\n",
	"analysis/durablelint/durablelint.go": "package durablelint\n",
	"analysis/cmd/durablelint/main.go":    "package main\n\nfunc main() {}\n",
	"conformance/go.mod":                  "module example.com/sdk/conformance\n\ngo 1.24\n\nrequire example.com/sdk v0.1.0\n\nreplace example.com/sdk => ../\n",
	"examples/go.mod":                     "module example.com/sdk/examples\n\ngo 1.24\n\nrequire (\n\texample.com/sdk v0.1.0\n\texample.com/sdk/insight v0.0.0\n)\n\nreplace example.com/sdk => ../\n\nreplace example.com/sdk/insight => ../insight\n",
}

// newRepo writes the fixture to a new directory and returns an app for it,
// and the buffer that the app reports to.
func newRepo(t *testing.T) (*app, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	for name, content := range fixture {
		writeFile(t, filepath.Join(dir, name), content)
	}
	out := new(bytes.Buffer)
	// The tests need no network. GOPROXY=off forbids downloads, except
	// where a test sets a file proxy. GO111MODULE=off makes every go
	// command fail unless the tool forces module mode for it. Each test
	// gets its own module cache, so no test sees a module that another
	// test downloaded. Git ignores the user's configuration, which could
	// require signed commits.
	cache := t.TempDir()
	t.Cleanup(func() {
		// The go command makes the module cache read-only, so the
		// cleanup of t.TempDir cannot remove it. go clean -modcache can.
		cmd := exec.Command("go", "clean", "-modcache")
		cmd.Env = withEnv(os.Environ(), "GOMODCACHE="+cache, "GOFLAGS=")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("go clean -modcache: %v\n%s", err, out)
		}
	})
	env := withEnv(os.Environ(),
		"GO111MODULE=off",
		"GOPROXY=off",
		"GOSUMDB=off",
		"GOFLAGS=",
		"GOMODCACHE="+cache,
		"GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=test",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	return &app{repo: dir, env: env, out: out}, out
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// snapshot returns the content of the fixture's files in the checkout.
func snapshot(t *testing.T, a *app) map[string]string {
	t.Helper()
	files := map[string]string{}
	for name := range fixture {
		files[name] = readFile(t, filepath.Join(a.repo, name))
	}
	return files
}

// requiredSDK returns the SDK version that the module in dir requires.
func requiredSDK(t *testing.T, a *app, dir string) string {
	t.Helper()
	mf, err := a.readModFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range mf.Require {
		if r.Path == "example.com/sdk" {
			return r.Version
		}
	}
	return ""
}

func gitCmd(t *testing.T, a *app, args ...string) string {
	t.Helper()
	out, err := a.git(args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// committedRepo returns a fixture repository whose files are one commit,
// which is also origin/main. It also returns the commit's hash.
func committedRepo(t *testing.T) (*app, *bytes.Buffer, string) {
	t.Helper()
	a, out := newRepo(t)
	gitCmd(t, a, "init", "-q")
	return a, out, merge(t, a, "first")
}

// merge commits every change in the checkout and moves origin/main to the
// new commit, as merging a pull request would. It returns the commit's
// hash.
func merge(t *testing.T, a *app, message string) string {
	t.Helper()
	gitCmd(t, a, "add", "-A")
	gitCmd(t, a, "commit", "-q", "-m", message)
	gitCmd(t, a, "update-ref", "refs/remotes/origin/main", "HEAD")
	return gitCmd(t, a, "rev-parse", "HEAD")
}

// TestPrepare follows the steps of an SDK release: prepare, merge, and
// check. check refuses the release until the changes are a commit on
// origin/main.
func TestPrepare(t *testing.T) {
	a, out, _ := committedRepo(t)
	if err := a.run([]string{"prepare", "v1.0.0"}); err != nil {
		t.Fatalf("prepare v1.0.0: %v\n%s", err, out)
	}
	if got := readFile(t, filepath.Join(a.repo, versionFile)); !strings.Contains(got, "\nconst Version = \"1.0.0\"\n") {
		t.Errorf("%s after prepare:\n%s", versionFile, got)
	}
	for _, dir := range []string{"insight", "conformance", "examples"} {
		if got := requiredSDK(t, a, dir); got != "v1.0.0" {
			t.Errorf("%s requires example.com/sdk %s, want v1.0.0", dir, got)
		}
	}
	if got := readFile(t, filepath.Join(a.repo, "analysis/go.mod")); got != fixture["analysis/go.mod"] {
		t.Errorf("prepare changed analysis/go.mod:\n%s", got)
	}

	check := []string{"check", "v1.0.0"}
	err := a.run(check)
	if want := "the working tree has changes"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("check v1.0.0 before the commit: error %v, want one that contains %q", err, want)
	}
	gitCmd(t, a, "commit", "-q", "-a", "-m", "chore(release): v1.0.0")
	err = a.run(check)
	if want := "which is not on origin/main"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("check v1.0.0 before the merge: error %v, want one that contains %q", err, want)
	}
	gitCmd(t, a, "update-ref", "refs/remotes/origin/main", "HEAD")
	if err := a.run(check); err != nil {
		t.Fatalf("check v1.0.0 after the merge: %v\n%s", err, out)
	}
	if want := "Commit " + gitCmd(t, a, "rev-parse", "HEAD") + " is ready for the release v1.0.0."; !strings.Contains(out.String(), want) {
		t.Errorf("check output has no line %q:\n%s", want, out)
	}

	before := snapshot(t, a)
	if err := a.run([]string{"prepare", "v1.0.0"}); err != nil {
		t.Fatalf("second prepare v1.0.0: %v", err)
	}
	if !maps.Equal(before, snapshot(t, a)) {
		t.Error("a second prepare v1.0.0 changed files")
	}
}

func TestPrepareRefuses(t *testing.T) {
	tests := []struct {
		arg  string
		want string // part of the error
	}{
		{"v1.0", "does not have the form"},
		{"1.0.0", "does not have the form"},
		{"v1.02.0", "does not have the form"},
		{"v1.0.0-rc1", "does not have the form"},
		{"v1.0.0-preview.1", "does not have the form"},
		{"v1.0.0+build.1", "does not have the form"},
		{"v2.0.0", "needs a new module path"},
		{"insight/v0.1.0", "only an SDK release needs preparing"},
		{"insight/v1.0.0", "insight stays below v1"},
		{"examples/v0.1.0", `"examples" is not a published module`},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			t.Parallel()
			a, _ := newRepo(t)
			err := a.run([]string{"prepare", tt.arg})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("prepare %s: error %v, want one that contains %q", tt.arg, err, tt.want)
			}
			if !maps.Equal(fixture, snapshot(t, a)) {
				t.Errorf("prepare %s changed files", tt.arg)
			}
		})
	}
}

// TestPrepareRefusesLowerMajor checks that prepare keeps the SDK at v1
// once durable.Version names a v1 version.
func TestPrepareRefusesLowerMajor(t *testing.T) {
	a, _ := newRepo(t)
	if err := a.run([]string{"prepare", "v1.0.0"}); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, a)
	err := a.run([]string{"prepare", "v0.1.1"})
	if want := "A release cannot return it to v0"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("prepare v0.1.1 after v1.0.0: error %v, want one that contains %q", err, want)
	}
	if !maps.Equal(before, snapshot(t, a)) {
		t.Error("prepare v0.1.1 changed files")
	}
	// A pre-release of v1 is not a lower major version.
	if err := a.run([]string{"prepare", "v1.0.0-rc.1"}); err != nil {
		t.Errorf("prepare v1.0.0-rc.1 after v1.0.0: %v", err)
	}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name string
		edit map[string]string // files to write before the check
		args []string
		want string // part of the error; "" when check succeeds
	}{
		{name: "versions agree"},
		{name: "SDK release", args: []string{"v0.1.0"}},
		{name: "SDK release of another version", args: []string{"v0.2.0"}, want: "durable.Version is 0.1.0, not 0.2.0"},
		{name: "analysis release", args: []string{"analysis/v0.1.0"}},
		{name: "analysis v1", args: []string{"analysis/v1.0.0"}, want: "analysis stays below v1"},
		{
			name: "pin differs",
			edit: map[string]string{"conformance/go.mod": strings.Replace(fixture["conformance/go.mod"], "v0.1.0", "v0.0.9", 1)},
			want: "conformance/go.mod requires v0.0.9",
		},
		{
			name: "Version with a v",
			edit: map[string]string{versionFile: "package durable\n\nconst Version = \"v0.1.0\"\n"},
			want: "without the leading v",
		},
		{
			name: "Version form",
			edit: map[string]string{versionFile: "package durable\n\nconst Version = \"0.1\"\n"},
			want: "must have the form",
		},
		{
			name: "Version declared twice",
			edit: map[string]string{versionFile: "package durable\n\nconst Version = \"0.1.0\"\n\n/*\nconst Version = \"0.1.0\"\n*/\n"},
			want: "exactly one line",
		},
		{
			name: "module path",
			edit: map[string]string{"insight/go.mod": strings.Replace(fixture["insight/go.mod"], "sdk/insight", "sdk/insights", 1)},
			want: "declares module example.com/sdk/insights",
		},
		{
			name: "untracked file",
			edit: map[string]string{"durable/new.go": "package durable\n"},
			args: []string{"v0.1.0"},
			want: "the working tree has changes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a, _, _ := committedRepo(t)
			for name, content := range tt.edit {
				writeFile(t, filepath.Join(a.repo, name), content)
			}
			err := a.run(append([]string{"check"}, tt.args...))
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("check %v: %v", tt.args, err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Fatalf("check %v: error %v, want one that contains %q", tt.args, err, tt.want)
			}
		})
	}
}

// TestCheckInsightRelease checks that check builds insight from the
// checkout against the released SDK version that insight requires.
func TestCheckInsightRelease(t *testing.T) {
	a, out, _ := committedRepo(t)
	proxy := t.TempDir()
	a.env = withEnv(a.env, "GOPROXY="+fileURL(proxy))
	args := []string{"check", "insight/v0.1.0"}
	err := a.run(args)
	if want := "If v0.1.0 is not released yet, release it first"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("check before the SDK release: error %v, want one that contains %q", err, want)
	}
	publish(t, a, proxy, ".", "v0.1.0", "0123456789abcdef0123456789abcdef01234567")
	if err := a.run(args); err != nil {
		t.Fatalf("check after the SDK release: %v\n%s", err, out)
	}

	// insight now uses an SDK API that the released SDK v0.1.0 lacks. It
	// builds in the checkout, where its replace directive applies, but not
	// for a user.
	writeFile(t, filepath.Join(a.repo, "durable/new.go"), "package durable\n\nconst New = 1\n")
	writeFile(t, filepath.Join(a.repo, "insight/new.go"), "package insight\n\nimport \"example.com/sdk/durable\"\n\nconst _ = durable.New\n")
	if _, err := a.command(a.goEnv(), filepath.Join(a.repo, "insight"), "go", "build", "./..."); err != nil {
		t.Fatalf("insight does not build in the checkout: %v", err)
	}
	merge(t, a, "use durable.New in insight")
	err = a.run(args)
	if want := "undefined: durable.New"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("check with an unreleased SDK API: error %v, want one that contains %q", err, want)
	}
}

func TestConsumer(t *testing.T) {
	a, out := newRepo(t)
	if err := a.run([]string{"consumer"}); err != nil {
		t.Fatalf("consumer: %v\n%s", err, out)
	}
	// The internal package and the command are not importable.
	for _, want := range []string{
		"example.com/sdk: a new module that imports its 2 packages",
		"example.com/sdk/insight: a new module that imports its 1 package",
		"example.com/sdk/analysis: a new module that imports its 1 package",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("consumer output has no line %q:\n%s", want, out)
		}
	}
}

// TestConsumerReplaceOnlyDependency checks the case the consumer command
// exists for. insight requires a module that only its own replace
// directive resolves. So insight builds in the checkout, but not for a
// user, whose build ignores that directive.
func TestConsumerReplaceOnlyDependency(t *testing.T) {
	a, _ := newRepo(t)
	writeFile(t, filepath.Join(a.repo, "extra/go.mod"), "module example.com/extra\n\ngo 1.24\n")
	writeFile(t, filepath.Join(a.repo, "extra/extra.go"), "package extra\n")
	writeFile(t, filepath.Join(a.repo, "insight/go.mod"), fixture["insight/go.mod"]+"\nrequire example.com/extra v0.0.0\n\nreplace example.com/extra => ../extra\n")
	writeFile(t, filepath.Join(a.repo, "insight/extra.go"), "package insight\n\nimport _ \"example.com/extra\"\n")
	if _, err := a.command(a.goEnv(), filepath.Join(a.repo, "insight"), "go", "build", "./..."); err != nil {
		t.Fatalf("insight does not build in the checkout: %v", err)
	}
	err := a.run([]string{"consumer", "insight"})
	if want := "example.com/extra"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("consumer insight: error %v, want one that contains %q", err, want)
	}
}

// taggedRepo returns a fixture repository with two commits on origin/main.
// The second commit is the SDK release v1.0.0, and HEAD is that commit,
// tagged v1.0.0. It also returns an empty file proxy, and the commit hashes.
func taggedRepo(t *testing.T) (a *app, out *bytes.Buffer, proxy, first, release string) {
	t.Helper()
	a, out, first = committedRepo(t)
	if err := a.run([]string{"prepare", "v1.0.0"}); err != nil {
		t.Fatal(err)
	}
	release = merge(t, a, "chore(release): v1.0.0")
	gitCmd(t, a, "tag", "v1.0.0")
	return a, out, t.TempDir(), first, release
}

func verifyArgs(proxy, tag string) []string {
	return []string{"verify", "-proxy", fileURL(proxy), "-timeout", "0", "-interval", "0", tag}
}

func TestVerify(t *testing.T) {
	a, out, proxy, _, release := taggedRepo(t)
	publish(t, a, proxy, ".", "v1.0.0", release)
	// A user's GOPRIVATE must not make verify fetch the modules from their
	// origin instead of the proxy.
	a.env = withEnv(a.env, "GOPRIVATE=example.com")
	if err := a.run(verifyArgs(proxy, "v1.0.0")); err != nil {
		t.Fatalf("verify v1.0.0: %v\n%s", err, out)
	}
	for _, want := range []string{
		"v1.0.0 points at " + release + ", which is on origin/main.",
		"The module proxy serves example.com/sdk@v1.0.0 from commit " + release + ".",
		"A new module that imports the 2 packages of example.com/sdk@v1.0.0 builds.",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("verify output has no line %q:\n%s", want, out)
		}
	}
}

func TestVerifyInsight(t *testing.T) {
	a, out, proxy, _, release := taggedRepo(t)
	gitCmd(t, a, "tag", "insight/v0.1.0")
	publish(t, a, proxy, "insight", "v0.1.0", release)

	// insight requires the SDK v1.0.0, so a user needs it from the proxy.
	err := a.run(verifyArgs(proxy, "insight/v0.1.0"))
	if want := "does not build"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("verify insight/v0.1.0 before the SDK release: error %v, want one that contains %q", err, want)
	}
	publish(t, a, proxy, ".", "v1.0.0", release)
	if err := a.run(verifyArgs(proxy, "insight/v0.1.0")); err != nil {
		t.Fatalf("verify insight/v0.1.0: %v\n%s", err, out)
	}
}

func TestVerifyRefuses(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, a *app, proxy, first, release string)
		tag   string
		want  string // part of the error
	}{
		{
			name: "no such tag",
			tag:  "v1.0.1",
			want: "the checkout has no tag v1.0.1",
		},
		{
			name: "proxy does not serve it",
			// Nothing is published.
			setup: func(*testing.T, *app, string, string, string) {},
			want:  "the module proxy does not serve example.com/sdk@v1.0.0 after 1 attempt",
		},
		{
			name: "proxy serves another commit",
			setup: func(t *testing.T, a *app, proxy, first, _ string) {
				publish(t, a, proxy, ".", "v1.0.0", first)
			},
			want: "A published version cannot change",
		},
		{
			name: "tag not on the branch",
			setup: func(t *testing.T, a *app, proxy, first, release string) {
				gitCmd(t, a, "update-ref", "refs/remotes/origin/main", first)
			},
			want: "which is not on origin/main",
		},
		{
			name: "HEAD is not the tag",
			setup: func(t *testing.T, a *app, proxy, first, release string) {
				gitCmd(t, a, "checkout", "-q", first)
			},
			want: "Check out the tag first",
		},
		{
			name: "changed files",
			setup: func(t *testing.T, a *app, proxy, first, release string) {
				writeFile(t, filepath.Join(a.repo, versionFile), "package durable\n\nconst Version = \"1.0.1\"\n")
			},
			want: "the working tree has changes",
		},
		{
			name: "tag of another version",
			setup: func(t *testing.T, a *app, proxy, first, release string) {
				gitCmd(t, a, "tag", "v1.0.1")
				publish(t, a, proxy, ".", "v1.0.1", release)
			},
			tag:  "v1.0.1",
			want: "sets durable.Version to 1.0.0, not 1.0.1",
		},
		{
			name: "tag form",
			tag:  "sdk-v1.0.0",
			want: "does not have the form",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a, _, proxy, first, release := taggedRepo(t)
			if tt.setup != nil {
				tt.setup(t, a, proxy, first, release)
			} else {
				publish(t, a, proxy, ".", "v1.0.0", release)
			}
			tag := tt.tag
			if tag == "" {
				tag = "v1.0.0"
			}
			err := a.run(verifyArgs(proxy, tag))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("verify %s: error %v, want one that contains %q", tag, err, tt.want)
			}
		})
	}
}

func TestRun(t *testing.T) {
	a, _, _ := committedRepo(t)
	tests := []struct {
		args []string
		code int
	}{
		{[]string{"-repo", a.repo, "check"}, 0},
		{[]string{"-repo", a.repo}, 2},
		{[]string{"-repo", a.repo, "release"}, 2},
		{[]string{"-repo", a.repo, "check", "v1.0.0"}, 1},
	}
	for _, tt := range tests {
		var stdout, stderr bytes.Buffer
		if code := run(tt.args, &stdout, &stderr); code != tt.code {
			t.Errorf("run %v = %d, want %d\n%s%s", tt.args, code, tt.code, &stdout, &stderr)
		}
	}
}

func fileURL(dir string) string {
	return "file://" + filepath.ToSlash(dir)
}

// publish adds the module in dir of a's checkout to the file proxy at
// proxy, at version, as the proxy would serve it after fetching that
// version from commit. A file proxy has the layout of the GOPROXY
// protocol.
func publish(t *testing.T, a *app, proxy, dir, version, commit string) {
	t.Helper()
	mf, err := a.readModFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := mf.Module.Path
	base := filepath.Join(proxy, filepath.FromSlash(path), "@v")
	info, err := json.Marshal(map[string]any{
		"Version": version,
		"Time":    "2026-01-01T00:00:00Z",
		"Origin":  map[string]string{"VCS": "git", "URL": "https://example.com/sdk", "Hash": commit},
	})
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(a.repo, dir)
	writeFile(t, filepath.Join(base, version+".info"), string(info))
	writeFile(t, filepath.Join(base, version+".mod"), readFile(t, filepath.Join(root, "go.mod")))
	f, err := os.OpenFile(filepath.Join(base, "list"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(version + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// The zip holds the module's files under path@version/. It leaves out
	// the Git directory and every nested module.
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if _, err := os.Stat(filepath.Join(p, "go.mod")); d.Name() == ".git" || (p != root && err == nil) {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		w, err := zw.Create(path + "@" + version + "/" + filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		_, err = w.Write([]byte(readFile(t, p)))
		return err
	})
	if err == nil {
		err = zw.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(base, version+".zip"), buf.String())
}
