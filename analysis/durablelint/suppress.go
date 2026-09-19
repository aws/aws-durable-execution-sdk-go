package durablelint

import (
	"go/ast"
	"go/token"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Suppression directives. See the package documentation for the syntax.
const (
	ignoreLine = "//durable:ignore"
	ignoreFile = "//durable:ignore-file"
)

// ruleSet is the set of rule names a directive names; nil means every rule.
type ruleSet map[string]bool

func (r ruleSet) covers(rule string) bool { return r == nil || r[rule] }

// union merges two directives; nil (every rule) absorbs any list.
func (r ruleSet) union(o ruleSet) ruleSet {
	if r == nil || o == nil {
		return nil
	}
	out := ruleSet{}
	for k := range r {
		out[k] = true
	}
	for k := range o {
		out[k] = true
	}
	return out
}

// fileSuppressions holds the directives found in one file.
type fileSuppressions struct {
	whole    bool            // an ignore-file directive is present
	wholeSet ruleSet         // rules the ignore-file directives name
	lines    map[int]ruleSet // line number to the rules ignored on that line
}

// suppressions indexes the directive comments of every file in a pass.
type suppressions struct {
	files map[*token.File]*fileSuppressions
}

// newSuppressions scans the comments of every file in pass.
func newSuppressions(pass *analysis.Pass) *suppressions {
	s := &suppressions{files: map[*token.File]*fileSuppressions{}}
	for _, f := range pass.Files {
		tf := pass.Fset.File(f.Pos())
		if tf == nil {
			continue
		}
		fs := &fileSuppressions{lines: map[int]ruleSet{}}
		var code lineSet
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if rest, ok := directive(c.Text, ignoreFile); ok {
					rules := parseRules(rest)
					if fs.whole {
						rules = fs.wholeSet.union(rules)
					}
					fs.whole, fs.wholeSet = true, rules
					continue
				}
				if rest, ok := directive(c.Text, ignoreLine); ok {
					if code == nil {
						code = codeLines(tf, f)
					}
					// A trailing comment covers its own line. A comment on
					// a line of its own covers the line below it.
					target := tf.Line(c.Pos())
					if !code[target] {
						target++
					}
					rules := parseRules(rest)
					if cur, ok := fs.lines[target]; ok {
						rules = cur.union(rules)
					}
					fs.lines[target] = rules
				}
			}
		}
		s.files[tf] = fs
	}
	return s
}

// lineSet is the set of line numbers in a file that carry code.
type lineSet map[int]bool

// codeLines returns the lines of f on which a token other than a comment
// starts or ends. A directive comment on one of those lines is a trailing
// comment; on any other line it stands alone.
func codeLines(tf *token.File, f *ast.File) lineSet {
	lines := lineSet{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n.(type) {
		case nil, *ast.File:
			return true
		case *ast.CommentGroup, *ast.Comment:
			return false
		}
		lines[tf.Line(n.Pos())] = true
		if end := n.End(); end > n.Pos() {
			lines[tf.Line(end-1)] = true
		}
		return true
	})
	return lines
}

// directive reports whether text is the given directive keyword, alone or
// followed by whitespace, and returns what follows the keyword.
func directive(text, keyword string) (string, bool) {
	if !strings.HasPrefix(text, keyword) {
		return "", false
	}
	rest := text[len(keyword):]
	if rest != "" && rest[0] != ' ' && rest[0] != '\t' {
		return "", false
	}
	return rest, true
}

// parseRules reads the rule list that may follow a directive keyword. The
// list is comma-separated and ends at "--", which introduces a free-text
// reason. An empty list means every rule.
func parseRules(rest string) ruleSet {
	if i := strings.Index(rest, "--"); i >= 0 {
		rest = rest[:i]
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return nil
	}
	set := ruleSet{}
	for name := range strings.SplitSeq(rest, ",") {
		if name = strings.TrimSpace(name); name != "" {
			set[name] = true
		}
	}
	return set
}

// suppressed reports whether a diagnostic from rule at pos is silenced by a
// directive.
func (s *suppressions) suppressed(fset *token.FileSet, rule string, pos token.Pos) bool {
	tf := fset.File(pos)
	if tf == nil {
		return false
	}
	fs, ok := s.files[tf]
	if !ok {
		return false
	}
	if fs.whole && fs.wholeSet.covers(rule) {
		return true
	}
	rules, ok := fs.lines[tf.Line(pos)]
	return ok && rules.covers(rule)
}

// report emits a diagnostic at node unless a directive suppresses it.
func (s *suppressions) report(pass *analysis.Pass, node ast.Node, msg string) {
	if s.suppressed(pass.Fset, pass.Analyzer.Name, node.Pos()) {
		return
	}
	pass.Report(analysis.Diagnostic{Pos: node.Pos(), End: node.End(), Message: msg})
}
