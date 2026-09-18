// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// TreeColumn selects one column of the operation tree rendered by
// [TestResult.FormatTree] and [TestResult.WriteTree].
type TreeColumn int

// Columns available to [TestResult.FormatTree] and [TestResult.WriteTree].
const (
	// ColumnName is the operation name, indented by nesting depth.
	ColumnName TreeColumn = iota
	// ColumnType is the operation type (e.g. "STEP", "CONTEXT").
	ColumnType
	// ColumnSubType is the operation subtype (e.g. "Step",
	// "RunInChildContext").
	ColumnSubType
	// ColumnStatus is the operation status (e.g. "SUCCEEDED").
	ColumnStatus
	// ColumnStartTime is [TestOperation.StartTime] in UTC.
	ColumnStartTime
	// ColumnEndTime is [TestOperation.EndTime] in UTC.
	ColumnEndTime
	// ColumnDuration is EndTime minus StartTime.
	ColumnDuration
	// ColumnID is the operation's wire ID.
	ColumnID
	// ColumnParentID is the parent operation's wire ID.
	ColumnParentID
)

// defaultTreeColumns is the column set rendered when no columns are given.
// It is unexported so that no caller can change the default output.
var defaultTreeColumns = [...]TreeColumn{
	ColumnName, ColumnType, ColumnSubType, ColumnStatus,
	ColumnStartTime, ColumnEndTime, ColumnDuration,
}

// DefaultTreeColumns returns the column set [TestResult.FormatTree] and
// [TestResult.WriteTree] render when called with no columns: name, type,
// subtype, status, start time, end time, and duration, in that order.
// Each call returns a new slice, so a caller can append to or reorder the
// result without changing the default.
func DefaultTreeColumns() []TreeColumn {
	return append([]TreeColumn(nil), defaultTreeColumns[:]...)
}

// treeMissing is printed in place of an empty or unknown cell value.
const treeMissing = "-"

// treeIndent is the indentation added per nesting level.
const treeIndent = "  "

// treeTimeFormat renders timestamps with millisecond precision in UTC, so
// the same result renders the same text in every time zone.
const treeTimeFormat = "2006-01-02T15:04:05.000Z"

// FormatTree renders the operations of the result as an indented tree and
// returns the text. Each operation is one row; a child operation is
// indented under its parent, and children keep the order they have in
// [TestResult.Operations]. The first line is a header naming the columns.
//
// columns selects which columns are rendered, in the given order. With no
// columns, the [DefaultTreeColumns] set is used. Empty values render as
// "-".
//
// An operation whose ParentID names no operation in the result is rendered
// at the top level rather than dropped. The output depends only on the
// result's operations, so it is deterministic for a given result and can
// be compared in tests.
//
// Pass the text to the test logger to see the tree when an assertion
// fails:
//
//	t.Log(result.FormatTree())
func (r *TestResult) FormatTree(columns ...TreeColumn) string {
	var sb strings.Builder
	// strings.Builder never returns a write error.
	_ = r.WriteTree(&sb, columns...)
	return sb.String()
}

// WriteTree renders the same text as [TestResult.FormatTree] to w. It
// returns the first error w reports.
func (r *TestResult) WriteTree(w io.Writer, columns ...TreeColumn) error {
	if len(columns) == 0 {
		columns = defaultTreeColumns[:]
	}
	if r == nil || len(r.Operations) == 0 {
		_, err := io.WriteString(w, "(no operations)\n")
		return err
	}

	rows := [][]string{treeHeader(columns)}
	for _, n := range r.treeNodes() {
		rows = append(rows, treeRow(n, columns))
	}

	widths := make([]int, len(columns))
	for _, row := range rows {
		for i, cell := range row {
			widths[i] = max(widths[i], len(cell))
		}
	}

	for _, row := range rows {
		var sb strings.Builder
		for i, cell := range row {
			if i > 0 {
				sb.WriteString("  ")
			}
			sb.WriteString(cell)
			if i < len(row)-1 {
				sb.WriteString(strings.Repeat(" ", widths[i]-len(cell)))
			}
		}
		sb.WriteByte('\n')
		if _, err := io.WriteString(w, sb.String()); err != nil {
			return err
		}
	}
	return nil
}

// treeNode is one operation together with its nesting depth in the
// rendered tree.
type treeNode struct {
	op    *TestOperation
	depth int
}

// treeNodes lists the operations in render order: depth-first from each
// top-level operation, with siblings in [TestResult.Operations] order.
//
// A top-level operation has an empty ParentID or a ParentID that names no
// operation in the result. Any operation not reached from a top-level one
// (which requires a cycle of ParentIDs) is appended at depth 0 so every
// operation is rendered.
func (r *TestResult) treeNodes() []treeNode {
	ids := make(map[string]bool, len(r.Operations))
	for i := range r.Operations {
		if id := r.Operations[i].ID; id != "" {
			ids[id] = true
		}
	}

	children := make(map[string][]int)
	var roots []int
	for i := range r.Operations {
		pid := r.Operations[i].ParentID
		if pid == "" || !ids[pid] {
			roots = append(roots, i)
			continue
		}
		children[pid] = append(children[pid], i)
	}

	visited := make([]bool, len(r.Operations))
	var nodes []treeNode
	var walk func(i, depth int)
	walk = func(i, depth int) {
		if visited[i] {
			return
		}
		visited[i] = true
		nodes = append(nodes, treeNode{op: &r.Operations[i], depth: depth})
		for _, c := range children[r.Operations[i].ID] {
			walk(c, depth+1)
		}
	}
	for _, i := range roots {
		walk(i, 0)
	}
	for i := range r.Operations {
		walk(i, 0)
	}
	return nodes
}

// treeHeader returns the header cells for the given columns.
func treeHeader(columns []TreeColumn) []string {
	cells := make([]string, len(columns))
	for i, c := range columns {
		cells[i] = c.String()
	}
	return cells
}

// treeRow returns the cells of one operation for the given columns.
func treeRow(n treeNode, columns []TreeColumn) []string {
	cells := make([]string, len(columns))
	for i, c := range columns {
		cells[i] = treeCell(n, c)
	}
	return cells
}

// treeCell renders one column of one operation.
func treeCell(n treeNode, c TreeColumn) string {
	op := n.op
	switch c {
	case ColumnName:
		return strings.Repeat(treeIndent, n.depth) + orMissing(op.Name)
	case ColumnType:
		return orMissing(op.Type)
	case ColumnSubType:
		return orMissing(op.SubType)
	case ColumnStatus:
		return orMissing(op.Status)
	case ColumnStartTime:
		return treeTime(op.StartTime)
	case ColumnEndTime:
		return treeTime(op.EndTime)
	case ColumnDuration:
		if op.StartTime.IsZero() || op.EndTime.IsZero() {
			return treeMissing
		}
		return op.EndTime.Sub(op.StartTime).String()
	case ColumnID:
		return orMissing(op.ID)
	case ColumnParentID:
		return orMissing(op.ParentID)
	default:
		return treeMissing
	}
}

// treeTime renders t in UTC, or "-" for the zero time.
func treeTime(t time.Time) string {
	if t.IsZero() {
		return treeMissing
	}
	return t.UTC().Format(treeTimeFormat)
}

// orMissing returns s, or "-" when s is empty.
func orMissing(s string) string {
	if s == "" {
		return treeMissing
	}
	return s
}

// String returns the column's header label.
func (c TreeColumn) String() string {
	switch c {
	case ColumnName:
		return "NAME"
	case ColumnType:
		return "TYPE"
	case ColumnSubType:
		return "SUBTYPE"
	case ColumnStatus:
		return "STATUS"
	case ColumnStartTime:
		return "START"
	case ColumnEndTime:
		return "END"
	case ColumnDuration:
		return "DURATION"
	case ColumnID:
		return "ID"
	case ColumnParentID:
		return "PARENT"
	default:
		return fmt.Sprintf("TreeColumn(%d)", int(c))
	}
}
