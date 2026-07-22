package durable

import "testing"

func TestHashID(t *testing.T) {
	// Known vector: md5("1") = c4ca4238a0b923820dcc509a6f75849b.
	if got, want := hashID("1"), "c4ca4238a0b92382"; got != want {
		t.Errorf("hashID(%q) = %q, want %q", "1", got, want)
	}
	// 16 hex chars regardless of input length.
	if got := hashID("7-2-11"); len(got) != 16 {
		t.Errorf("len(hashID()) = %d, want 16", len(got))
	}
}
