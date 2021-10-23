package litestream

import (
	"testing"
)

func TestReplica_Name(t *testing.T) {
	if got, want := NewReplica(nil, "NAME").Name(), "NAME"; got != want {
		t.Fatalf("Name()=%v, want %v", got, want)
	}
}
