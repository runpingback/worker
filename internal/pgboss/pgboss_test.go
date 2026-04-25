package pgboss

import "testing"

func TestNewClient(t *testing.T) {
	c := New(nil)
	if c == nil {
		t.Fatal("New returned nil")
	}
}
