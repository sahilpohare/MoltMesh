package capability_test

import (
	"testing"

	"github.com/sahilpohare/p2p-a2a/pkg/capability"
)

func TestNew(t *testing.T) {
	cases := []struct {
		name    string
		wantID  string
		wantErr bool
	}{
		{"text-generation", "a2a:v1:cap:text-generation", false},
		{"search", "a2a:v1:cap:search", false},
		{"code-execution", "a2a:v1:cap:code-execution", false},
		{"", "", true},
		{"Bad Name", "", true},
		{"has_underscore", "", true},
		{"UPPER", "", true},
	}
	for _, c := range cases {
		id, err := capability.New(c.name)
		if (err != nil) != c.wantErr {
			t.Errorf("New(%q) err=%v, wantErr=%v", c.name, err, c.wantErr)
		}
		if !c.wantErr && id != c.wantID {
			t.Errorf("New(%q) = %q, want %q", c.name, id, c.wantID)
		}
	}
}

func TestValidate(t *testing.T) {
	valid := []string{
		capability.TextGeneration,
		capability.CodeExecution,
		"a2a:v2:cap:my-capability",
	}
	for _, id := range valid {
		if err := capability.Validate(id); err != nil {
			t.Errorf("Validate(%q) unexpected error: %v", id, err)
		}
	}

	invalid := []string{
		"a2a:v1:text-generation",     // missing "cap" segment
		"mcp:v1:cap:text-generation", // wrong scheme
		"a2a:1:cap:text",             // version missing 'v'
		"a2a:v1:cap:",                // empty name
		"a2a:v1:cap:Bad Name",        // invalid chars
	}
	for _, id := range invalid {
		if err := capability.Validate(id); err == nil {
			t.Errorf("Validate(%q) expected error, got nil", id)
		}
	}
}

func TestParse(t *testing.T) {
	scheme, ver, name, err := capability.Parse(capability.TextGeneration)
	if err != nil {
		t.Fatalf("Parse unexpected error: %v", err)
	}
	if scheme != "a2a" || ver != "v1" || name != "text-generation" {
		t.Errorf("Parse = (%q, %q, %q), want (a2a, v1, text-generation)", scheme, ver, name)
	}
}

func TestName(t *testing.T) {
	if got := capability.Name(capability.Search); got != "search" {
		t.Errorf("Name = %q, want %q", got, "search")
	}
	// invalid input is returned as-is
	if got := capability.Name("not-a-cap"); got != "not-a-cap" {
		t.Errorf("Name(invalid) = %q, want passthrough", got)
	}
}

func TestShort(t *testing.T) {
	if got := capability.Short(capability.TextGeneration); got != "text-generation" {
		t.Errorf("Short = %q, want %q", got, "text-generation")
	}
}

func TestWithVersion(t *testing.T) {
	id, err := capability.WithVersion(capability.TextGeneration, "v2")
	if err != nil {
		t.Fatalf("WithVersion error: %v", err)
	}
	if id != "a2a:v2:cap:text-generation" {
		t.Errorf("WithVersion = %q, want a2a:v2:cap:text-generation", id)
	}
}
