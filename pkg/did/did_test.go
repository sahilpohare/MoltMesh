package did_test

import (
	"testing"

	"github.com/sahilpohare/p2p-a2a/pkg/did"
)

const exampleDID = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

func TestValidate(t *testing.T) {
	cases := []struct {
		input string
		ok    bool
	}{
		{exampleDID, true},
		{"did:key:z6Mk" + "A", false}, // too short
		{"did:web:example.com", false},
		{"not-a-did", false},
		{"did:key:z6Mk" + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", true},
	}
	for _, c := range cases {
		err := did.Validate(c.input)
		if (err == nil) != c.ok {
			t.Errorf("Validate(%q) err=%v, want ok=%v", c.input, err, c.ok)
		}
	}
}

func TestMethod(t *testing.T) {
	if got := did.Method(exampleDID); got != "key" {
		t.Errorf("Method = %q, want %q", got, "key")
	}
	if got := did.Method("not-a-did"); got != "" {
		t.Errorf("Method(invalid) = %q, want empty", got)
	}
}

func TestKeyMaterial(t *testing.T) {
	km := did.KeyMaterial(exampleDID)
	if km == "" {
		t.Error("KeyMaterial returned empty string")
	}
	if km[0] != '6' { // base58btc multibase prefix stripped, key starts with '6'
		t.Errorf("KeyMaterial[0] = %q, expected '6'", km[0])
	}
}

func TestShort(t *testing.T) {
	short := did.Short(exampleDID)
	if len(short) >= len(exampleDID) {
		t.Errorf("Short(%q) = %q, expected shorter", exampleDID, short)
	}
	if short == did.Short("short") {
		t.Error("Short should not truncate strings that are already short")
	}
}

func TestEqual(t *testing.T) {
	if !did.Equal(exampleDID, exampleDID) {
		t.Error("Equal(same, same) should be true")
	}
	if did.Equal(exampleDID, exampleDID+"X") {
		t.Error("Equal(a, a+X) should be false")
	}
}
