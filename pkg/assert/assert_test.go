package assert

import "testing"

func TestThat_PassesOnTrue(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	That(true, "should not fire")
}

func TestThat_PanicsOnFalse(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on false condition")
		}
	}()
	That(1 == 2, "1 does not equal %d", 2)
}
