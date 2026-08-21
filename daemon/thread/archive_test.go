package thread

import "testing"

func TestArchiveProviderCIDStableAndScoped(t *testing.T) {
	a, err := ArchiveProviderCID("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ArchiveProviderCID("thread-a")
	if err != nil {
		t.Fatal(err)
	}
	c, err := ArchiveProviderCID("thread-b")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a == c {
		t.Fatalf("archive CIDs not stable/scoped: %s %s %s", a, b, c)
	}
	if _, err := ArchiveProviderCID(""); err == nil {
		t.Fatal("empty thread ID accepted")
	}
}
