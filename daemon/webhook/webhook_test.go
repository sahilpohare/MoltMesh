package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func newDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	log, _ := zap.NewDevelopment()
	return New(log)
}

// ─── validateWebhookURL ───────────────────────────────────────────────────────

func TestValidateURL_LocalhostRejected(t *testing.T) {
	for _, u := range []string{
		"http://localhost/hook",
		"http://localhost:8080/hook",
		"https://localhost/hook",
	} {
		if err := validateWebhookURL(u); err == nil {
			t.Errorf("expected rejection of %q, got nil", u)
		}
	}
}

func TestValidateURL_BadScheme(t *testing.T) {
	for _, u := range []string{
		"ftp://example.com/hook",
		"file:///etc/passwd",
		"ws://example.com/hook",
	} {
		if err := validateWebhookURL(u); err == nil {
			t.Errorf("expected rejection of scheme in %q, got nil", u)
		}
	}
}

func TestValidateURL_LoopbackIPv4Rejected(t *testing.T) {
	// 127.0.0.1 is loopback — should always be blocked.
	if err := validateWebhookURL("http://127.0.0.1/hook"); err == nil {
		t.Error("expected rejection of 127.0.0.1")
	}
}

func TestValidateURL_LoopbackIPv6Rejected(t *testing.T) {
	if err := validateWebhookURL("http://[::1]/hook"); err == nil {
		t.Error("expected rejection of ::1")
	}
}

func TestValidateURL_PrivateRFCRejected(t *testing.T) {
	for _, u := range []string{
		"http://10.0.0.1/hook",
		"http://192.168.1.1/hook",
		"http://172.16.0.1/hook",
	} {
		if err := validateWebhookURL(u); err == nil {
			t.Errorf("expected rejection of private IP in %q, got nil", u)
		}
	}
}

func TestValidateURL_MetadataEndpointRejected(t *testing.T) {
	if err := validateWebhookURL("http://169.254.169.254/latest/meta-data/"); err == nil {
		t.Error("expected rejection of cloud metadata IP")
	}
}

func TestValidateURL_GoogleMetadataHostRejected(t *testing.T) {
	if err := validateWebhookURL("http://metadata.google.internal/"); err == nil {
		t.Error("expected rejection of metadata.google.internal")
	}
}

func TestValidateURL_MalformedRejected(t *testing.T) {
	if err := validateWebhookURL("not a url"); err == nil {
		t.Error("expected rejection of malformed URL")
	}
}

// ─── Dispatcher.Set / Clear / URL ─────────────────────────────────────────────

func TestSet_ValidURL(t *testing.T) {
	// Use a real httptest server so the host resolves to a public-ish IP (127.0.0.1
	// would be rejected, but we can test Set separately via an allowed server).
	// For purely unit-testing Set/Clear/URL we bypass validateWebhookURL by using
	// an httptest.Server and then overriding the URL field directly.
	d := newDispatcher(t)
	d.mu.Lock()
	d.url = "https://example.com/hook"
	d.secret = "s3cr3t"
	d.mu.Unlock()

	if got := d.URL(); got != "https://example.com/hook" {
		t.Errorf("URL: got %q", got)
	}
}

func TestClear_ResetsURL(t *testing.T) {
	d := newDispatcher(t)
	d.mu.Lock()
	d.url = "https://example.com/hook"
	d.secret = "secret"
	d.mu.Unlock()

	d.Clear()
	if d.URL() != "" {
		t.Error("expected empty URL after Clear")
	}
}

func TestURL_EmptyByDefault(t *testing.T) {
	d := newDispatcher(t)
	if d.URL() != "" {
		t.Errorf("expected empty URL on fresh Dispatcher, got %q", d.URL())
	}
}

// ─── Dispatcher.Send / deliver ────────────────────────────────────────────────

// startWebhookServer returns an httptest.Server that records received events.
func startWebhookServer(t *testing.T, statusCode int, received *[]Event) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e Event
		if err := json.NewDecoder(r.Body).Decode(&e); err == nil {
			*received = append(*received, e)
		}
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestSend_DeliveredToServer(t *testing.T) {
	var received []Event
	ts := startWebhookServer(t, http.StatusOK, &received)

	d := newDispatcher(t)
	d.mu.Lock()
	d.url = ts.URL + "/webhook"
	d.mu.Unlock()

	d.Send(EventMessage, map[string]string{"text": "hello"})

	// Allow async delivery to complete.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(received) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 event, got %d", len(received))
	}
	if received[0].Kind != EventMessage {
		t.Errorf("kind mismatch: %q", received[0].Kind)
	}
	if received[0].Timestamp == 0 {
		t.Error("expected non-zero Timestamp")
	}
}

func TestSend_SecretHeader(t *testing.T) {
	var secretHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secretHeader = r.Header.Get("X-MoltMesh-Secret")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	d := newDispatcher(t)
	d.mu.Lock()
	d.url = ts.URL + "/hook"
	d.secret = "my-secret"
	d.mu.Unlock()

	d.Send(EventPubSub, "data")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if secretHeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if secretHeader != "my-secret" {
		t.Errorf("X-MoltMesh-Secret: got %q, want %q", secretHeader, "my-secret")
	}
}

func TestSend_EventKindHeader(t *testing.T) {
	var kindHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kindHeader = r.Header.Get("X-MoltMesh-Event")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	d := newDispatcher(t)
	d.mu.Lock()
	d.url = ts.URL + "/hook"
	d.mu.Unlock()

	d.Send(EventTaskEvent, nil)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if kindHeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if kindHeader != string(EventTaskEvent) {
		t.Errorf("X-MoltMesh-Event: got %q", kindHeader)
	}
}

func TestSend_Disabled_NoRequest(t *testing.T) {
	var hitCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hitCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	d := newDispatcher(t)
	// Do NOT set a URL — dispatcher is disabled.

	d.Send(EventMessage, "test")
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&hitCount) != 0 {
		t.Error("expected no HTTP request when dispatcher URL is empty")
	}
}

func TestDeliver_RetryOn5xx(t *testing.T) {
	var attempts int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < int32(maxRetries) {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(ts.Close)

	log, _ := zap.NewDevelopment()
	d := &Dispatcher{
		// Short retry to keep the test fast.
		client: &http.Client{Timeout: httpTimeout},
		log:    log,
		url:    ts.URL + "/hook",
	}

	event := Event{Kind: EventMessage, Timestamp: time.Now().UnixMilli(), Data: "x"}
	d.deliver(context.Background(), ts.URL+"/hook", "", event)

	if got := atomic.LoadInt32(&attempts); got != int32(maxRetries) {
		t.Errorf("expected %d attempts, got %d", maxRetries, got)
	}
}

func TestDeliver_ExhaustsRetries(t *testing.T) {
	var attempts int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(ts.Close)

	log, _ := zap.NewDevelopment()
	d := &Dispatcher{
		client: &http.Client{Timeout: httpTimeout},
		log:    log,
	}

	event := Event{Kind: EventMessage, Timestamp: time.Now().UnixMilli(), Data: "fail"}
	d.deliver(context.Background(), ts.URL+"/hook", "", event)

	if got := atomic.LoadInt32(&attempts); got != int32(maxRetries) {
		t.Errorf("expected %d attempts on exhaustion, got %d", maxRetries, got)
	}
}

func TestDeliver_ContentTypeJSON(t *testing.T) {
	var contentType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	log, _ := zap.NewDevelopment()
	d := &Dispatcher{client: &http.Client{Timeout: httpTimeout}, log: log}
	event := Event{Kind: EventPubSub, Timestamp: 1}
	d.deliver(context.Background(), ts.URL+"/hook", "", event)

	if !strings.HasPrefix(contentType, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", contentType)
	}
}
