// Package webhook delivers event notifications to a configured HTTP endpoint.
//
// Events are POSTed as JSON to the configured URL with a shared secret in the
// X-MoltMesh-Secret header (when set). Delivery is best-effort with up to 3
// retries and exponential backoff. Failed deliveries are logged and dropped —
// webhooks are not durable.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"go.uber.org/zap"
)

const (
	maxRetries    = 3
	retryBaseWait = 500 * time.Millisecond
	httpTimeout   = 10 * time.Second
)

// EventKind identifies the type of webhook event.
type EventKind string

const (
	EventMessage   EventKind = "message"
	EventTaskEvent EventKind = "task_event"
	EventPubSub    EventKind = "pubsub"
)

// Event is the JSON body POSTed to the webhook URL.
type Event struct {
	Kind      EventKind   `json:"kind"`
	Timestamp int64       `json:"timestamp"` // unix ms
	Data      interface{} `json:"data"`
}

// Dispatcher holds the webhook configuration and delivers events.
type Dispatcher struct {
	mu      sync.RWMutex
	configs map[string]config
	// Legacy default-owner mirror retained for callers/tests that construct a
	// Dispatcher directly. Authenticated owners use configs instead.
	url    string
	secret string
	client *http.Client
	log    *zap.Logger
	exec   *appactors.Executor
}
type config struct{ url, secret string }

func (d *Dispatcher) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	exec, err := appactors.NewExecutor(ctx, h, "webhooks")
	if err != nil {
		return err
	}
	d.exec = exec
	return nil
}

// New creates a Dispatcher. url and secret may be empty (disabled).
func New(log *zap.Logger) *Dispatcher {
	return &Dispatcher{
		client:  &http.Client{Timeout: httpTimeout},
		log:     log,
		configs: make(map[string]config),
	}
}

// Set configures the webhook URL and optional secret.
// Returns an error if the URL points to a private/internal network.
func (d *Dispatcher) Set(rawURL, secret string) error {
	return d.SetForOwner("", rawURL, secret)
}
func (d *Dispatcher) SetForOwner(owner, rawURL, secret string) error {
	if d.exec != nil {
		_, err := d.exec.Call(context.Background(), func() (any, error) { return nil, d.set(owner, rawURL, secret) })
		return err
	}
	return d.set(owner, rawURL, secret)
}
func (d *Dispatcher) set(owner, rawURL, secret string) error {
	if err := validateWebhookURL(rawURL); err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}
	d.mu.Lock()
	if d.configs == nil {
		d.configs = make(map[string]config)
	}
	d.configs[owner] = config{rawURL, secret}
	if owner == "" {
		d.url, d.secret = rawURL, secret
	}
	d.mu.Unlock()
	return nil
}

// validateWebhookURL rejects URLs targeting localhost, private networks,
// link-local, and cloud metadata endpoints.
func validateWebhookURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("scheme %q not allowed, use http or https", u.Scheme)
	}
	host := u.Hostname()

	// Block obvious hostnames
	lower := strings.ToLower(host)
	if lower == "localhost" || lower == "metadata.google.internal" {
		return fmt.Errorf("host %q is not allowed", host)
	}

	// Resolve and check IPs
	ips, err := net.LookupHost(host)
	if err != nil {
		return fmt.Errorf("resolve host %q: %w", host, err)
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("host %q resolves to private/internal IP %s", host, ipStr)
		}
		// Block cloud metadata (169.254.169.254)
		if ip.Equal(net.ParseIP("169.254.169.254")) {
			return fmt.Errorf("host %q resolves to cloud metadata IP", host)
		}
	}
	return nil
}

// Clear disables webhook delivery.
func (d *Dispatcher) Clear() {
	d.ClearForOwner("")
}
func (d *Dispatcher) ClearForOwner(owner string) {
	if d.exec != nil {
		_, _ = d.exec.Call(context.Background(), func() (any, error) { d.clear(owner); return nil, nil })
		return
	}
	d.clear(owner)
}
func (d *Dispatcher) clear(owner string) {
	d.mu.Lock()
	delete(d.configs, owner)
	if owner == "" {
		d.url, d.secret = "", ""
	}
	d.mu.Unlock()
}

// URL returns the currently configured webhook URL (empty if disabled).
func (d *Dispatcher) URL() string {
	return d.URLForOwner("")
}
func (d *Dispatcher) URLForOwner(owner string) string {
	if d.exec != nil {
		value, err := d.exec.Call(context.Background(), func() (any, error) { return d.currentURL(owner), nil })
		if err == nil {
			return value.(string)
		}
		return ""
	}
	return d.currentURL(owner)
}
func (d *Dispatcher) currentURL(owner string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if cfg, ok := d.configs[owner]; ok {
		return cfg.url
	}
	if owner == "" {
		return d.url
	}
	return ""
}

// Send dispatches an event asynchronously.
func (d *Dispatcher) Send(kind EventKind, data interface{}) {
	d.SendForOwner("", kind, data)
}
func (d *Dispatcher) SendForOwner(owner string, kind EventKind, data interface{}) {
	if d.exec != nil {
		_ = d.exec.Cast(context.Background(), func() (any, error) { d.send(owner, kind, data); return nil, nil })
		return
	}
	d.send(owner, kind, data)
}
func (d *Dispatcher) send(owner string, kind EventKind, data interface{}) {
	d.mu.RLock()
	cfg, ok := d.configs[owner]
	url, secret := cfg.url, cfg.secret
	if !ok && owner == "" {
		url, secret = d.url, d.secret
	}
	d.mu.RUnlock()

	if url == "" {
		return
	}

	event := Event{
		Kind:      kind,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}

	d.deliver(context.Background(), url, secret, event)
}

func (d *Dispatcher) deliver(ctx context.Context, url, secret string, event Event) {
	body, err := json.Marshal(event)
	if err != nil {
		d.log.Error("webhook marshal", zap.Error(err))
		return
	}

	wait := retryBaseWait
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(wait):
				wait *= 2
			case <-ctx.Done():
				return
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			d.log.Error("webhook build request", zap.Error(err))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-MoltMesh-Event", string(event.Kind))
		if secret != "" {
			req.Header.Set("X-MoltMesh-Secret", secret)
		}

		resp, err := d.client.Do(req)
		if err != nil {
			d.log.Warn("webhook delivery failed", zap.String("url", url), zap.Int("attempt", attempt+1), zap.Error(err))
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			d.log.Debug("webhook delivered", zap.String("url", url), zap.String("kind", string(event.Kind)))
			return
		}
		d.log.Warn("webhook non-2xx", zap.String("url", url), zap.Int("status", resp.StatusCode), zap.Int("attempt", attempt+1))
	}

	d.log.Error("webhook delivery exhausted retries",
		zap.String("url", url),
		zap.String("kind", string(event.Kind)),
		zap.Int("max_retries", maxRetries),
		zap.String("hint", fmt.Sprintf("check that %s is reachable and returns 2xx", url)),
	)
}
