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
	mu     sync.RWMutex
	url    string
	secret string
	client *http.Client
	log    *zap.Logger
}

// New creates a Dispatcher. url and secret may be empty (disabled).
func New(log *zap.Logger) *Dispatcher {
	return &Dispatcher{
		client: &http.Client{Timeout: httpTimeout},
		log:    log,
	}
}

// Set configures the webhook URL and optional secret.
// Returns an error if the URL points to a private/internal network.
func (d *Dispatcher) Set(rawURL, secret string) error {
	if err := validateWebhookURL(rawURL); err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}
	d.mu.Lock()
	d.url = rawURL
	d.secret = secret
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
	d.mu.Lock()
	d.url = ""
	d.secret = ""
	d.mu.Unlock()
}

// URL returns the currently configured webhook URL (empty if disabled).
func (d *Dispatcher) URL() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.url
}

// Send dispatches an event asynchronously.
func (d *Dispatcher) Send(kind EventKind, data interface{}) {
	d.mu.RLock()
	url := d.url
	secret := d.secret
	d.mu.RUnlock()

	if url == "" {
		return
	}

	event := Event{
		Kind:      kind,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}

	go d.deliver(context.Background(), url, secret, event)
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
