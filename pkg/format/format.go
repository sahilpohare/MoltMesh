// Package format provides human-readable display helpers for MoltMesh types.
// All functions are pure — no I/O, no side effects.
package format

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/capability"
	"github.com/sahilpohare/p2p-a2a/pkg/did"
)

// ── DID ───────────────────────────────────────────────────────────────────────

// DID returns a short display form of a DID.
//   "did:key:z6MkhaX…r2jP"
func DID(d string) string { return did.Short(d) }

// DIDFull returns the full DID unchanged.
func DIDFull(d string) string { return d }

// ── Capability ────────────────────────────────────────────────────────────────

// Capability returns the short name for display (e.g. "text-generation").
func Capability(capID string) string { return capability.Short(capID) }

// ── Time ──────────────────────────────────────────────────────────────────────

// UnixMs formats a Unix millisecond timestamp as "2006-01-02 15:04:05 UTC".
// Returns "-" if ms is 0.
func UnixMs(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05 UTC")
}

// UnixMsAgo formats a Unix millisecond timestamp as a relative age: "3m ago", "2h ago", etc.
// Returns "-" if ms is 0.
func UnixMsAgo(ms int64) string {
	if ms == 0 {
		return "-"
	}
	d := time.Since(time.UnixMilli(ms))
	return DurationShort(d) + " ago"
}

// DurationShort formats a duration as "5s", "3m", "2h", "1d".
func DurationShort(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// Uptime formats seconds as "2d 3h 4m 5s".
func Uptime(secs int64) string {
	d := time.Duration(secs) * time.Second
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	var b strings.Builder
	if days > 0 {
		fmt.Fprintf(&b, "%dd ", days)
	}
	if hours > 0 || days > 0 {
		fmt.Fprintf(&b, "%dh ", hours)
	}
	if mins > 0 || hours > 0 || days > 0 {
		fmt.Fprintf(&b, "%dm ", mins)
	}
	fmt.Fprintf(&b, "%ds", s)
	return b.String()
}

// ── Enums ─────────────────────────────────────────────────────────────────────

// TaskStatus returns a short colored-friendly label for a task status.
func TaskStatus(s pb.TaskStatus) string {
	switch s {
	case pb.TaskStatus_TASK_STATUS_SUBMITTED:
		return "submitted"
	case pb.TaskStatus_TASK_STATUS_WORKING:
		return "working"
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		return "completed"
	case pb.TaskStatus_TASK_STATUS_FAILED:
		return "failed"
	case pb.TaskStatus_TASK_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "unknown"
	}
}

// MessageKind returns a short label for a message kind.
func MessageKind(k pb.MessageKind) string {
	switch k {
	case pb.MessageKind_MESSAGE_KIND_TEXT:
		return "text"
	case pb.MessageKind_MESSAGE_KIND_TASK_REQUEST:
		return "task-request"
	case pb.MessageKind_MESSAGE_KIND_TASK_EVENT:
		return "task-event"
	case pb.MessageKind_MESSAGE_KIND_TASK_RESULT:
		return "task-result"
	case pb.MessageKind_MESSAGE_KIND_TASK_CANCEL:
		return "task-cancel"
	case pb.MessageKind_MESSAGE_KIND_ACK:
		return "ack"
	default:
		return "unknown"
	}
}

// EventKind returns a short label for an event kind.
func EventKind(k pb.EventKind) string {
	switch k {
	case pb.EventKind_EVENT_KIND_TOKEN_CHUNK:
		return "token"
	case pb.EventKind_EVENT_KIND_TOOL_CALL:
		return "tool-call"
	case pb.EventKind_EVENT_KIND_TOOL_RESULT:
		return "tool-result"
	case pb.EventKind_EVENT_KIND_STATUS_UPDATE:
		return "status"
	case pb.EventKind_EVENT_KIND_ARTIFACT:
		return "artifact"
	case pb.EventKind_EVENT_KIND_DONE:
		return "done"
	case pb.EventKind_EVENT_KIND_ERROR:
		return "error"
	default:
		return "unknown"
	}
}

// ── Multiaddr ─────────────────────────────────────────────────────────────────

// Multiaddr shortens a multiaddr string for display.
// "/ip4/192.168.1.5/udp/9000/quic-v1" → "192.168.1.5:9000/quic-v1"
// "/ip6/::1/tcp/9000"                 → "[::1]:9000/tcp"
func Multiaddr(ma string) string {
	parts := strings.Split(strings.TrimPrefix(ma, "/"), "/")
	if len(parts) < 4 {
		return ma
	}
	proto, addr, portProto, port := parts[0], parts[1], parts[2], parts[3]
	transport := portProto
	if len(parts) > 4 {
		transport = strings.Join(parts[4:], "/")
		if transport == "" {
			transport = portProto
		}
	}
	switch proto {
	case "ip6":
		return fmt.Sprintf("[%s]:%s/%s", addr, port, transport)
	default:
		return fmt.Sprintf("%s:%s/%s", addr, port, transport)
	}
}

// ── Bytes ─────────────────────────────────────────────────────────────────────

// Bytes formats a byte count as "1.2 KB", "3.4 MB", etc.
func Bytes(n int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case n < KB:
		return fmt.Sprintf("%d B", n)
	case n < MB:
		return fmt.Sprintf("%.1f KB", float64(n)/KB)
	case n < GB:
		return fmt.Sprintf("%.1f MB", float64(n)/MB)
	default:
		return fmt.Sprintf("%.2f GB", float64(n)/GB)
	}
}

// ── Table ─────────────────────────────────────────────────────────────────────

// Table renders a slice of rows as aligned columns.
// header is the column titles; rows is the data.
// Each row must have len(header) columns.
func Table(header []string, rows [][]string) string {
	if len(header) == 0 {
		return ""
	}
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = utf8.RuneCountInString(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				if w := utf8.RuneCountInString(cell); w > widths[i] {
					widths[i] = w
				}
			}
		}
	}

	var b strings.Builder
	writeRow := func(cols []string) {
		for i, col := range cols {
			if i > 0 {
				b.WriteString("  ")
			}
			b.WriteString(col)
			pad := widths[i] - utf8.RuneCountInString(col)
			if i < len(cols)-1 && pad > 0 {
				b.WriteString(strings.Repeat(" ", pad))
			}
		}
		b.WriteByte('\n')
	}

	writeRow(header)
	// separator line
	sep := make([]string, len(header))
	for i, w := range widths {
		sep[i] = strings.Repeat("─", w)
	}
	writeRow(sep)
	for _, row := range rows {
		writeRow(row)
	}
	return b.String()
}

// ── Message / Task display ────────────────────────────────────────────────────

// Message returns a single-line summary of a message.
func Message(m *pb.Message) string {
	return fmt.Sprintf("[%s]  %s → %s  %-12s  %s",
		m.Id[:8],
		DID(m.FromDid),
		DID(m.ToDid),
		MessageKind(m.Kind),
		UnixMsAgo(m.SentAt),
	)
}

// Task returns a single-line summary of a task.
func Task(t *pb.Task) string {
	return fmt.Sprintf("[%s]  %-10s  %-18s  %s → %s  %s",
		t.Id[:8],
		TaskStatus(t.Status),
		Capability(t.Skill),
		DID(t.Initiator),
		DID(t.Assignee),
		UnixMsAgo(t.CreatedAt),
	)
}

// AgentCard returns a single-line summary of an agent card.
func AgentCard(c *pb.AgentCard) string {
	skills := make([]string, 0, len(c.Skills))
	for _, s := range c.Skills {
		skills = append(skills, Capability(s.Id))
	}
	skillStr := strings.Join(skills, ", ")
	if skillStr == "" {
		skillStr = "-"
	}
	return fmt.Sprintf("%-20s  %s  [%s]", c.Name, DID(c.Did), skillStr)
}
