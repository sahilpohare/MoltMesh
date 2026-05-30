// Package network manages named agent groups (networks).
//
// A network is a named group of agents that can exchange broadcast messages.
// Membership is tracked locally in SQLite. Messages are broadcast via GossipSub
// on the topic "a2a/networks/<id>/broadcast".
package network

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"
)

// Network describes a named agent group.
type Network struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	CreatorDID string           `json:"creator_did"`
	CreatedAt int64             `json:"created_at"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Member is an agent belonging to a network.
type Member struct {
	NetworkID string `json:"network_id"`
	DID       string `json:"did"`
	JoinedAt  int64  `json:"joined_at"`
}

// Store persists network membership to SQLite.
type Store struct {
	db *sql.DB
}

// New opens (or creates) the network store at the given path.
func New(path string) (*Store, error) {
	db, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open network db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate network db: %w", err)
	}
	return &Store{db: db}, nil
}

// Create creates a new network with the given creator.
func (s *Store) Create(name, creatorDID string, meta map[string]string) (*Network, error) {
	id := uuid.New().String()
	now := time.Now().UnixMilli()
	_, err := s.db.Exec(
		`INSERT INTO networks (id, name, creator_did, created_at) VALUES (?, ?, ?, ?)`,
		id, name, creatorDID, now,
	)
	if err != nil {
		return nil, err
	}
	// Creator is automatically a member.
	if err := s.addMember(id, creatorDID, now); err != nil {
		return nil, err
	}
	return s.Get(id)
}

// Get fetches a network by ID.
func (s *Store) Get(id string) (*Network, error) {
	row := s.db.QueryRow(
		`SELECT id, name, creator_did, created_at FROM networks WHERE id = ?`, id,
	)
	var n Network
	if err := row.Scan(&n.ID, &n.Name, &n.CreatorDID, &n.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("network %q not found", id)
		}
		return nil, err
	}
	return &n, nil
}

// GetByName fetches a network by name.
func (s *Store) GetByName(name string) (*Network, error) {
	row := s.db.QueryRow(
		`SELECT id, name, creator_did, created_at FROM networks WHERE name = ?`, name,
	)
	var n Network
	if err := row.Scan(&n.ID, &n.Name, &n.CreatorDID, &n.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("network %q not found", name)
		}
		return nil, err
	}
	return &n, nil
}

// List returns all networks the given DID is a member of.
func (s *Store) List(memberDID string) ([]*Network, error) {
	rows, err := s.db.Query(`
		SELECT n.id, n.name, n.creator_did, n.created_at
		FROM networks n
		JOIN network_members m ON n.id = m.network_id
		WHERE m.did = ?
		ORDER BY n.created_at DESC`, memberDID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nets []*Network
	for rows.Next() {
		var n Network
		if err := rows.Scan(&n.ID, &n.Name, &n.CreatorDID, &n.CreatedAt); err != nil {
			return nil, err
		}
		nets = append(nets, &n)
	}
	return nets, rows.Err()
}

// Join adds a DID to a network. Idempotent.
func (s *Store) Join(networkID, did string) error {
	net, err := s.Get(networkID)
	if err != nil {
		return err
	}
	_ = net
	return s.addMember(networkID, did, time.Now().UnixMilli())
}

// Leave removes a DID from a network.
func (s *Store) Leave(networkID, did string) error {
	_, err := s.db.Exec(
		`DELETE FROM network_members WHERE network_id = ? AND did = ?`, networkID, did,
	)
	return err
}

// Members returns all members of a network.
func (s *Store) Members(networkID string) ([]*Member, error) {
	rows, err := s.db.Query(
		`SELECT network_id, did, joined_at FROM network_members WHERE network_id = ? ORDER BY joined_at`,
		networkID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []*Member
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.NetworkID, &m.DID, &m.JoinedAt); err != nil {
			return nil, err
		}
		members = append(members, &m)
	}
	return members, rows.Err()
}

// IsMember reports whether a DID is a member of the network.
func (s *Store) IsMember(networkID, did string) (bool, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM network_members WHERE network_id = ? AND did = ?`, networkID, did,
	).Scan(&count)
	return count > 0, err
}

// BroadcastTopic returns the GossipSub topic for broadcasting to a network.
func BroadcastTopic(networkID string) string {
	return "a2a/networks/" + networkID + "/broadcast"
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// ─── internal ────────────────────────────────────────────────────────────────

func (s *Store) addMember(networkID, did string, now int64) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO network_members (network_id, did, joined_at) VALUES (?, ?, ?)`,
		networkID, did, now,
	)
	return err
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS networks (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL UNIQUE,
			creator_did TEXT NOT NULL,
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS network_members (
			network_id TEXT NOT NULL,
			did        TEXT NOT NULL,
			joined_at  INTEGER NOT NULL,
			PRIMARY KEY (network_id, did),
			FOREIGN KEY (network_id) REFERENCES networks(id) ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_members_did     ON network_members(did);
		CREATE INDEX IF NOT EXISTS idx_members_network ON network_members(network_id);
	`)
	return err
}

// Manager wraps Store with gossip-backed broadcast.
type Manager struct {
	store     *Store
	publisher interface {
		Publish(ctx context.Context, topic string, data []byte) error
		SubscribeTopic(ctx context.Context, topic string) (<-chan []byte, func(), error)
	}
}

// NewManager creates a network Manager.
func NewManager(store *Store, publisher interface {
	Publish(ctx context.Context, topic string, data []byte) error
	SubscribeTopic(ctx context.Context, topic string) (<-chan []byte, func(), error)
}) *Manager {
	return &Manager{store: store, publisher: publisher}
}

// Store returns the underlying Store (for direct access in RPC handlers).
func (m *Manager) Store() *Store { return m.store }

// Broadcast publishes a message to all members of the network via GossipSub.
func (m *Manager) Broadcast(ctx context.Context, networkID string, data []byte) error {
	return m.publisher.Publish(ctx, BroadcastTopic(networkID), data)
}

// SubscribeBroadcast subscribes to broadcast messages for a network.
func (m *Manager) SubscribeBroadcast(ctx context.Context, networkID string) (<-chan []byte, func(), error) {
	return m.publisher.SubscribeTopic(ctx, BroadcastTopic(networkID))
}
