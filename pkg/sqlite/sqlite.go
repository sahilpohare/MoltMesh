package sqlite

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"
)

// Open opens a SQLite database with WAL mode and a busy timeout.
// MaxOpenConns is set to 1 so Go's connection pool serialises all writes
// at the application level, eliminating "database is locked" errors that
// arise when multiple goroutines attempt concurrent SQLite writes.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path+"?_journal=WAL&_busy_timeout=5000&_sync=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("open sqlite db %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// ScanProtos scans a single []byte column from rows into a slice of protobuf messages.
// The newMsg function should return a newly allocated instance of the message type.
func ScanProtos[T proto.Message](rows *sql.Rows, newMsg func() T) ([]T, error) {
	var msgs []T
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		msg := newMsg()
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, rows.Err()
}
