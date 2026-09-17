package thread

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func (s *Store) SaveMember(threadID string, member *pb.ThreadMember) error {
	if threadID == "" || member == nil || member.Did == "" || member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_UNSPECIFIED {
		return fmt.Errorf("invalid thread member")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`INSERT OR REPLACE INTO thread_members(thread_id,did,role,joined_epoch) VALUES(?,?,?,?)`, threadID, member.Did, int(member.Role), member.JoinedEpoch); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO thread_membership_epochs(thread_id,membership_epoch) VALUES(?,1)`, threadID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MembershipEpoch(threadID string) (uint64, error) {
	var epoch uint64
	err := s.db.QueryRow(`SELECT membership_epoch FROM thread_membership_epochs WHERE thread_id=?`, threadID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	return epoch, err
}

func (s *Store) AdvanceMembershipEpoch(threadID string) (uint64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var epoch uint64
	err = tx.QueryRow(`SELECT membership_epoch FROM thread_membership_epochs WHERE thread_id=?`, threadID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		epoch = 1
		if _, err = tx.Exec(`INSERT INTO thread_membership_epochs(thread_id,membership_epoch) VALUES(?,?)`, threadID, epoch); err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	} else {
		epoch++
		if _, err = tx.Exec(`UPDATE thread_membership_epochs SET membership_epoch=? WHERE thread_id=?`, epoch, threadID); err != nil {
			return 0, err
		}
	}
	return epoch, tx.Commit()
}

// advanceMembershipEpochTx must be called in the same transaction as the
// member mutation. A membership change is not visible without its new epoch.
func advanceMembershipEpochTx(tx *sql.Tx, threadID string) (uint64, error) {
	var epoch uint64
	err := tx.QueryRow(`SELECT membership_epoch FROM thread_membership_epochs WHERE thread_id=?`, threadID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		// Threads created before membership epochs were introduced start at 1.
		epoch = 1
		if _, err := tx.Exec(`INSERT INTO thread_membership_epochs(thread_id,membership_epoch) VALUES(?,?)`, threadID, epoch); err != nil {
			return 0, err
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	epoch++
	if _, err := tx.Exec(`UPDATE thread_membership_epochs SET membership_epoch=? WHERE thread_id=?`, epoch, threadID); err != nil {
		return 0, err
	}
	return epoch, nil
}

func (s *Store) ListMembers(threadID string) ([]*pb.ThreadMember, error) {
	rows, err := s.db.Query(`SELECT did,role,joined_epoch FROM thread_members WHERE thread_id=? ORDER BY did`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.ThreadMember
	for rows.Next() {
		m := &pb.ThreadMember{}
		var role int
		if err := rows.Scan(&m.Did, &role, &m.JoinedEpoch); err != nil {
			return nil, err
		}
		m.Role = pb.ThreadMemberRole(role)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) RemoveMember(threadID, did string) error {
	_, err := s.RemoveMemberWithEpoch(threadID, did)
	return err
}

// RemoveMemberWithEpoch removes a member and advances the membership epoch in
// one durable transaction. It refuses to remove the last voting member.
func (s *Store) RemoveMemberWithEpoch(threadID, did string) (uint64, error) {
	if threadID == "" || did == "" {
		return 0, fmt.Errorf("invalid thread member")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	var role int
	if err := tx.QueryRow(`SELECT role FROM thread_members WHERE thread_id=? AND did=?`, threadID, did).Scan(&role); err != nil {
		return 0, err
	}
	if pb.ThreadMemberRole(role) == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER || pb.ThreadMemberRole(role) == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN {
		var voters int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM thread_members WHERE thread_id=? AND role IN (?,?)`, threadID, int(pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER), int(pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN)).Scan(&voters); err != nil {
			return 0, err
		}
		if voters <= 1 {
			return 0, fmt.Errorf("cannot remove last voter")
		}
	}
	if _, err := tx.Exec(`DELETE FROM thread_members WHERE thread_id=? AND did=?`, threadID, did); err != nil {
		return 0, err
	}
	epoch, err := advanceMembershipEpochTx(tx, threadID)
	if err != nil {
		return 0, err
	}
	return epoch, tx.Commit()
}

func (s *Store) PromoteMember(threadID, did string) (*pb.ThreadMember, error) {
	member, _, err := s.PromoteMemberWithEpoch(threadID, did)
	return member, err
}

// PromoteMemberWithEpoch promotes an observer after its history catch-up has
// been verified by the caller, atomically advancing the membership epoch.
func (s *Store) PromoteMemberWithEpoch(threadID, did string) (*pb.ThreadMember, uint64, error) {
	if threadID == "" || did == "" {
		return nil, 0, fmt.Errorf("invalid thread member")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	var role int
	var epoch uint64
	if err := tx.QueryRow(`SELECT role,joined_epoch FROM thread_members WHERE thread_id=? AND did=?`, threadID, did).Scan(&role, &epoch); err != nil {
		return nil, 0, err
	}
	if pb.ThreadMemberRole(role) != pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER {
		return nil, 0, fmt.Errorf("only observers may be promoted")
	}
	if _, err := tx.Exec(`UPDATE thread_members SET role=? WHERE thread_id=? AND did=?`, int(pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER), threadID, did); err != nil {
		return nil, 0, err
	}
	membershipEpoch, err := advanceMembershipEpochTx(tx, threadID)
	if err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return &pb.ThreadMember{Did: did, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER, JoinedEpoch: epoch}, membershipEpoch, nil
}

func (s *Store) SaveInvite(threadID, invitee string, role pb.ThreadMemberRole, nonce []byte, expiresAt int64) error {
	if threadID == "" || invitee == "" || role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_UNSPECIFIED || len(nonce) == 0 || expiresAt <= time.Now().UnixMilli() {
		return fmt.Errorf("invalid thread invite")
	}
	_, err := s.db.Exec(`INSERT OR REPLACE INTO thread_invites(thread_id,invitee_did,role,nonce,expires_at,accepted_at) VALUES(?,?,?,?,?,0)`, threadID, invitee, int(role), nonce, expiresAt)
	return err
}

func (s *Store) AcceptInvite(threadID, invitee string, nonce []byte) (*pb.ThreadMember, error) {
	member, _, err := s.AcceptInviteWithEpoch(threadID, invitee, nonce)
	return member, err
}

// AcceptInviteWithEpoch consumes a signed-invite record exactly once and
// makes the observer visible at the new membership epoch.
func (s *Store) AcceptInviteWithEpoch(threadID, invitee string, nonce []byte) (*pb.ThreadMember, uint64, error) {
	if threadID == "" || invitee == "" || len(nonce) == 0 {
		return nil, 0, fmt.Errorf("invalid thread invite")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	var role int
	var expires, accepted int64
	if err := tx.QueryRow(`SELECT role,expires_at,accepted_at FROM thread_invites WHERE thread_id=? AND invitee_did=? AND nonce=?`, threadID, invitee, nonce).Scan(&role, &expires, &accepted); err != nil {
		return nil, 0, err
	}
	if accepted != 0 || expires <= time.Now().UnixMilli() {
		return nil, 0, fmt.Errorf("thread invite expired or already accepted")
	}
	membershipEpoch, err := advanceMembershipEpochTx(tx, threadID)
	if err != nil {
		return nil, 0, err
	}
	member := &pb.ThreadMember{Did: invitee, Role: pb.ThreadMemberRole(role), JoinedEpoch: membershipEpoch}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO thread_members(thread_id,did,role,joined_epoch) VALUES(?,?,?,?)`, threadID, member.Did, int(member.Role), member.JoinedEpoch); err != nil {
		return nil, 0, err
	}
	if _, err := tx.Exec(`UPDATE thread_invites SET accepted_at=? WHERE thread_id=? AND invitee_did=? AND nonce=? AND accepted_at=0`, time.Now().UnixMilli(), threadID, invitee, nonce); err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return member, membershipEpoch, nil
}
