package client

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const notLeaderMessagePrefix = "raft node is not leader"

var ErrNotLeader = errors.New(notLeaderMessagePrefix)

type NotLeaderError struct {
	LeaderID   string
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" && e.LeaderAddr == "" {
		return ErrNotLeader.Error()
	}
	return fmt.Sprintf("%s: leader id=%q addr=%q", ErrNotLeader, e.LeaderID, e.LeaderAddr)
}

func (e *NotLeaderError) Unwrap() error {
	return ErrNotLeader
}

func IsNotLeader(err error) bool {
	return errors.Is(err, ErrNotLeader)
}

func LeaderHint(err error) (string, string, bool) {
	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		return "", "", false
	}
	return notLeader.LeaderID, notLeader.LeaderAddr, true
}

func mapError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return err
	}

	message := st.Message()
	if !strings.HasPrefix(message, notLeaderMessagePrefix) {
		return err
	}

	leaderID, leaderAddr := parseLeaderHint(message)
	return &NotLeaderError{
		LeaderID:   leaderID,
		LeaderAddr: leaderAddr,
	}
}

func parseLeaderHint(message string) (string, string) {
	leaderID := parseQuotedValue(message, "leader id=")
	leaderAddr := parseQuotedValue(message, "addr=")
	return leaderID, leaderAddr
}

func parseQuotedValue(message string, marker string) string {
	start := strings.Index(message, marker)
	if start < 0 {
		return ""
	}
	valueStart := start + len(marker)
	if valueStart >= len(message) || message[valueStart] != '"' {
		return ""
	}
	valueStart++

	valueEnd := strings.IndexByte(message[valueStart:], '"')
	if valueEnd < 0 {
		return ""
	}
	return message[valueStart : valueStart+valueEnd]
}
