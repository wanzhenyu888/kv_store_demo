package raftnode

import (
	"context"
	"errors"
	"fmt"
	"time"

	kv "kv_store_demo"
)

const (
	defaultApplyTimeout   = 3 * time.Second
	defaultBarrierTimeout = 3 * time.Second
)

var ErrNotLeader = errors.New("raft node is not leader")

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

type RaftStoreConfig struct {
	ApplyTimeout   time.Duration
	BarrierTimeout time.Duration
}

type raftNode interface {
	IsLeader() bool
	Leader() (string, string)
	Apply(data []byte, timeout time.Duration) error
	Barrier(timeout time.Duration) error
}

type RaftStore struct {
	node           raftNode
	db             *kv.DB
	applyTimeout   time.Duration
	barrierTimeout time.Duration
}

func NewRaftStore(node *Node, db *kv.DB, config RaftStoreConfig) *RaftStore {
	return newRaftStore(node, db, config)
}

func newRaftStore(node raftNode, db *kv.DB, config RaftStoreConfig) *RaftStore {
	applyTimeout := config.ApplyTimeout
	if applyTimeout <= 0 {
		applyTimeout = defaultApplyTimeout
	}
	barrierTimeout := config.BarrierTimeout
	if barrierTimeout <= 0 {
		barrierTimeout = defaultBarrierTimeout
	}

	return &RaftStore{
		node:           node,
		db:             db,
		applyTimeout:   applyTimeout,
		barrierTimeout: barrierTimeout,
	}
}

func (s *RaftStore) Put(ctx context.Context, key, value []byte) error {
	if len(key) == 0 {
		return kv.ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkLeader(); err != nil {
		return err
	}

	data, err := EncodeCommand(Command{
		Type:  CommandTypePut,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	timeout, err := timeoutFromContext(ctx, s.applyTimeout)
	if err != nil {
		return err
	}
	return s.node.Apply(data, timeout)
}

func (s *RaftStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, kv.ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	timeout, err := timeoutFromContext(ctx, s.barrierTimeout)
	if err != nil {
		return nil, err
	}
	if err := s.node.Barrier(timeout); err != nil {
		return nil, err
	}
	return s.db.Get(key)
}

func (s *RaftStore) Delete(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return kv.ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkLeader(); err != nil {
		return err
	}

	data, err := EncodeCommand(Command{
		Type: CommandTypeDelete,
		Key:  key,
	})
	if err != nil {
		return err
	}

	timeout, err := timeoutFromContext(ctx, s.applyTimeout)
	if err != nil {
		return err
	}
	return s.node.Apply(data, timeout)
}

func (s *RaftStore) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkLeader(); err != nil {
		return err
	}
	return s.db.Flush()
}

func (s *RaftStore) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkLeader(); err != nil {
		return err
	}
	return s.db.Compact()
}

func (s *RaftStore) checkLeader() error {
	if s.node.IsLeader() {
		return nil
	}
	leaderID, leaderAddr := s.node.Leader()
	return &NotLeaderError{
		LeaderID:   leaderID,
		LeaderAddr: leaderAddr,
	}
}

func timeoutFromContext(ctx context.Context, fallback time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return fallback, nil
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return 0, context.DeadlineExceeded
	}
	if remaining < fallback {
		return remaining, nil
	}
	return fallback, nil
}
