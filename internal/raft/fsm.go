package raftnode

import (
	"bytes"
	"io"

	kv "kv_store_demo"
	"kv_store_demo/internal/platform/kv_errors"

	"github.com/hashicorp/raft"
)

type FSM struct {
	db *kv.DB
}

type fsmSnapshot struct {
	data []byte
}

func NewFSM(db *kv.DB) *FSM {
	return &FSM{db: db}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		return err
	}

	switch cmd.Type {
	case CommandTypePut:
		return f.db.Put(cmd.Key, cmd.Value)
	case CommandTypeDelete:
		return f.db.Delete(cmd.Key)
	default:
		return kv_errors.ErrInvalidType
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	var buf bytes.Buffer
	if err := f.db.CreateSnapshot(&buf); err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: append([]byte(nil), buf.Bytes()...)}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.db.RestoreSnapshot(rc)
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
