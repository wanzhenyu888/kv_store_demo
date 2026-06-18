package raftnode

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	kv "kv_store_demo"
	"kv_store_demo/internal/engine/record"
	"kv_store_demo/internal/platform/kv_errors"

	"github.com/hashicorp/raft"
)

type memorySnapshotSink struct {
	bytes.Buffer
	closed   bool
	canceled bool
}

func (s *memorySnapshotSink) ID() string {
	return "memory"
}

func (s *memorySnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *memorySnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}

func openTestFSM(t *testing.T) (*FSM, *kv.DB) {
	t.Helper()

	db, err := kv.Open(kv.Options{
		Dir:          t.TempDir(),
		MemTableSize: kv.DefaultMemTableSize,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	return NewFSM(db), db
}

func applyCommand(t *testing.T, fsm *FSM, cmd Command) interface{} {
	t.Helper()

	encoded, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	return fsm.Apply(&raft.Log{Data: encoded})
}

func requireApplyOK(t *testing.T, resp interface{}) {
	t.Helper()

	if resp != nil {
		t.Fatalf("Apply() response = %v, want nil", resp)
	}
}

func requireApplyError(t *testing.T, resp interface{}, want error) {
	t.Helper()

	err, ok := resp.(error)
	if !ok {
		t.Fatalf("Apply() response = %T(%v), want error %v", resp, resp, want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("Apply() error = %v, want %v", err, want)
	}
}

func TestFSMApplyPut(t *testing.T) {
	fsm, db := openTestFSM(t)
	defer db.Close()

	requireApplyOK(t, applyCommand(t, fsm, Command{
		Type:  CommandTypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	}))

	got, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get() = %q, want %q", got, "alice")
	}
}

func TestFSMApplyDelete(t *testing.T) {
	fsm, db := openTestFSM(t)
	defer db.Close()

	requireApplyOK(t, applyCommand(t, fsm, Command{
		Type:  CommandTypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	}))
	requireApplyOK(t, applyCommand(t, fsm, Command{
		Type: CommandTypeDelete,
		Key:  []byte("name"),
	}))

	got, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get() after Delete error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get() after Delete = %q, want empty value", got)
	}
}

func TestFSMApplyRejectsIncompleteCommand(t *testing.T) {
	fsm, db := openTestFSM(t)
	defer db.Close()

	resp := fsm.Apply(&raft.Log{Data: []byte{CommandTypePut}})
	requireApplyError(t, resp, kv_errors.ErrIncompleteRecord)
}

func TestFSMApplyRejectsInvalidCommandType(t *testing.T) {
	fsm, db := openTestFSM(t)
	defer db.Close()

	data := make([]byte, record.RecordHeaderSize+len("name"))
	data[0] = 99
	binary.BigEndian.PutUint32(data[1:5], uint32(len("name")))
	copy(data[record.RecordHeaderSize:], []byte("name"))

	resp := fsm.Apply(&raft.Log{Data: data})
	requireApplyError(t, resp, kv_errors.ErrInvalidType)
}

func TestFSMSnapshotAndRestore(t *testing.T) {
	fsm, db := openTestFSM(t)
	defer db.Close()

	requireApplyOK(t, applyCommand(t, fsm, Command{
		Type:  CommandTypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	}))
	requireApplyOK(t, applyCommand(t, fsm, Command{
		Type:  CommandTypePut,
		Key:   []byte("city"),
		Value: []byte("paris"),
	}))
	requireApplyOK(t, applyCommand(t, fsm, Command{
		Type: CommandTypeDelete,
		Key:  []byte("city"),
	}))

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	defer snapshot.Release()

	sink := &memorySnapshotSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}
	if !sink.closed {
		t.Fatal("Persist() did not close sink")
	}
	if sink.canceled {
		t.Fatal("Persist() canceled sink")
	}

	restoreFSM, restoreDB := openTestFSM(t)
	defer restoreDB.Close()
	requireApplyOK(t, applyCommand(t, restoreFSM, Command{
		Type:  CommandTypePut,
		Key:   []byte("stale"),
		Value: []byte("value"),
	}))

	if err := restoreFSM.Restore(ioNopCloser{Reader: bytes.NewReader(sink.Bytes())}); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	got, err := restoreDB.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get(name) error = %v", err)
	}
	if !bytes.Equal(got, []byte("alice")) {
		t.Fatalf("Get(name) = %q, want alice", got)
	}
	got, err = restoreDB.Get([]byte("city"))
	if err != nil {
		t.Fatalf("Get(city) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get(city) = %q, want missing", got)
	}
	got, err = restoreDB.Get([]byte("stale"))
	if err != nil {
		t.Fatalf("Get(stale) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get(stale) = %q, want missing", got)
	}
}

type ioNopCloser struct {
	*bytes.Reader
}

func (c ioNopCloser) Close() error {
	return nil
}
