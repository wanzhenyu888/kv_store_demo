package raftnode

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"kv_store_demo/internal/engine/record"
	"kv_store_demo/internal/platform/kv_errors"
)

func TestEncodeDecodePutCommand(t *testing.T) {
	input := Command{
		Type:  CommandTypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	}

	encoded, err := EncodeCommand(input)
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	got, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand() error = %v", err)
	}

	if got.Type != input.Type || !bytes.Equal(got.Key, input.Key) || !bytes.Equal(got.Value, input.Value) {
		t.Fatalf("DecodeCommand() = %+v, want %+v", got, input)
	}
}

func TestEncodeDecodeDeleteCommand(t *testing.T) {
	input := Command{
		Type: CommandTypeDelete,
		Key:  []byte("name"),
	}

	encoded, err := EncodeCommand(input)
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	got, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand() error = %v", err)
	}

	if got.Type != input.Type || !bytes.Equal(got.Key, input.Key) || len(got.Value) != 0 {
		t.Fatalf("DecodeCommand() = %+v, want delete command with empty value", got)
	}
}

func TestEncodeCommandRejectsEmptyKey(t *testing.T) {
	_, err := EncodeCommand(Command{Type: CommandTypePut, Value: []byte("alice")})
	if !errors.Is(err, kv_errors.ErrEmptyKey) {
		t.Fatalf("EncodeCommand() error = %v, want ErrEmptyKey", err)
	}
}

func TestEncodeCommandRejectsInvalidType(t *testing.T) {
	_, err := EncodeCommand(Command{Type: 99, Key: []byte("name"), Value: []byte("alice")})
	if !errors.Is(err, kv_errors.ErrInvalidType) {
		t.Fatalf("EncodeCommand() error = %v, want ErrInvalidType", err)
	}
}

func TestEncodeCommandRejectsDeleteWithValue(t *testing.T) {
	_, err := EncodeCommand(Command{
		Type:  CommandTypeDelete,
		Key:   []byte("name"),
		Value: []byte("alice"),
	})
	if !errors.Is(err, kv_errors.ErrUnexpectedValue) {
		t.Fatalf("EncodeCommand() error = %v, want ErrUnexpectedValue", err)
	}
}

func TestDecodeCommandRejectsIncompleteData(t *testing.T) {
	_, err := DecodeCommand([]byte{CommandTypePut})
	if !errors.Is(err, kv_errors.ErrIncompleteRecord) {
		t.Fatalf("DecodeCommand() error = %v, want ErrIncompleteRecord", err)
	}
}

func TestDecodeCommandRejectsInvalidType(t *testing.T) {
	data := make([]byte, record.RecordHeaderSize+len("name"))
	data[0] = 99
	binary.BigEndian.PutUint32(data[1:5], uint32(len("name")))
	copy(data[record.RecordHeaderSize:], []byte("name"))

	_, err := DecodeCommand(data)
	if !errors.Is(err, kv_errors.ErrInvalidType) {
		t.Fatalf("DecodeCommand() error = %v, want ErrInvalidType", err)
	}
}
