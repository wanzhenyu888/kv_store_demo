package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestEncodeDecodePut(t *testing.T) {
	input := Record{
		Type:  TypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	}

	encoded, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.Type != input.Type || !bytes.Equal(got.Key, input.Key) || !bytes.Equal(got.Value, input.Value) {
		t.Fatalf("Decode() = %+v, want %+v", got, input)
	}
}

func TestEncodeDecodeDelete(t *testing.T) {
	input := Record{
		Type: TypeDelete,
		Key:  []byte("name"),
	}

	encoded, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.Type != TypeDelete || !bytes.Equal(got.Key, input.Key) || len(got.Value) != 0 {
		t.Fatalf("Decode() = %+v, want DELETE with empty value", got)
	}
}

func TestEncodeDecodePutWithEmptyValue(t *testing.T) {
	input := Record{
		Type:  TypePut,
		Key:   []byte("empty"),
		Value: nil,
	}

	encoded, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.Type != TypePut || !bytes.Equal(got.Key, input.Key) || len(got.Value) != 0 {
		t.Fatalf("Decode() = %+v, want PUT with empty value", got)
	}
}

func TestEncodeDecodeBinaryPayload(t *testing.T) {
	input := Record{
		Type:  TypePut,
		Key:   []byte{'k', ' ', '\n', 0xff},
		Value: []byte{'v', ' ', '\n', 0x00, 0xfe},
	}

	encoded, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	got, err := Decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if got.Type != input.Type || !bytes.Equal(got.Key, input.Key) || !bytes.Equal(got.Value, input.Value) {
		t.Fatalf("Decode() = %+v, want %+v", got, input)
	}
}

func TestEncodeRejectsInvalidType(t *testing.T) {
	_, err := Encode(Record{Type: 99, Key: []byte("name"), Value: []byte("alice")})
	if !errors.Is(err, ErrInvalidType) {
		t.Fatalf("Encode() error = %v, want ErrInvalidType", err)
	}
}

func TestEncodeRejectsEmptyKey(t *testing.T) {
	_, err := Encode(Record{Type: TypePut, Value: []byte("alice")})
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Encode() error = %v, want ErrEmptyKey", err)
	}
}

func TestEncodeRejectsDeleteWithValue(t *testing.T) {
	_, err := Encode(Record{Type: TypeDelete, Key: []byte("name"), Value: []byte("alice")})
	if !errors.Is(err, ErrUnexpectedValue) {
		t.Fatalf("Encode() error = %v, want ErrUnexpectedValue", err)
	}
}

func TestDecodeRejectsInvalidType(t *testing.T) {
	data := make([]byte, RecordHeaderSize+len("name"))
	data[0] = 99
	binary.BigEndian.PutUint32(data[1:5], uint32(len("name")))
	copy(data[RecordHeaderSize:], []byte("name"))

	_, err := Decode(bytes.NewReader(data))
	if !errors.Is(err, ErrInvalidType) {
		t.Fatalf("Decode() error = %v, want ErrInvalidType", err)
	}
}

func TestDecodeRejectsEmptyKey(t *testing.T) {
	data := make([]byte, RecordHeaderSize)
	data[0] = TypePut

	_, err := Decode(bytes.NewReader(data))
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Decode() error = %v, want ErrEmptyKey", err)
	}
}

func TestDecodeRejectsIncompleteHeader(t *testing.T) {
	_, err := Decode(bytes.NewReader([]byte{TypePut}))
	if !errors.Is(err, ErrIncompleteRecord) {
		t.Fatalf("Decode() error = %v, want ErrIncompleteRecord", err)
	}
}

func TestDecodeRejectsIncompleteKey(t *testing.T) {
	data := make([]byte, RecordHeaderSize)
	data[0] = TypePut
	binary.BigEndian.PutUint32(data[1:5], 4)

	_, err := Decode(bytes.NewReader(data))
	if !errors.Is(err, ErrIncompleteRecord) {
		t.Fatalf("Decode() error = %v, want ErrIncompleteRecord", err)
	}
}

func TestDecodeRejectsIncompleteValue(t *testing.T) {
	data := make([]byte, RecordHeaderSize+len("name"))
	data[0] = TypePut
	binary.BigEndian.PutUint32(data[1:5], uint32(len("name")))
	binary.BigEndian.PutUint32(data[5:9], 5)
	copy(data[RecordHeaderSize:], []byte("name"))

	_, err := Decode(bytes.NewReader(data))
	if !errors.Is(err, ErrIncompleteRecord) {
		t.Fatalf("Decode() error = %v, want ErrIncompleteRecord", err)
	}
}

func TestEncodedLength(t *testing.T) {
	input := Record{
		Type:  TypePut,
		Key:   []byte("name"),
		Value: []byte("alice"),
	}

	encoded, err := Encode(input)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	want := RecordHeaderSize + len(input.Key) + len(input.Value)
	if len(encoded) != want {
		t.Fatalf("len(encoded) = %d, want %d", len(encoded), want)
	}
}
