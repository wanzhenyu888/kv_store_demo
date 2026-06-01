package record

import "errors"

var ErrNotImplemented = errors.New("record: not implemented")

type Type byte

const (
	TypePut    Type = 1
	TypeDelete Type = 2
)

type Record struct {
	Type  Type
	Key   []byte
	Value []byte
}

func Encode(record Record) ([]byte, error) {
	return nil, ErrNotImplemented
}

func Decode(data []byte) (Record, error) {
	return Record{}, ErrNotImplemented
}
