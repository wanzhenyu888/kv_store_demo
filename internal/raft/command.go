package raftnode

import (
	"bytes"

	"kv_store_demo/internal/engine/record"
)

const (
	CommandTypePut    byte = record.TypePut
	CommandTypeDelete byte = record.TypeDelete
)

type Command struct {
	Type  byte
	Key   []byte
	Value []byte
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return record.Encode(record.Record{
		Type:  cmd.Type,
		Key:   cmd.Key,
		Value: cmd.Value,
	})
}

func DecodeCommand(data []byte) (Command, error) {
	decoded, err := record.Decode(bytes.NewReader(data))
	if err != nil {
		return Command{}, err
	}

	return Command{
		Type:  decoded.Type,
		Key:   decoded.Key,
		Value: decoded.Value,
	}, nil
}
