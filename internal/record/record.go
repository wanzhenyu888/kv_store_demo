package record

import (
	"encoding/binary"
	"io"
	"math"
)

const (
	TypePut    byte = 1
	TypeDelete byte = 2
)

/* Type (1 byte) + Key Length (4 bytes) + Value Length (4 bytes) */
const (
	RecordTypeSize   = 1
	RecordKeySize    = 4
	RecordValueSize  = 4
	RecordHeaderSize = RecordTypeSize + RecordKeySize + RecordValueSize
)

type Record struct {
	Type  byte
	Key   []byte
	Value []byte
}

func CheckEncodePara(record Record) error {
	// 校验Type
	if record.Type != TypePut && record.Type != TypeDelete {
		return ErrInvalidType
	} else if record.Type == TypeDelete && len(record.Value) != 0 {
		return ErrUnexpectedValue
	}

	// 校验Key和Value的长度
	if len(record.Key) > math.MaxUint32 || len(record.Value) > math.MaxUint32 {
		return ErrRecordTooLarge
	} else if len(record.Key) == 0 {
		return ErrEmptyKey
	}

	return nil
}

func Encode(record Record) ([]byte, error) {
	if err := CheckEncodePara(record); err != nil {
		return nil, err
	}

	buf := make([]byte, RecordHeaderSize+len(record.Key)+len(record.Value))
	offset := 0
	buf[offset] = record.Type
	offset += RecordTypeSize
	binary.BigEndian.PutUint32(buf[offset:offset+RecordKeySize], uint32(len(record.Key)))
	offset += RecordKeySize
	binary.BigEndian.PutUint32(buf[offset:offset+RecordValueSize], uint32(len(record.Value)))
	offset += RecordValueSize
	copy(buf[offset:offset+len(record.Key)], record.Key)
	offset += len(record.Key)
	copy(buf[offset:], record.Value)

	return buf, nil
}

func Decode(r io.Reader) (Record, error) {
	// 读取Record Header
	headerBuf := make([]byte, RecordHeaderSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return Record{}, ErrIncompleteRecord
	}

	// 解析Record Header
	offset := 0
	recordType := headerBuf[offset]
	offset += RecordTypeSize
	keySize := binary.BigEndian.Uint32(headerBuf[offset : offset+RecordKeySize])
	offset += RecordKeySize
	valueSize := binary.BigEndian.Uint32(headerBuf[offset : offset+RecordValueSize])
	if recordType != TypePut && recordType != TypeDelete {
		return Record{}, ErrInvalidType
	} else if keySize == 0 {
		return Record{}, ErrEmptyKey
	}

	// 读取Record Key
	keyBuf := make([]byte, keySize)
	if _, err := io.ReadFull(r, keyBuf); err != nil {
		return Record{}, ErrIncompleteRecord
	}

	// 读取Record Value
	valueBuf := make([]byte, valueSize)
	if _, err := io.ReadFull(r, valueBuf); err != nil {
		return Record{}, ErrIncompleteRecord
	}

	return Record{
		Type:  recordType,
		Key:   keyBuf,
		Value: valueBuf,
	}, nil
}
