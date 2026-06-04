package record

import (
	"errors"
)

var (
	ErrNotImplemented   = errors.New("record: not implemented")
	ErrInvalidType      = errors.New("record: invalid type")
	ErrEmptyKey         = errors.New("record: empty key")
	ErrUnexpectedValue  = errors.New("record: delete record should not have value")
	ErrRecordTooLarge   = errors.New("record: record too large")
	ErrIncompleteRecord = errors.New("record: incomplete record")
)
