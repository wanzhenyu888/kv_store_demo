package kv_errors

import (
	"errors"
)

var (
	ErrNotImplemented   = errors.New("not implemented")
	ErrInvalidPara		= errors.New("invalid para")
	ErrInvalidType      = errors.New("invalid type")
	ErrEmptyKey         = errors.New("empty key")
	ErrUnexpectedValue  = errors.New("delete record should not have value")
	ErrRecordTooLarge   = errors.New("record too large")
	ErrIncompleteRecord = errors.New("incomplete record")
	ErrNilCallBack		= errors.New("callback should not be nil")
	ErrFileClosed		= errors.New("file is closed")
)
