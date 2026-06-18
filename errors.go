package kv

import "kv_store_demo/internal/platform/kv_errors"

var (
	ErrNotImplemented   = kv_errors.ErrNotImplemented
	ErrInvalidPara      = kv_errors.ErrInvalidPara
	ErrInvalidType      = kv_errors.ErrInvalidType
	ErrEmptyKey         = kv_errors.ErrEmptyKey
	ErrUnexpectedValue  = kv_errors.ErrUnexpectedValue
	ErrRecordTooLarge   = kv_errors.ErrRecordTooLarge
	ErrIncompleteRecord = kv_errors.ErrIncompleteRecord
	ErrNilCallBack      = kv_errors.ErrNilCallBack
	ErrFileClosed       = kv_errors.ErrFileClosed
	ErrDbClosed         = kv_errors.ErrDbClosed
)
