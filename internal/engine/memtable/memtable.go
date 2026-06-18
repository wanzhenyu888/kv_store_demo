package memtable

import (
	"slices"

	"kv_store_demo/internal/engine/record"
	"kv_store_demo/internal/platform/kv_errors"
)

type MemTable struct {
	records map[string]record.Record
	size    uint64
}

func (m *MemTable) generateOneRecordByCopy(opType byte, key, value []byte) record.Record {
	keyBuf := make([]byte, len(key))
	valueBuf := make([]byte, len(value))
	copy(keyBuf, key)
	copy(valueBuf, value)
	return record.Record{
		Type:  opType,
		Key:   keyBuf,
		Value: valueBuf,
	}
}

func New() *MemTable {
	return &MemTable{
		records: make(map[string]record.Record),
		size:    0,
	}
}

func (m *MemTable) insertToMemtable(newRecord record.Record) error {
	oldRecord, isOk := m.records[string(newRecord.Key)]
	if isOk {
		m.size -= uint64(record.RecordHeaderSize + len(oldRecord.Key) + len(oldRecord.Value))
	}
	m.records[string(newRecord.Key)] = newRecord
	m.size += uint64(record.RecordHeaderSize + len(newRecord.Key) + len(newRecord.Value))
	return nil
}

func (m *MemTable) Put(key, value []byte) error {
	if len(key) == 0 {
		return kv_errors.ErrEmptyKey
	}

	newRecord := m.generateOneRecordByCopy(record.TypePut, key, value)
	return m.insertToMemtable(newRecord)
}

func (m *MemTable) getFromMemtable(key []byte) (record.Record, bool) {
	curRecord, isOk := m.records[string(key)]
	if !isOk {
		return record.Record{}, false
	}

	resRecord := m.generateOneRecordByCopy(curRecord.Type, curRecord.Key, curRecord.Value)
	return resRecord, true
}

func (m *MemTable) Get(key []byte) (record.Record, bool) {
	return m.getFromMemtable(key)
}

func (m *MemTable) deleteInMemtable(record record.Record) error {
	return m.insertToMemtable(record)
}

func (m *MemTable) Delete(key []byte) error {
	if len(key) == 0 {
		return kv_errors.ErrEmptyKey
	}

	newRecord := m.generateOneRecordByCopy(record.TypeDelete, key, nil)
	return m.deleteInMemtable(newRecord)
}

func (m *MemTable) Size() uint64 {
	return m.size
}

func (m *MemTable) ForEachRecord(fn func(record.Record) error) error {
	if fn == nil {
		return kv_errors.ErrNilCallBack
	}

	keysList := make([]string, 0, len(m.records))
	for key := range m.records {
		keysList = append(keysList, key)
	}
	slices.Sort(keysList)

	for _, key := range keysList {
		rec := m.records[key]
		if err := fn(m.generateOneRecordByCopy(rec.Type, rec.Key, rec.Value)); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemTable) Reset() {
	m.size = 0
	clear(m.records)
}
