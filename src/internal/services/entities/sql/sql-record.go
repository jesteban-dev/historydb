package sql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
)

type SQLRecord struct {
	hash    string
	Content map[string]interface{}
}

func (record SQLRecord) Hash() string {
	if record.hash == "" {
		hash := sha256.Sum256(record.EncodeToBytes())
		record.hash = hex.EncodeToString(hash[:])
	}
	return record.hash
}

func (record SQLRecord) EncodeToBytes() []byte {
	var buf bytes.Buffer
	encode.EncodeMap(&buf, record.Content)
	return buf.Bytes()
}

func (record *SQLRecord) DecodeFromBytes(data []byte) (*SQLRecord, error) {
	buf := bytes.NewBuffer(data)
	content, err := decode.DecodeMap(buf)
	if err != nil {
		return nil, err
	}

	return &SQLRecord{
		Content: content,
	}, nil
}

type SQLRecordDiff struct {
	PrevRef *string
	Record  map[string]interface{}
}

func (diff SQLRecordDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	buf.WriteByte(diff.getByteFlags())
	encode.EncodeString(&buf, diff.PrevRef)
	encode.EncodeMap(&buf, diff.Record)

	return buf.Bytes()
}

func (diff *SQLRecordDiff) DecodeFromBytes(data []byte) (*SQLRecordDiff, error) {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	var prevRef *string
	if flags&(1<<0) != 0 {
		prevRef, err = decode.DecodeString(buf)
		if err != nil {
			return nil, err
		}
	}
	var record map[string]interface{}
	if flags&(1<<1) != 0 {
		record, err = decode.DecodeMap(buf)
		if err != nil {
			return nil, err
		}
	}

	return &SQLRecordDiff{
		PrevRef: prevRef,
		Record:  record,
	}, nil
}

func (diff SQLRecordDiff) getByteFlags() byte {
	var flags byte

	if diff.PrevRef != nil {
		flags |= 1 << 0
	}
	if len(diff.Record) != 0 {
		flags |= 1 << 1
	}

	return flags
}
