package decode

import (
	"bytes"
	"encoding/binary"
	"historydb/src/internal/utils/pointers"
	"io"
	"time"
)

func DecodeString(buf *bytes.Buffer) (*string, error) {
	var length uint64
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	strBytes := make([]byte, length)
	n, err := io.ReadFull(buf, strBytes)
	if err != nil {
		return nil, err
	}

	s := string(strBytes[:n])
	return &s, nil
}

func DecodeInt(buf *bytes.Buffer) (*int64, error) {
	var i int64
	if err := binary.Read(buf, binary.LittleEndian, &i); err != nil {
		return nil, err
	}
	return &i, nil
}

func DecodeBool(buf *bytes.Buffer) (*bool, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == 0x01 {
		return pointers.Ptr(true), nil
	} else {
		return pointers.Ptr(false), nil
	}
}

func DecodeFloat(buf *bytes.Buffer) (*float64, error) {
	var f float64
	if err := binary.Read(buf, binary.LittleEndian, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func DecodeTime(buf *bytes.Buffer) (*time.Time, error) {
	timeString, err := DecodeString(buf)
	if err != nil {
		return nil, err
	}

	time, err := time.Parse(time.RFC3339, *timeString)
	if err != nil {
		return nil, err
	}

	return &time, nil
}
