package encode

import (
	"bytes"
	"encoding/binary"
	"historydb/src/internal/utils/types"
	"time"
)

func EncodeString(buf *bytes.Buffer, s *string) {
	if s != nil {
		binary.Write(buf, binary.LittleEndian, uint64(len(*s)))
		buf.WriteString(*s)
	}
}

func EncodeInt(buf *bytes.Buffer, i *int64) {
	if i != nil {
		binary.Write(buf, binary.LittleEndian, *i)
	}
}

func EncodeBigInt(buf *bytes.Buffer, bi *types.BigInt) {
	if bi != nil {
		b := bi.Bytes()

		binary.Write(buf, binary.LittleEndian, uint64(len(b)))
		buf.Write(b)
	}
}

func EncodeBool(buf *bytes.Buffer, b *bool) {
	if b != nil {
		if *b {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}
}

func EncodeFloat(buf *bytes.Buffer, f *float64) {
	if f != nil {
		binary.Write(buf, binary.LittleEndian, *f)
	}
}

func EncodeTime(buf *bytes.Buffer, t *time.Time) {
	if t != nil {
		timeString := t.Format(time.RFC3339)
		EncodeString(buf, &timeString)
	}
}
