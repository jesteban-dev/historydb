package encode

import (
	"bytes"
	"encoding/binary"
)

func EncodeString(buf *bytes.Buffer, s *string) {
	if s != nil {
		binary.Write(buf, binary.LittleEndian, uint64(len(*s)))
		buf.WriteString(*s)
	}
}

func EncodeInt(buf *bytes.Buffer, i *int) {
	if i != nil {
		binary.Write(buf, binary.LittleEndian, int64(*i))
	}
}

func EncodeBool(buf *bytes.Buffer, b *bool) {
	if b != nil {
		if *b {
			buf.WriteByte('1')
		} else {
			buf.WriteByte('0')
		}
	}
}
