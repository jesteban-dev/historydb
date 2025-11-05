package encode

import (
	"bytes"
	"encoding/binary"
)

func EncodePrimitiveSlice[T string | int | bool](buf *bytes.Buffer, s []T) {
	if len(s) > 0 {
		var sliceBuf bytes.Buffer

		for _, v := range s {
			switch val := any(v).(type) {
			case string:
				vv := val
				EncodeString(&sliceBuf, &vv)
			case int64:
				vv := val
				EncodeInt(&sliceBuf, &vv)
			case bool:
				vv := val
				EncodeBool(&sliceBuf, &vv)
			}
		}

		binary.Write(buf, binary.LittleEndian, uint64(len(sliceBuf.Bytes())))
		buf.Write(sliceBuf.Bytes())
	}
}

func EncodeSlice[T Encodable](buf *bytes.Buffer, s []T) {
	if len(s) > 0 {
		var sliceBuf bytes.Buffer
		var totalSize uint64

		for _, v := range s {
			data := v.EncodeToBytes()
			size := uint64(len(data))

			binary.Write(&sliceBuf, binary.LittleEndian, size)
			sliceBuf.Write(data)
			totalSize += size + 8 // Size of size littleEndian
		}

		binary.Write(buf, binary.LittleEndian, totalSize)
		buf.Write(sliceBuf.Bytes())
	}
}
