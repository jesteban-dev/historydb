package encode

import (
	"bytes"
	"encoding/binary"
)

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
