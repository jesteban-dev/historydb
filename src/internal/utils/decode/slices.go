package decode

import (
	"bytes"
	"encoding/binary"
	"io"
)

func DecodeSlice[T Decodable[T]](buf *bytes.Buffer) ([]T, error) {
	var length uint64
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	sliceBytes := make([]byte, length)
	n, err := io.ReadFull(buf, sliceBytes)
	if err != nil {
		return nil, err
	}

	sliceBuf := bytes.NewBuffer(sliceBytes[:n])

	slice := []T{}
	for {
		var itemLength uint64
		if err := binary.Read(sliceBuf, binary.LittleEndian, &itemLength); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		itemBytes := make([]byte, itemLength)
		n, err := io.ReadFull(sliceBuf, itemBytes)
		if err != nil {
			return nil, err
		}

		var item T
		item, err = item.DecodeFromBytes(itemBytes[:n])
		if err != nil {
			return nil, err
		}
		slice = append(slice, item)
	}

	return slice, nil
}
