package decode

import (
	"bytes"
	"encoding/binary"
	"io"
)

func DecodePrimitiveSlice[T string | int | bool](buf *bytes.Buffer) ([]T, error) {
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
loop:
	for {
		var val T
		switch any(val).(type) {
		case string:
			s, err := DecodeString(sliceBuf)
			if err != nil {
				if err == io.EOF {
					break loop
				}
				return nil, err
			}
			slice = append(slice, any(s).(T))
		case int:
			i, err := DecodeInt(sliceBuf)
			if err != nil {
				if err == io.EOF {
					break loop
				}
				return nil, err
			}
			slice = append(slice, any(i).(T))
		case bool:
			b, err := DecodeBool(sliceBuf)
			if err != nil {
				if err == io.EOF {
					break loop
				}
				return nil, err
			}
			slice = append(slice, any(b).(T))
		default:
			break
		}
	}

	return slice, nil
}

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
