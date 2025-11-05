package decode

import (
	"bytes"
	"encoding/binary"
	"io"
)

func DecodeMap(buf *bytes.Buffer) (map[string]interface{}, error) {
	var length uint64
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	mapBytes := make([]byte, length)
	n, err := io.ReadFull(buf, mapBytes)
	if err != nil {
		return nil, err
	}

	mapBuf := bytes.NewBuffer(mapBytes[:n])

	m := make(map[string]interface{})
	for {
		k, err := DecodeString(mapBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		typ, err := mapBuf.ReadByte()
		if err != nil {
			return nil, err
		}

		switch typ {
		case 0x01:
			str, err := DecodeString(mapBuf)
			if err != nil {
				return nil, err
			}
			m[*k] = *str
		default:
			panic("unsupported type")
		}
	}

	return m, nil
}
