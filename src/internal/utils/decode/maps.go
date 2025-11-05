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
		case 0x00:
			m[*k] = nil
		case 0x01:
			str, err := DecodeString(mapBuf)
			if err != nil {
				return nil, err
			}
			m[*k] = *str
		case 0x02:
			i, err := DecodeInt(mapBuf)
			if err != nil {
				return nil, err
			}
			m[*k] = *i
		case 0x03:
			b, err := DecodeBool(mapBuf)
			if err != nil {
				return nil, err
			}
			m[*k] = *b
		case 0x04:
			f, err := DecodeFloat(mapBuf)
			if err != nil {
				return nil, err
			}
			m[*k] = *f
		case 0x05:
			t, err := DecodeTime(mapBuf)
			if err != nil {
				return nil, err
			}
			m[*k] = *t
		default:
			panic("unsupported type")
		}
	}

	return m, nil
}

func DecodeStructMap[T Decodable[T]](buf *bytes.Buffer) (map[string]T, error) {
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

	m := make(map[string]T)
	for {
		var v T

		k, err := DecodeString(mapBuf)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		var structLength int64
		if err := binary.Read(mapBuf, binary.LittleEndian, &structLength); err != nil {
			return nil, err
		}

		structBytes := make([]byte, structLength)
		if _, err := io.ReadFull(mapBuf, structBytes); err != nil {
			return nil, err
		}

		value, err := v.DecodeFromBytes(structBytes)
		if err != nil {
			return nil, err
		}
		m[*k] = value
	}

	return m, nil
}
