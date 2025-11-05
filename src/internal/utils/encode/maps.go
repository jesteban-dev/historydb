package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"historydb/src/internal/utils/pointers"
	"sort"
	"time"
)

func EncodeMap(buf *bytes.Buffer, m map[string]interface{}) {
	if len(m) > 0 {
		var mapBuf bytes.Buffer

		// Sort keys for deterministic order
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Encode each key-value pair
		for _, k := range keys {
			v := m[k]
			EncodeString(&mapBuf, &k)

			switch val := v.(type) {
			case nil:
				mapBuf.WriteByte(0x00)
			case string:
				mapBuf.WriteByte(0x01)
				EncodeString(&mapBuf, &val)
			case int64:
				mapBuf.WriteByte(0x02)
				EncodeInt(&mapBuf, &val)
			case int:
				mapBuf.WriteByte(0x02)
				EncodeInt(&mapBuf, pointers.Ptr(int64(val)))
			case bool:
				mapBuf.WriteByte(0x03)
				EncodeBool(&mapBuf, &val)
			case float64:
				mapBuf.WriteByte(0x04)
				EncodeFloat(&mapBuf, &val)
			case time.Time:
				mapBuf.WriteByte(0x05)
				EncodeTime(&mapBuf, &val)
			default:
				panic(fmt.Sprintf("unsupported type: %T", val))
			}
		}

		binary.Write(buf, binary.LittleEndian, uint64(len(mapBuf.Bytes())))
		buf.Write(mapBuf.Bytes())
	}
}

func EncodeStructMap[T Encodable](buf *bytes.Buffer, m map[string]T) {
	if len(m) > 0 {
		var mapBuf bytes.Buffer

		// Sort keys for deterministic order
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Encode each key-value pair
		for _, k := range keys {
			v := m[k]
			EncodeString(&mapBuf, &k)
			encodedData := v.EncodeToBytes()
			binary.Write(&mapBuf, binary.LittleEndian, int64(len(encodedData)))
			mapBuf.Write(encodedData)
		}

		binary.Write(buf, binary.LittleEndian, uint64(len(mapBuf.Bytes())))
		buf.Write(mapBuf.Bytes())
	}
}
