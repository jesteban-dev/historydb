package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
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
			case string:
				mapBuf.WriteByte(0x01)
				EncodeString(&mapBuf, &val)
			default:
				panic(fmt.Sprintf("unsupported type: %T", val))
			}
		}

		binary.Write(buf, binary.LittleEndian, uint64(len(mapBuf.Bytes())))
		buf.Write(mapBuf.Bytes())
	}
}
