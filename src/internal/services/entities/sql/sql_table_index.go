package sql

import (
	"bytes"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"reflect"
	"sort"
)

type SQLTableIndex struct {
	Name    string
	Type    string
	Columns []string
	Options map[string]interface{}
}

func (index SQLTableIndex) EncodeToBytes() []byte {
	var buf bytes.Buffer

	buf.WriteByte(index.getByteFlags())
	encode.EncodeString(&buf, &index.Name)
	encode.EncodeString(&buf, &index.Type)
	encode.EncodePrimitiveSlice(&buf, index.Columns)
	encode.EncodeMap(&buf, index.Options)

	return buf.Bytes()
}

func (index *SQLTableIndex) DecodeFromBytes(data []byte) (*SQLTableIndex, error) {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	name, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	indexType, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	var columns []string
	if flags&(1<<0) != 0 {
		columns, err = decode.DecodePrimitiveSlice[string](buf)
		if err != nil {
			return nil, err
		}
	}
	var options map[string]interface{}
	if flags&(1<<1) != 0 {
		options, err = decode.DecodeMap(buf)
		if err != nil {
			return nil, err
		}
	}

	if index == nil {
		return &SQLTableIndex{
			Name:    *name,
			Type:    *indexType,
			Columns: columns,
			Options: options,
		}, nil
	} else {
		index.Name = *name
		index.Type = *indexType
		index.Columns = columns
		index.Options = options
		return nil, nil
	}
}

func (index SQLTableIndex) getByteFlags() byte {
	var flags byte
	if len(index.Columns) > 0 {
		flags |= 1 << 0
	}
	if index.Options != nil {
		flags |= 1 << 1
	}
	return flags
}

func (idx1 SQLTableIndex) equal(idx2 SQLTableIndex) bool {
	if len(idx1.Columns) != len(idx2.Columns) || len(idx1.Options) != len(idx2.Options) {
		return false
	}
	for i := 0; i < len(idx1.Columns); i++ {
		if idx1.Columns[i] != idx2.Columns[i] {
			return false
		}
	}
	for k, v1 := range idx1.Options {
		v2, ok := idx2.Options[k]
		if !ok || !reflect.DeepEqual(v1, v2) {
			return false
		}
	}
	return idx1.Name == idx2.Name && idx1.Type == idx2.Type
}

func mergeIndexes(originalIndexes, addedIndexes, removedIndexes []SQLTableIndex) []SQLTableIndex {
	newIndexes := make([]SQLTableIndex, 0, len(originalIndexes)+len(addedIndexes)-len(removedIndexes))

outerLoop:
	for _, idx := range originalIndexes {
		for _, del := range removedIndexes {
			if idx.equal(del) {
				continue outerLoop
			}
		}
		newIndexes = append(newIndexes, idx)
	}

	newIndexes = append(newIndexes, addedIndexes...)
	sort.Slice(newIndexes, func(i, j int) bool {
		return newIndexes[i].Name < newIndexes[j].Name
	})

	return newIndexes
}
