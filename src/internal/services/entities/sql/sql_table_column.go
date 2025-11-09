package sql

import (
	"bytes"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"sort"
)

type SQLTableColumn struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	IsNullable   bool    `json:"isNullable"`
	DefaultValue *string `json:"defaultValue"`
	Position     int64   `json:"position"`
}

func (column SQLTableColumn) EncodeToBytes() []byte {
	var buf bytes.Buffer

	buf.WriteByte(column.getByteFlags())
	encode.EncodeString(&buf, &column.Name)
	encode.EncodeString(&buf, &column.Type)
	encode.EncodeBool(&buf, &column.IsNullable)
	encode.EncodeString(&buf, column.DefaultValue)
	encode.EncodeInt(&buf, &column.Position)

	return buf.Bytes()
}

func (column *SQLTableColumn) DecodeFromBytes(data []byte) (*SQLTableColumn, error) {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	name, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	columnType, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	isNullable, err := decode.DecodeBool(buf)
	if err != nil {
		return nil, err
	}
	var defaultValue *string
	if flags&(1<<0) != 0 {
		defaultValue, err = decode.DecodeString(buf)
		if err != nil {
			return nil, err
		}
	}
	position, err := decode.DecodeInt(buf)
	if err != nil {
		return nil, err
	}

	if column == nil {
		return &SQLTableColumn{
			Name:         *name,
			Type:         *columnType,
			IsNullable:   *isNullable,
			DefaultValue: defaultValue,
			Position:     *position,
		}, nil
	} else {
		column.Name = *name
		column.Type = *columnType
		column.IsNullable = *isNullable
		column.DefaultValue = defaultValue
		column.Position = *position
		return nil, nil
	}
}

func (column SQLTableColumn) getByteFlags() byte {
	var flags byte
	if column.DefaultValue != nil {
		flags |= 1 << 0
	}
	return flags
}

func (c1 SQLTableColumn) equal(c2 SQLTableColumn) bool {
	if (c1.DefaultValue != nil && c2.DefaultValue != nil && *c1.DefaultValue == *c2.DefaultValue) || (c1.DefaultValue == nil && c2.DefaultValue == nil) {
		return c1.Name == c2.Name && c1.Type == c2.Type && c1.IsNullable == c2.IsNullable && c1.Position == c2.Position
	}
	return false
}

func mergeColumns(originalColumns, addedColumns, removedColumns []SQLTableColumn) []SQLTableColumn {
	newColumns := make([]SQLTableColumn, 0, len(originalColumns)+len(addedColumns)-len(removedColumns))

outerLoop:
	for _, column := range originalColumns {
		for _, del := range removedColumns {
			if column.equal(del) {
				continue outerLoop
			}
		}
		newColumns = append(newColumns, column)
	}

	newColumns = append(newColumns, addedColumns...)
	sort.Slice(newColumns, func(i, j int) bool {
		return newColumns[i].Position < newColumns[j].Position
	})

	return newColumns
}
