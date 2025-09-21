package sql

import (
	"bytes"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"sort"
)

type ActionType string

const (
	NoAction   ActionType = "NO ACTION"
	Restrict   ActionType = "RESTRICT"
	Cascade    ActionType = "CASCADE"
	SetNull    ActionType = "SET NULL"
	SetDefault ActionType = "SET DEFAULT"
)

type SQLTableForeignKey struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	UpdateAction      ActionType
	DeleteAction      ActionType
}

func (fk SQLTableForeignKey) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, &fk.Name)
	encode.EncodePrimitiveSlice(&buf, fk.Columns)
	encode.EncodeString(&buf, &fk.ReferencedTable)
	encode.EncodePrimitiveSlice(&buf, fk.ReferencedColumns)
	encode.EncodeString(&buf, (*string)(&fk.UpdateAction))
	encode.EncodeString(&buf, (*string)(&fk.DeleteAction))

	return buf.Bytes()
}

func (fk *SQLTableForeignKey) DecodeFromBytes(data []byte) (*SQLTableForeignKey, error) {
	buf := bytes.NewBuffer(data)

	name, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	columns, err := decode.DecodePrimitiveSlice[string](buf)
	if err != nil {
		return nil, err
	}
	referencedTable, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	referencedColumns, err := decode.DecodePrimitiveSlice[string](buf)
	if err != nil {
		return nil, err
	}
	updateAtion, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	deleteAction, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}

	if fk == nil {
		return &SQLTableForeignKey{
			Name:              *name,
			Columns:           columns,
			ReferencedTable:   *referencedTable,
			ReferencedColumns: referencedColumns,
			UpdateAction:      ActionType(*updateAtion),
			DeleteAction:      ActionType(*deleteAction),
		}, nil
	} else {
		fk.Name = *name
		fk.Columns = columns
		fk.ReferencedTable = *referencedTable
		fk.ReferencedColumns = referencedColumns
		fk.UpdateAction = ActionType(*updateAtion)
		fk.DeleteAction = ActionType(*deleteAction)
		return nil, nil
	}
}

func (fk1 SQLTableForeignKey) equal(fk2 SQLTableForeignKey) bool {
	if len(fk1.Columns) != len(fk2.Columns) || len(fk1.ReferencedColumns) != len(fk2.ReferencedColumns) {
		return false
	}
	for i := 0; i < len(fk1.Columns); i++ {
		if fk1.Columns[i] != fk2.Columns[i] {
			return false
		}
	}
	for i := 0; i < len(fk1.ReferencedColumns); i++ {
		if fk1.ReferencedColumns[i] != fk2.ReferencedColumns[i] {
			return false
		}
	}
	return fk1.Name == fk2.Name && fk1.ReferencedTable == fk2.ReferencedTable && fk1.UpdateAction == fk2.UpdateAction && fk1.DeleteAction == fk2.DeleteAction
}

func mergeForeignKeys(originalFKeys, addedFKeys, removedFKeys []SQLTableForeignKey) []SQLTableForeignKey {
	newFKeys := make([]SQLTableForeignKey, 0, len(originalFKeys)+len(addedFKeys)-len(removedFKeys))

outerLoop:
	for _, fKey := range originalFKeys {
		for _, del := range removedFKeys {
			if fKey.equal(del) {
				continue outerLoop
			}
		}
		newFKeys = append(newFKeys, fKey)
	}

	newFKeys = append(newFKeys, addedFKeys...)
	sort.Slice(newFKeys, func(i, j int) bool {
		return newFKeys[i].Name < newFKeys[j].Name
	})

	return newFKeys
}
