package sql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"historydb/src/internal/utils/pointers"
	"historydb/src/internal/utils/types"
)

var SQLTABLE_VERSION int64 = 1

type SQLTable struct {
	Version     int64                `json:"version"`
	Name        string               `json:"name"`
	Columns     []SQLTableColumn     `json:"columns"`
	Constraints []SQLTableConstraint `json:"constraints"`
	ForeignKeys []SQLTableForeignKey `json:"foreignKeys"`
	Indexes     []SQLTableIndex      `json:"indexes"`
}

func (table *SQLTable) GetSchemaType() entities.SchemaType {
	return entities.SQLTable
}

func (table *SQLTable) GetName() string {
	return table.Name
}

func (table *SQLTable) Hash() string {
	hash := sha256.Sum256(table.encodeData())
	return hex.EncodeToString(hash[:])
}

func (table *SQLTable) Diff(schema entities.Schema, isDiff bool) entities.SchemaDiff {
	oldTable := schema.(*SQLTable)

	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", schema.Hash())
	} else {
		prevRef = schema.Hash()
	}

	diff := SQLTableDiff{
		hash:    table.Hash(),
		PrevRef: prevRef,
	}
	diff.AddedColumns, diff.RemovedColumns = types.DiffSlices(table.Columns, oldTable.Columns)
	diff.AddedConstraints, diff.RemovedConstraints = types.DiffSlices(table.Constraints, oldTable.Constraints)
	diff.AddedForeignKeys, diff.RemovedForeignKeys = types.DiffSlices(table.ForeignKeys, oldTable.ForeignKeys)
	diff.AddedIndexes, diff.RemovedIndexes = types.DiffSlices(table.Indexes, oldTable.Indexes)

	return &diff
}

func (table *SQLTable) ApplyDiff(diff entities.SchemaDiff) entities.Schema {
	updateTable := *table
	tableDiff := diff.(*SQLTableDiff)

	updateTable.Columns = mergeColumns(table.Columns, tableDiff.AddedColumns, tableDiff.RemovedColumns)
	updateTable.Constraints = mergeConstraints(table.Constraints, tableDiff.AddedConstraints, tableDiff.RemovedConstraints)
	updateTable.ForeignKeys = mergeForeignKeys(table.ForeignKeys, tableDiff.AddedForeignKeys, tableDiff.RemovedForeignKeys)
	updateTable.Indexes = mergeIndexes(table.Indexes, tableDiff.AddedIndexes, tableDiff.RemovedIndexes)

	return &updateTable
}

func (table *SQLTable) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := table.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (table *SQLTable) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	if _, err := decode.DecodeString(buf); err != nil {
		return err
	}
	version, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	name, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	var columnSlice []SQLTableColumn
	if flags&(1<<0) != 0 {
		columns, err := decode.DecodeSlice[*SQLTableColumn](buf)
		if err != nil {
			return err
		}

		columnSlice = make([]SQLTableColumn, 0, len(columns))
		for _, v := range columns {
			columnSlice = append(columnSlice, *v)
		}
	}
	var constraintSlice []SQLTableConstraint
	if flags&(1<<1) != 0 {
		constraints, err := decode.DecodeSlice[*SQLTableConstraint](buf)
		if err != nil {
			return err
		}

		constraintSlice = make([]SQLTableConstraint, 0, len(constraints))
		for _, v := range constraints {
			constraintSlice = append(constraintSlice, *v)
		}
	}
	var fkSlice []SQLTableForeignKey
	if flags&(1<<2) != 0 {
		fKeys, err := decode.DecodeSlice[*SQLTableForeignKey](buf)
		if err != nil {
			return err
		}

		fkSlice = make([]SQLTableForeignKey, 0, len(fKeys))
		for _, v := range fKeys {
			fkSlice = append(fkSlice, *v)
		}
	}
	var idxSlice []SQLTableIndex
	if flags&(1<<3) != 0 {
		idxs, err := decode.DecodeSlice[*SQLTableIndex](buf)
		if err != nil {
			return err
		}

		idxSlice = make([]SQLTableIndex, 0, len(idxs))
		for _, v := range idxs {
			idxSlice = append(idxSlice, *v)
		}
	}

	table.Version = *version
	table.Name = *name
	table.Columns = columnSlice
	table.Constraints = constraintSlice
	table.ForeignKeys = fkSlice
	table.Indexes = idxSlice
	return nil
}

func (table *SQLTable) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, (*string)(pointers.Ptr(entities.SQLTable)))
	encode.EncodeInt(&buf, &SQLTABLE_VERSION)
	buf.WriteByte(table.getByteFlags())
	encode.EncodeString(&buf, &table.Name)
	encode.EncodeSlice(&buf, table.Columns)
	encode.EncodeSlice(&buf, table.Constraints)
	encode.EncodeSlice(&buf, table.ForeignKeys)
	encode.EncodeSlice(&buf, table.Indexes)

	return buf.Bytes()
}

func (table *SQLTable) getByteFlags() byte {
	var flags byte
	if len(table.Columns) > 0 {
		flags |= 1 << 0
	}
	if len(table.Constraints) > 0 {
		flags |= 1 << 1
	}
	if len(table.ForeignKeys) > 0 {
		flags |= 1 << 2
	}
	if len(table.Indexes) > 0 {
		flags |= 2 << 3
	}
	return flags
}

type SQLTableDiff struct {
	hash               string
	PrevRef            string
	AddedColumns       []SQLTableColumn
	RemovedColumns     []SQLTableColumn
	AddedConstraints   []SQLTableConstraint
	RemovedConstraints []SQLTableConstraint
	AddedForeignKeys   []SQLTableForeignKey
	RemovedForeignKeys []SQLTableForeignKey
	AddedIndexes       []SQLTableIndex
	RemovedIndexes     []SQLTableIndex
}

func (diff *SQLTableDiff) Hash() string {
	return diff.hash
}

func (diff *SQLTableDiff) GetPrevRef() string {
	return diff.PrevRef
}

func (diff *SQLTableDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := diff.encodeData()
	integerityHash := sha256.Sum256(encodedData)

	buf.Write(integerityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (diff *SQLTableDiff) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	prevRef, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	var addedColumns []SQLTableColumn
	if flags&(1<<0) != 0 {
		columns, err := decode.DecodeSlice[*SQLTableColumn](buf)
		if err != nil {
			return err
		}

		addedColumns = make([]SQLTableColumn, 0, len(columns))
		for _, v := range columns {
			addedColumns = append(addedColumns, *v)
		}
	}
	var removedColumns []SQLTableColumn
	if flags&(1<<1) != 0 {
		columns, err := decode.DecodeSlice[*SQLTableColumn](buf)
		if err != nil {
			return err
		}

		removedColumns = make([]SQLTableColumn, 0, len(columns))
		for _, v := range columns {
			removedColumns = append(removedColumns, *v)
		}
	}
	var addedConstraints []SQLTableConstraint
	if flags&(1<<2) != 0 {
		constraints, err := decode.DecodeSlice[*SQLTableConstraint](buf)
		if err != nil {
			return err
		}

		addedConstraints = make([]SQLTableConstraint, 0, len(constraints))
		for _, v := range constraints {
			addedConstraints = append(addedConstraints, *v)
		}
	}
	var removedConstraints []SQLTableConstraint
	if flags&(1<<3) != 0 {
		constraints, err := decode.DecodeSlice[*SQLTableConstraint](buf)
		if err != nil {
			return err
		}

		removedConstraints = make([]SQLTableConstraint, 0, len(constraints))
		for _, v := range constraints {
			removedConstraints = append(removedConstraints, *v)
		}
	}
	var addedFKeys []SQLTableForeignKey
	if flags&(1<<4) != 0 {
		fKeys, err := decode.DecodeSlice[*SQLTableForeignKey](buf)
		if err != nil {
			return err
		}

		addedFKeys = make([]SQLTableForeignKey, 0, len(fKeys))
		for _, v := range fKeys {
			addedFKeys = append(addedFKeys, *v)
		}
	}
	var removedFKeys []SQLTableForeignKey
	if flags&(1<<5) != 0 {
		fKeys, err := decode.DecodeSlice[*SQLTableForeignKey](buf)
		if err != nil {
			return err
		}

		removedFKeys = make([]SQLTableForeignKey, 0, len(fKeys))
		for _, v := range fKeys {
			removedFKeys = append(removedFKeys, *v)
		}
	}
	var addedIdxs []SQLTableIndex
	if flags&(1<<6) != 0 {
		idxs, err := decode.DecodeSlice[*SQLTableIndex](buf)
		if err != nil {
			return err
		}

		addedIdxs = make([]SQLTableIndex, 0, len(idxs))
		for _, v := range idxs {
			addedIdxs = append(addedIdxs, *v)
		}
	}
	var removedIdxs []SQLTableIndex
	if flags&(1<<7) != 0 {
		idxs, err := decode.DecodeSlice[*SQLTableIndex](buf)
		if err != nil {
			return err
		}

		removedIdxs = make([]SQLTableIndex, 0, len(idxs))
		for _, v := range idxs {
			removedIdxs = append(removedIdxs, *v)
		}
	}

	diff.PrevRef = *prevRef
	diff.AddedColumns = addedColumns
	diff.RemovedColumns = removedColumns
	diff.AddedConstraints = addedConstraints
	diff.RemovedConstraints = removedConstraints
	diff.AddedForeignKeys = addedFKeys
	diff.RemovedForeignKeys = removedFKeys
	diff.AddedIndexes = addedIdxs
	diff.RemovedIndexes = removedIdxs
	return nil
}

func (diff *SQLTableDiff) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, &diff.PrevRef)
	buf.WriteByte(diff.getByteFlags())
	encode.EncodeSlice(&buf, diff.AddedColumns)
	encode.EncodeSlice(&buf, diff.RemovedColumns)
	encode.EncodeSlice(&buf, diff.AddedConstraints)
	encode.EncodeSlice(&buf, diff.RemovedConstraints)
	encode.EncodeSlice(&buf, diff.AddedForeignKeys)
	encode.EncodeSlice(&buf, diff.RemovedForeignKeys)
	encode.EncodeSlice(&buf, diff.AddedIndexes)
	encode.EncodeSlice(&buf, diff.RemovedIndexes)

	return buf.Bytes()
}

func (diff *SQLTableDiff) getByteFlags() byte {
	var flags byte
	if len(diff.AddedColumns) > 0 {
		flags |= 1 << 0
	}
	if len(diff.RemovedColumns) > 0 {
		flags |= 1 << 1
	}
	if len(diff.AddedConstraints) > 0 {
		flags |= 1 << 2
	}
	if len(diff.RemovedConstraints) > 0 {
		flags |= 1 << 3
	}
	if len(diff.AddedForeignKeys) > 0 {
		flags |= 1 << 4
	}
	if len(diff.RemovedForeignKeys) > 0 {
		flags |= 1 << 5
	}
	if len(diff.AddedIndexes) > 0 {
		flags |= 1 << 6
	}
	if len(diff.RemovedIndexes) > 0 {
		flags |= 1 << 7
	}
	return flags
}
