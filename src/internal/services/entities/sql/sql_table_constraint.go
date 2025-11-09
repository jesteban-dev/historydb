package sql

import (
	"bytes"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"sort"
)

type ConstraintType string

const (
	PrimaryKey ConstraintType = "PRIMARY KEY"
	Unique     ConstraintType = "UNIQUE"
	Check      ConstraintType = "CHECK"
)

type SQLTableConstraint struct {
	Type       ConstraintType `json:"type"`
	Name       string         `json:"name"`
	Columns    []string       `json:"columns"`
	Definition *string        `json:"definition"`
}

func (constraint SQLTableConstraint) EncodeToBytes() []byte {
	var buf bytes.Buffer

	buf.WriteByte(constraint.getByteFlags())
	encode.EncodeString(&buf, (*string)(&constraint.Type))
	encode.EncodeString(&buf, &constraint.Name)
	encode.EncodePrimitiveSlice(&buf, constraint.Columns)
	encode.EncodeString(&buf, constraint.Definition)

	return buf.Bytes()
}

func (constraint *SQLTableConstraint) DecodeFromBytes(data []byte) (*SQLTableConstraint, error) {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	constraintType, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}
	name, err := decode.DecodeString(buf)
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
	var definition *string
	if flags&(1<<1) != 0 {
		definition, err = decode.DecodeString(buf)
		if err != nil {
			return nil, err
		}
	}

	if constraint == nil {
		return &SQLTableConstraint{
			Type:       ConstraintType(*constraintType),
			Name:       *name,
			Columns:    columns,
			Definition: definition,
		}, nil
	} else {
		constraint.Type = ConstraintType(*constraintType)
		constraint.Name = *name
		constraint.Columns = columns
		constraint.Definition = definition
		return nil, nil
	}
}

func (constraint SQLTableConstraint) getByteFlags() byte {
	var flags byte
	if len(constraint.Columns) > 0 {
		flags |= 1 << 0
	}
	if constraint.Definition != nil {
		flags |= 1 << 1
	}
	return flags
}

func (c1 SQLTableConstraint) equal(c2 SQLTableConstraint) bool {
	if (c1.Definition != nil && c2.Definition != nil && *c1.Definition == *c2.Definition) || (c1.Definition == nil && c2.Definition == nil) {
		if len(c1.Columns) != len(c2.Columns) {
			return false
		}

		for i := 0; i < len(c1.Columns); i++ {
			if c1.Columns[i] != c2.Columns[i] {
				return false
			}
		}
		return c1.Type == c2.Type && c1.Name == c2.Name
	}
	return false
}

func mergeConstraints(originalConstraints, addedConstraints, removedConstraints []SQLTableConstraint) []SQLTableConstraint {
	newConstraints := make([]SQLTableConstraint, 0, len(originalConstraints)+len(addedConstraints)-len(removedConstraints))

outerLoop:
	for _, constraint := range originalConstraints {
		for _, del := range removedConstraints {
			if constraint.equal(del) {
				continue outerLoop
			}
		}
		newConstraints = append(newConstraints, constraint)
	}

	newConstraints = append(newConstraints, addedConstraints...)
	sort.Slice(newConstraints, func(i, j int) bool {
		return newConstraints[i].Name < newConstraints[j].Name
	})

	return newConstraints
}
