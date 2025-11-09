package psql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/utils/comparation"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"historydb/src/internal/utils/pointers"
	"slices"
)

var PSQLPROCEDURE_VERSION int64 = 1

type PSQLProcedure struct {
	Version      int64
	Name         string   `json:"name"`
	Language     string   `json:"language"`
	Dependencies []string `json:"dependencies"`
	Parameters   string   `json:"parameters"`
	Tag          string   `json:"tag"`
	Definition   string   `json:"definition"`
}

func (procedure *PSQLProcedure) GetName() string {
	return procedure.Name
}

func (procedure *PSQLProcedure) GetRoutineType() entities.RoutineType {
	return entities.PSQLProcedure
}

func (procedure *PSQLProcedure) GetDependencies() []string {
	return procedure.Dependencies
}

func (procedure *PSQLProcedure) Hash() string {
	hash := sha256.Sum256(procedure.encodeData())
	return hex.EncodeToString(hash[:])
}

func (procedure *PSQLProcedure) Diff(routine entities.Routine, isDiff bool) entities.RoutineDiff {
	oldProcedure := routine.(*PSQLProcedure)

	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", routine.Hash())
	} else {
		prevRef = routine.Hash()
	}

	diff := PSQLProcedureDiff{
		hash:    procedure.Hash(),
		PrevRef: prevRef,
	}
	comparation.AssignIfChanged(&diff.Language, &procedure.Language, &oldProcedure.Language)
	comparation.AssignIfChanged(&diff.Parameters, &procedure.Parameters, &oldProcedure.Parameters)
	comparation.AssignIfChanged(&diff.Tag, &procedure.Tag, &oldProcedure.Tag)
	comparation.AssignIfChanged(&diff.Definition, &procedure.Definition, &oldProcedure.Definition)

	if !slices.Equal(procedure.Dependencies, oldProcedure.Dependencies) {
		diff.Dependencies = make([]string, len(procedure.Dependencies))
		copy(diff.Dependencies, procedure.Dependencies)
	}

	return &diff
}

func (procedure *PSQLProcedure) ApplyDiff(diff entities.RoutineDiff) entities.Routine {
	updateProcedure := *procedure
	procedureDiff := diff.(*PSQLProcedureDiff)

	comparation.AssignIfNotNil(&updateProcedure.Language, procedureDiff.Language)
	comparation.AssignIfNotNil(&updateProcedure.Parameters, procedureDiff.Parameters)
	comparation.AssignIfNotNil(&updateProcedure.Tag, procedureDiff.Tag)
	comparation.AssignIfNotNil(&updateProcedure.Definition, procedureDiff.Definition)

	if len(updateProcedure.Dependencies) == 0 && len(procedureDiff.Dependencies) > 0 {
		updateProcedure.Dependencies = make([]string, len(procedureDiff.Dependencies))
		copy(updateProcedure.Dependencies, procedureDiff.Dependencies)
	}

	return &updateProcedure
}

func (procedure *PSQLProcedure) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := procedure.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (procedure *PSQLProcedure) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	dependencies := []string{}

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
	language, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	if flags&(1<<0) != 0 {
		dependencies, err = decode.DecodePrimitiveSlice[string](buf)
		if err != nil {
			return err
		}
	}
	parameters, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	tag, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	definition, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}

	procedure.Version = *version
	procedure.Name = *name
	procedure.Language = *language
	procedure.Dependencies = dependencies
	procedure.Parameters = *parameters
	procedure.Tag = *tag
	procedure.Definition = *definition
	return nil
}

func (procedure *PSQLProcedure) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, (*string)(pointers.Ptr(entities.PSQLProcedure)))
	encode.EncodeInt(&buf, &PSQLPROCEDURE_VERSION)
	buf.WriteByte(procedure.getByteFlags())
	encode.EncodeString(&buf, &procedure.Name)
	encode.EncodeString(&buf, &procedure.Language)
	encode.EncodePrimitiveSlice(&buf, procedure.Dependencies)
	encode.EncodeString(&buf, &procedure.Parameters)
	encode.EncodeString(&buf, &procedure.Tag)
	encode.EncodeString(&buf, &procedure.Definition)

	return buf.Bytes()
}

func (procedure *PSQLProcedure) getByteFlags() byte {
	var flags byte
	if len(procedure.Dependencies) > 0 {
		flags |= 1 << 0
	}
	return flags
}

type PSQLProcedureDiff struct {
	hash         string
	PrevRef      string
	Language     *string
	Dependencies []string
	Parameters   *string
	Tag          *string
	Definition   *string
}

func (diff *PSQLProcedureDiff) Hash() string {
	return diff.hash
}

func (diff *PSQLProcedureDiff) GetPrevRef() string {
	return diff.PrevRef
}

func (diff *PSQLProcedureDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := diff.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (diff *PSQLProcedureDiff) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	prevRef, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}

	var language, parameters, tag, definition *string
	var dependencies []string

	if flags&(1<<0) != 0 {
		language, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<1) != 0 {
		dependencies, err = decode.DecodePrimitiveSlice[string](buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<2) != 0 {
		parameters, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<3) != 0 {
		tag, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<4) != 0 {
		definition, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}

	diff.PrevRef = *prevRef
	diff.Language = language
	diff.Dependencies = dependencies
	diff.Parameters = parameters
	diff.Tag = tag
	diff.Definition = definition
	return nil
}

func (diff *PSQLProcedureDiff) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, &diff.PrevRef)
	buf.WriteByte(diff.getByteFlags())
	encode.EncodeString(&buf, diff.Language)
	encode.EncodePrimitiveSlice(&buf, diff.Dependencies)
	encode.EncodeString(&buf, diff.Parameters)
	encode.EncodeString(&buf, diff.Tag)
	encode.EncodeString(&buf, diff.Definition)

	return buf.Bytes()
}

func (diff *PSQLProcedureDiff) getByteFlags() byte {
	var flags byte
	if diff.Language != nil {
		flags |= 1 << 0
	}
	if len(diff.Dependencies) > 0 {
		flags |= 1 << 1
	}
	if diff.Parameters != nil {
		flags |= 1 << 2
	}
	if diff.Tag != nil {
		flags |= 1 << 3
	}
	if diff.Definition != nil {
		flags |= 1 << 4
	}
	return flags
}
