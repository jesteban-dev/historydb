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
)

var PSQLFUNCTION_VERSION int64 = 1

type PSQLFunction struct {
	Version      int64
	Name         string
	Language     string
	Volatility   *string
	Dependencies []string
	Parameters   *string
	ReturnType   string
	Tag          string
	Definition   string
}

func (function *PSQLFunction) GetName() string {
	return function.Name
}

func (function *PSQLFunction) GetRoutineType() entities.RoutineType {
	return entities.PSQLFunction
}

func (function *PSQLFunction) Hash() string {
	hash := sha256.Sum256(function.encodeData())
	return hex.EncodeToString(hash[:])
}

func (function *PSQLFunction) Diff(routine entities.Routine, isDiff bool) entities.RoutineDiff {
	oldFunction := routine.(*PSQLFunction)

	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", routine.Hash())
	} else {
		prevRef = routine.Hash()
	}

	diff := PSQLFunctionDiff{
		hash:    function.Hash(),
		PrevRef: prevRef,
	}
	comparation.AssignIfChanged(&diff.Language, &function.Language, &oldFunction.Language)
	comparation.AssignIfChanged(&diff.Volatility, function.Volatility, oldFunction.Volatility)
	comparation.AssignIfChangedSlice(diff.Dependencies, function.Dependencies, oldFunction.Dependencies)
	comparation.AssignIfChanged(&diff.Parameters, function.Parameters, oldFunction.Parameters)
	comparation.AssignIfChanged(&diff.ReturnType, &function.ReturnType, &function.ReturnType)
	comparation.AssignIfChanged(&diff.Tag, &function.Tag, &oldFunction.Tag)
	comparation.AssignIfChanged(&diff.Definition, &function.Definition, &oldFunction.Definition)

	return &diff
}

func (function *PSQLFunction) ApplyDiff(diff entities.RoutineDiff) entities.Routine {
	updateFunction := *function
	functionDiff := diff.(*PSQLFunctionDiff)

	comparation.AssignIfNotNil(&updateFunction.Language, functionDiff.Language)
	comparation.AssignIfNotNil(updateFunction.Volatility, functionDiff.Volatility)
	comparation.AssignSliceIfNotNil(updateFunction.Dependencies, functionDiff.Dependencies)
	comparation.AssignIfNotNil(updateFunction.Parameters, functionDiff.Parameters)
	comparation.AssignIfNotNil(&updateFunction.ReturnType, functionDiff.ReturnType)
	comparation.AssignIfNotNil(&updateFunction.Tag, functionDiff.Tag)
	comparation.AssignIfNotNil(&updateFunction.Definition, functionDiff.Definition)

	return &updateFunction
}

func (function *PSQLFunction) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := function.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (function *PSQLFunction) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	var volatility *string
	var dependencies []string

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
		return nil
	}
	language, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	if flags&(1<<0) != 0 {
		volatility, err = decode.DecodeString(buf)
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
	parameters, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	returnType, err := decode.DecodeString(buf)
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

	function.Version = *version
	function.Name = *name
	function.Language = *language
	function.Volatility = volatility
	function.Dependencies = dependencies
	function.Parameters = parameters
	function.ReturnType = *returnType
	function.Tag = *tag
	function.Definition = *definition
	return nil
}

func (function *PSQLFunction) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, (*string)(pointers.Ptr(entities.PSQLFunction)))
	encode.EncodeInt(&buf, &PSQLFUNCTION_VERSION)
	buf.WriteByte(function.getByteFlags())
	encode.EncodeString(&buf, &function.Name)
	encode.EncodeString(&buf, &function.Language)
	encode.EncodeString(&buf, function.Volatility)
	encode.EncodePrimitiveSlice(&buf, function.Dependencies)
	encode.EncodeString(&buf, function.Parameters)
	encode.EncodeString(&buf, &function.ReturnType)
	encode.EncodeString(&buf, &function.Tag)
	encode.EncodeString(&buf, &function.Definition)

	return buf.Bytes()
}

func (function *PSQLFunction) getByteFlags() byte {
	var flags byte
	if function.Volatility != nil {
		flags |= 1 << 0
	}
	if len(function.Dependencies) > 0 {
		flags |= 1 << 1
	}
	return flags
}

type PSQLFunctionDiff struct {
	hash         string
	PrevRef      string
	Language     *string
	Volatility   *string
	Dependencies []string
	Parameters   *string
	ReturnType   *string
	Tag          *string
	Definition   *string
}

func (diff *PSQLFunctionDiff) Hash() string {
	return diff.hash
}

func (diff *PSQLFunctionDiff) GetPrevRef() string {
	return diff.PrevRef
}

func (diff *PSQLFunctionDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := diff.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (diff *PSQLFunctionDiff) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	prevRef, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	var language, volatility, parameters, returnType, tag, definition *string
	var dependencies []string

	if flags&(1<<0) != 0 {
		language, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<1) != 0 {
		volatility, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<2) != 0 {
		dependencies, err = decode.DecodePrimitiveSlice[string](buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<3) != 0 {
		parameters, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<4) != 0 {
		returnType, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<5) != 0 {
		tag, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	if flags&(1<<6) != 0 {
		definition, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}

	diff.PrevRef = *prevRef
	diff.Language = language
	diff.Volatility = volatility
	diff.Dependencies = dependencies
	diff.Parameters = parameters
	diff.ReturnType = returnType
	diff.Tag = tag
	diff.Definition = definition
	return nil
}

func (diff *PSQLFunctionDiff) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, &diff.PrevRef)
	buf.WriteByte(diff.getByteFlags())
	encode.EncodeString(&buf, diff.Language)
	encode.EncodeString(&buf, diff.Volatility)
	encode.EncodePrimitiveSlice(&buf, diff.Dependencies)
	encode.EncodeString(&buf, diff.Parameters)
	encode.EncodeString(&buf, diff.ReturnType)
	encode.EncodeString(&buf, diff.Tag)
	encode.EncodeString(&buf, diff.Definition)

	return buf.Bytes()
}

func (diff *PSQLFunctionDiff) getByteFlags() byte {
	var flags byte
	if diff.Language != nil {
		flags |= 1 << 0
	}
	if diff.Volatility != nil {
		flags |= 1 << 1
	}
	if len(diff.Dependencies) > 0 {
		flags |= 1 << 2
	}
	if diff.Parameters != nil {
		flags |= 1 << 3
	}
	if diff.ReturnType != nil {
		flags |= 1 << 4
	}
	if diff.Tag != nil {
		flags |= 1 << 5
	}
	if diff.Definition != nil {
		flags |= 1 << 6
	}
	return flags
}
