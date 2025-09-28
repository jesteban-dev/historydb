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
)

const (
	CURRENT_VERSION string = "1"
)

type PSQLSequence struct {
	DependencyType entities.DependencyType
	Version        string
	Name           string
	Type           string
	Start          int
	Min            int
	Max            int
	Increment      int
	IsCycle        bool
	LastValue      int
	IsCalled       bool
}

func (seq *PSQLSequence) GetDependencyType() entities.DependencyType {
	return seq.DependencyType
}

func (seq *PSQLSequence) GetName() string {
	return seq.Name
}

func (seq *PSQLSequence) Hash() string {
	hash := sha256.Sum256(seq.encodeData())
	return hex.EncodeToString(hash[:])
}

func (seq *PSQLSequence) Diff(dependency entities.SchemaDependency, isDiff bool) entities.SchemaDependencyDiff {
	oldSeq := dependency.(*PSQLSequence)

	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", dependency.Hash())
	} else {
		prevRef = dependency.Hash()
	}

	diff := PSQLSequenceDiff{
		hash:    seq.Hash(),
		PrevRef: prevRef,
	}
	comparation.AssignIfChanged(&diff.Type, &seq.Type, &oldSeq.Type)
	comparation.AssignIfChanged(&diff.Start, &seq.Start, &oldSeq.Start)
	comparation.AssignIfChanged(&diff.Min, &seq.Min, &oldSeq.Min)
	comparation.AssignIfChanged(&diff.Max, &seq.Max, &oldSeq.Max)
	comparation.AssignIfChanged(&diff.Increment, &seq.Increment, &oldSeq.Increment)
	comparation.AssignIfChanged(&diff.IsCycle, &seq.IsCycle, &oldSeq.IsCycle)
	comparation.AssignIfChanged(&diff.LastValue, &seq.LastValue, &oldSeq.LastValue)
	comparation.AssignIfChanged(&diff.IsCalled, &seq.IsCalled, &oldSeq.IsCalled)

	return &diff
}

func (seq *PSQLSequence) ApplyDiff(diff entities.SchemaDependencyDiff) entities.SchemaDependency {
	updateSeq := *seq
	seqDiff := diff.(*PSQLSequenceDiff)

	comparation.AssignIfNotNil(&updateSeq.Type, seqDiff.Type)
	comparation.AssignIfNotNil(&updateSeq.Start, seqDiff.Start)
	comparation.AssignIfNotNil(&updateSeq.Min, seqDiff.Min)
	comparation.AssignIfNotNil(&updateSeq.Max, seqDiff.Max)
	comparation.AssignIfNotNil(&updateSeq.Increment, seqDiff.Increment)
	comparation.AssignIfNotNil(&updateSeq.IsCycle, seqDiff.IsCycle)
	comparation.AssignIfNotNil(&updateSeq.LastValue, seqDiff.LastValue)
	comparation.AssignIfNotNil(&updateSeq.IsCalled, seqDiff.IsCalled)

	return &updateSeq
}

func (seq *PSQLSequence) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := seq.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (seq *PSQLSequence) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	dependencyType, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	version, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	name, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	typeData, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	start, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	min, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	max, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	increment, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	isCycle, err := decode.DecodeBool(buf)
	if err != nil {
		return err
	}
	lastValue, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	isCalled, err := decode.DecodeBool(buf)
	if err != nil {
		return err
	}

	seq.DependencyType = entities.DependencyType(*dependencyType)
	seq.Version = *version
	seq.Name = *name
	seq.Type = *typeData
	seq.Start = *start
	seq.Min = *min
	seq.Max = *max
	seq.Increment = *increment
	seq.IsCycle = *isCycle
	seq.LastValue = *lastValue
	seq.IsCalled = *isCalled
	return nil
}

func (seq *PSQLSequence) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, (*string)(&seq.DependencyType))
	encode.EncodeString(&buf, &seq.Version)
	encode.EncodeString(&buf, &seq.Name)
	encode.EncodeString(&buf, &seq.Type)
	encode.EncodeInt(&buf, &seq.Start)
	encode.EncodeInt(&buf, &seq.Min)
	encode.EncodeInt(&buf, &seq.Max)
	encode.EncodeInt(&buf, &seq.Increment)
	encode.EncodeBool(&buf, &seq.IsCycle)
	encode.EncodeInt(&buf, &seq.LastValue)
	encode.EncodeBool(&buf, &seq.IsCalled)

	return buf.Bytes()
}

type PSQLSequenceDiff struct {
	hash      string
	PrevRef   string
	Type      *string
	Start     *int
	Min       *int
	Max       *int
	Increment *int
	IsCycle   *bool
	LastValue *int
	IsCalled  *bool
}

func (diff *PSQLSequenceDiff) Hash() string {
	return diff.hash
}

func (diff *PSQLSequenceDiff) GetPrevRef() string {
	return diff.PrevRef
}

func (diff *PSQLSequenceDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := diff.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (diff *PSQLSequenceDiff) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	prevRef, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	var typeData *string
	if flags&(1<<0) != 0 {
		typeData, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	var start *int
	if flags&(1<<1) != 0 {
		start, err = decode.DecodeInt(buf)
		if err != nil {
			return err
		}
	}
	var min *int
	if flags&(1<<2) != 0 {
		min, err = decode.DecodeInt(buf)
		if err != nil {
			return err
		}
	}
	var max *int
	if flags&(1<<3) != 0 {
		max, err = decode.DecodeInt(buf)
		if err != nil {
			return err
		}
	}
	var increment *int
	if flags&(1<<4) != 0 {
		increment, err = decode.DecodeInt(buf)
		if err != nil {
			return err
		}
	}
	var isCycle *bool
	if flags&(1<<5) != 0 {
		isCycle, err = decode.DecodeBool(buf)
		if err != nil {
			return err
		}
	}
	var lastValue *int
	if flags&(1<<6) != 0 {
		lastValue, err = decode.DecodeInt(buf)
		if err != nil {
			return err
		}
	}
	var isCalled *bool
	if flags&(1<<7) != 0 {
		isCalled, err = decode.DecodeBool(buf)
		if err != nil {
			return err
		}
	}

	diff.PrevRef = *prevRef
	diff.Type = typeData
	diff.Start = start
	diff.Min = min
	diff.Max = max
	diff.Increment = increment
	diff.IsCycle = isCycle
	diff.LastValue = lastValue
	diff.IsCalled = isCalled
	return nil
}

func (diff *PSQLSequenceDiff) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, &diff.PrevRef)
	buf.WriteByte(diff.getByteFlags())
	encode.EncodeString(&buf, diff.Type)
	encode.EncodeInt(&buf, diff.Start)
	encode.EncodeInt(&buf, diff.Min)
	encode.EncodeInt(&buf, diff.Max)
	encode.EncodeInt(&buf, diff.Increment)
	encode.EncodeBool(&buf, diff.IsCycle)
	encode.EncodeInt(&buf, diff.LastValue)
	encode.EncodeBool(&buf, diff.IsCalled)

	return buf.Bytes()
}

func (diff *PSQLSequenceDiff) getByteFlags() byte {
	var flags byte
	if diff.Type != nil {
		flags |= 1 << 0
	}
	if diff.Start != nil {
		flags |= 1 << 1
	}
	if diff.Min != nil {
		flags |= 1 << 2
	}
	if diff.Max != nil {
		flags |= 1 << 3
	}
	if diff.Increment != nil {
		flags |= 1 << 4
	}
	if diff.IsCycle != nil {
		flags |= 1 << 5
	}
	if diff.LastValue != nil {
		flags |= 1 << 6
	}
	if diff.IsCalled != nil {
		flags |= 1 << 7
	}
	return flags
}
