package psql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"historydb/src/internal/entities"
	"historydb/src/internal/utils/comparation"
	"historydb/src/internal/utils/encode"
)

const (
	CURRENT_VERSION string = "1"
)

type PSQLSequence struct {
	hash           string
	DependencyType entities.DependencyType
	Version        string
	Name           string
	Type           *string
	Start          *int
	Min            *int
	Max            *int
	Increment      *int
	IsCycle        *bool
	LastValue      *int
	IsCalled       *bool
}

func (seq *PSQLSequence) GetName() string {
	return seq.Name
}

func (seq *PSQLSequence) Hash() string {
	if seq.hash == "" {
		hash := sha256.Sum256(seq.encodeData())
		seq.hash = hex.EncodeToString(hash[:])
	}
	return seq.hash
}

func (seq *PSQLSequence) Diff(dependency entities.SchemaDependency) entities.SchemaDependencyDiff {
	oldSeq := dependency.(*PSQLSequence)

	diff := PSQLSequenceDiff{
		sequenceHash: seq.Hash(),
		PrevRef:      dependency.Hash(),
	}
	comparation.AssignIfChanged(&diff.Type, seq.Type, oldSeq.Type)
	comparation.AssignIfChanged(&diff.Start, seq.Start, oldSeq.Start)
	comparation.AssignIfChanged(&diff.Min, seq.Min, oldSeq.Min)
	comparation.AssignIfChanged(&diff.Max, seq.Max, oldSeq.Max)
	comparation.AssignIfChanged(&diff.Increment, seq.Increment, oldSeq.Increment)
	comparation.AssignIfChanged(&diff.IsCycle, seq.IsCycle, oldSeq.IsCycle)
	comparation.AssignIfChanged(&diff.LastValue, seq.LastValue, oldSeq.LastValue)
	comparation.AssignIfChanged(&diff.IsCalled, seq.IsCalled, oldSeq.IsCalled)

	return &diff
}

func (seq *PSQLSequence) ApplyDiff(diff entities.SchemaDependencyDiff) entities.SchemaDependency {
	updateSeq := *seq
	seqDiff := diff.(*PSQLSequenceDiff)

	comparation.AssignIfChanged(&updateSeq.Type, seqDiff.Type, nil)
	comparation.AssignIfChanged(&updateSeq.Start, seqDiff.Start, nil)
	comparation.AssignIfChanged(&updateSeq.Min, seqDiff.Min, nil)
	comparation.AssignIfChanged(&updateSeq.Max, seqDiff.Max, nil)
	comparation.AssignIfChanged(&updateSeq.Increment, seqDiff.Increment, nil)
	comparation.AssignIfChanged(&updateSeq.IsCycle, seqDiff.IsCycle, nil)
	comparation.AssignIfChanged(&updateSeq.LastValue, seqDiff.LastValue, nil)
	comparation.AssignIfChanged(&updateSeq.IsCalled, seqDiff.IsCalled, nil)

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

func (seq *PSQLSequence) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, (*string)(&seq.DependencyType))
	encode.EncodeString(&buf, &seq.Version)
	buf.WriteByte(seq.getByteFlags())
	encode.EncodeString(&buf, &seq.Name)
	encode.EncodeString(&buf, seq.Type)
	encode.EncodeInt(&buf, seq.Start)
	encode.EncodeInt(&buf, seq.Min)
	encode.EncodeInt(&buf, seq.Max)
	encode.EncodeInt(&buf, seq.Increment)
	encode.EncodeBool(&buf, seq.IsCycle)
	encode.EncodeInt(&buf, seq.LastValue)
	encode.EncodeBool(&buf, seq.IsCalled)

	return buf.Bytes()
}

func (seq *PSQLSequence) getByteFlags() byte {
	var flags byte
	if seq.Type != nil {
		flags |= 1 << 0
	}
	if seq.Start != nil {
		flags |= 1 << 1
	}
	if seq.Min != nil {
		flags |= 1 << 2
	}
	if seq.Max != nil {
		flags |= 1 << 3
	}
	if seq.Increment != nil {
		flags |= 1 << 4
	}
	if seq.IsCycle != nil {
		flags |= 1 << 5
	}
	if seq.LastValue != nil {
		flags |= 1 << 6
	}
	if seq.IsCalled != nil {
		flags |= 1 << 7
	}
	return flags
}

type PSQLSequenceDiff struct {
	sequenceHash string
	PrevRef      string
	Type         *string
	Start        *int
	Min          *int
	Max          *int
	Increment    *int
	IsCycle      *bool
	LastValue    *int
	IsCalled     *bool
}

func (diff *PSQLSequenceDiff) GetSchemaDependencyHash() string {
	return diff.sequenceHash
}

func (diff *PSQLSequenceDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := diff.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
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
