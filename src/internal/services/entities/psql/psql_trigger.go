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

var PSQLTRIGGER_VERSION int64 = 1

type PSQLTrigger struct {
	Version    int64
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

func (trigger *PSQLTrigger) GetName() string {
	return trigger.Name
}

func (trigger *PSQLTrigger) GetRoutineType() entities.RoutineType {
	return entities.PSQLTrigger
}

func (trigger *PSQLTrigger) GetDependencies() []string {
	return nil
}

func (trigger *PSQLTrigger) Hash() string {
	hash := sha256.Sum256(trigger.encodeData())
	return hex.EncodeToString(hash[:])
}

func (trigger *PSQLTrigger) Diff(routine entities.Routine, isDiff bool) entities.RoutineDiff {
	oldTrigger := routine.(*PSQLTrigger)

	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", routine.Hash())
	} else {
		prevRef = routine.Hash()
	}

	diff := PSQLTriggerDiff{
		hash:    trigger.Hash(),
		PrevRef: prevRef,
	}
	comparation.AssignIfChanged(&diff.Definition, &trigger.Definition, &oldTrigger.Definition)

	return &diff
}

func (trigger *PSQLTrigger) ApplyDiff(diff entities.RoutineDiff) entities.Routine {
	updateTrigger := *trigger
	triggerDiff := diff.(*PSQLTriggerDiff)

	comparation.AssignIfNotNil(&updateTrigger.Definition, triggerDiff.Definition)

	return &updateTrigger
}

func (trigger *PSQLTrigger) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := trigger.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (trigger *PSQLTrigger) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	if _, err := decode.DecodeString(buf); err != nil {
		return err
	}
	version, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	name, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	definition, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}

	trigger.Version = *version
	trigger.Name = *name
	trigger.Definition = *definition
	return nil
}

func (trigger *PSQLTrigger) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, (*string)(pointers.Ptr(entities.PSQLTrigger)))
	encode.EncodeInt(&buf, &PSQLTRIGGER_VERSION)
	encode.EncodeString(&buf, &trigger.Name)
	encode.EncodeString(&buf, &trigger.Definition)

	return buf.Bytes()
}

type PSQLTriggerDiff struct {
	hash       string
	PrevRef    string
	Definition *string
}

func (diff *PSQLTriggerDiff) Hash() string {
	return diff.hash
}

func (diff *PSQLTriggerDiff) GetPrevRef() string {
	return diff.PrevRef
}

func (diff *PSQLTriggerDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := diff.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (diff *PSQLTriggerDiff) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	prevRef, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	definition, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}

	diff.PrevRef = *prevRef
	diff.Definition = definition
	return nil
}

func (diff *PSQLTriggerDiff) encodeData() []byte {
	var buf bytes.Buffer

	encode.EncodeString(&buf, &diff.PrevRef)
	encode.EncodeString(&buf, diff.Definition)

	return buf.Bytes()
}
