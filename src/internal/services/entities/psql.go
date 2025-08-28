package entities

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"historydb/src/internal/entities"
	"historydb/src/internal/helpers"
)

type PSQLTableSequence struct {
	Name      string  `json:"name,omitempty"`
	Type      *string `json:"type,omitempty"`
	Start     *int    `json:"start,omitempty"`
	Min       *int    `json:"min,omitempty"`
	Max       *int    `json:"max,omitempty"`
	Increment *int    `json:"increment,omitempty"`
	IsCycle   *bool   `json:"isCycle,omitempty"`
	LastValue *int    `json:"lastValue,omitempty"`
	IsCalled  *bool   `json:"isCalled,omitempty"`
}

func (seq PSQLTableSequence) GetName() string {
	return seq.Name
}

func (seq PSQLTableSequence) Hash() (string, error) {
	json, err := json.Marshal(seq)
	if err != nil {
		return "", err
	}

	hashValue := sha256.Sum256(json)
	return hex.EncodeToString(hashValue[:]), nil
}

func (seq PSQLTableSequence) Diff(dependency entities.SchemaDependency) entities.SchemaDependencyDiff {
	diff := PSQLTableSequenceDiff{}
	oldSeq := dependency.(PSQLTableSequence)

	diff.dependencyHash, _ = seq.Hash()
	diff.PrevRef, _ = dependency.Hash()

	helpers.AssignIfChanged(&diff.Type, seq.Type, oldSeq.Type)
	helpers.AssignIfChanged(&diff.Start, seq.Start, oldSeq.Start)
	helpers.AssignIfChanged(&diff.Min, seq.Min, oldSeq.Min)
	helpers.AssignIfChanged(&diff.Max, seq.Max, oldSeq.Max)
	helpers.AssignIfChanged(&diff.Increment, seq.Increment, oldSeq.Increment)
	helpers.AssignIfChanged(&diff.IsCycle, seq.IsCycle, oldSeq.IsCycle)
	helpers.AssignIfChanged(&diff.LastValue, seq.LastValue, oldSeq.LastValue)
	helpers.AssignIfChanged(&diff.IsCalled, seq.IsCalled, oldSeq.IsCalled)

	return diff
}

func (seq PSQLTableSequence) ApplyDiff(diff entities.SchemaDependencyDiff) entities.SchemaDependency {
	updateSeq := seq
	seqDiff := diff.(PSQLTableSequenceDiff)

	helpers.AssignIfChanged(&updateSeq.Type, seqDiff.Type, nil)
	helpers.AssignIfChanged(&updateSeq.Start, seqDiff.Start, nil)
	helpers.AssignIfChanged(&updateSeq.Min, seqDiff.Min, nil)
	helpers.AssignIfChanged(&updateSeq.Max, seqDiff.Max, nil)
	helpers.AssignIfChanged(&updateSeq.Increment, seqDiff.Increment, nil)
	helpers.AssignIfChanged(&updateSeq.IsCycle, seqDiff.IsCycle, nil)
	helpers.AssignIfChanged(&updateSeq.LastValue, seqDiff.LastValue, nil)
	helpers.AssignIfChanged(&updateSeq.IsCalled, seqDiff.IsCalled, nil)

	return updateSeq
}

type PSQLTableSequenceDiff struct {
	dependencyHash string  `json:"-"`
	PrevRef        string  `json:"prevRef"`
	Type           *string `json:"type,omitempty"`
	Start          *int    `json:"start,omitempty"`
	Min            *int    `json:"min,omitempty"`
	Max            *int    `json:"max,omitempty"`
	Increment      *int    `json:"increment,omitempty"`
	IsCycle        *bool   `json:"isCycle,omitempty"`
	LastValue      *int    `json:"lastValue,omitempty"`
	IsCalled       *bool   `json:"isCalled,omitempty"`
}

func (dependencyDiff PSQLTableSequenceDiff) GetDependencyHash() string {
	return dependencyDiff.dependencyHash
}
