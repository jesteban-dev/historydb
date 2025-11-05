package entities

import (
	"bytes"
	"crypto/sha256"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"time"
)

// BackupMetadata defines a struct which contains all the basic data required by the app
//
// DatabaseEngine -> The DB Engine used in the backup
// Snapshots -> List of all snapshots taken in the backup
type BackupMetadata struct {
	DatabaseEngine string
	Snapshots      []BackupMetadataSnapshot
}

func (metadata *BackupMetadata) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := metadata.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (metadata *BackupMetadata) encodeData() []byte {
	var buf bytes.Buffer

	var flags byte
	if len(metadata.Snapshots) > 0 {
		flags |= 1 << 0
	}

	buf.WriteByte(flags)
	encode.EncodeString(&buf, &metadata.DatabaseEngine)
	encode.EncodeSlice(&buf, metadata.Snapshots)

	return buf.Bytes()
}

func (metadata *BackupMetadata) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	engine, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	var snapshotSlice []BackupMetadataSnapshot
	if flags&(1<<0) != 0 {
		snapshots, err := decode.DecodeSlice[*BackupMetadataSnapshot](buf)
		if err != nil {
			return err
		}

		snapshotSlice = make([]BackupMetadataSnapshot, 0, len(snapshots))
		for _, v := range snapshots {
			snapshotSlice = append(snapshotSlice, *v)
		}
	}

	metadata.DatabaseEngine = *engine
	metadata.Snapshots = snapshotSlice
	return nil
}

// BackupMetadataSnapshot defines the basic info of a struct saved into the BackupMetadata struct to recover the snapshots
//
// Timestamp -> The timestamp when the snapshot was taken
// SnapshotId -> The snapshot identificator
type BackupMetadataSnapshot struct {
	Timestamp  time.Time
	SnapshotId string
}

func (snapshot BackupMetadataSnapshot) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encode.EncodeTime(&buf, &snapshot.Timestamp)
	encode.EncodeString(&buf, &snapshot.SnapshotId)

	return buf.Bytes()
}

func (snapshot *BackupMetadataSnapshot) DecodeFromBytes(data []byte) (*BackupMetadataSnapshot, error) {
	buf := bytes.NewBuffer(data)

	timestamp, err := decode.DecodeTime(buf)
	if err != nil {
		return nil, err
	}
	snapshotId, err := decode.DecodeString(buf)
	if err != nil {
		return nil, err
	}

	if snapshot == nil {
		return &BackupMetadataSnapshot{
			Timestamp:  *timestamp,
			SnapshotId: *snapshotId,
		}, nil
	} else {
		snapshot.Timestamp = *timestamp
		snapshot.SnapshotId = *snapshotId
		return nil, nil
	}
}
