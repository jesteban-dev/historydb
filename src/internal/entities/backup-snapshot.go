package entities

import (
	"bytes"
	"crypto/sha256"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"historydb/src/internal/utils/types"
	"time"
)

// BackupSnapshot defines the main info for all the snapshots taken in the backup
//
// Id -> Snaphost Id
// Timestamp -> Timestamp the snapshot was taken
// Schemas -> map that links every schema with its schema backup file
// Data -> map that link every schema with its schema backup data files
type BackupSnapshot struct {
	Timestamp          time.Time
	SnapshotId         string
	SchemaDependencies map[string]string
	//Schemas            map[string]string                   `json:"schemas"`
	//Data               map[string]BackupSnapshotSchemaData `json:"data"`
}

func (snapshot *BackupSnapshot) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encodedData := snapshot.encodeData()
	integrityHash := sha256.Sum256(encodedData)

	buf.Write(integrityHash[:])
	buf.Write(encodedData)

	return buf.Bytes()
}

func (snapshot *BackupSnapshot) encodeData() []byte {
	var buf bytes.Buffer

	var flags byte
	if len(snapshot.SchemaDependencies) > 0 {
		flags |= 1 << 0
	}

	buf.WriteByte(flags)
	encode.EncodeTime(&buf, &snapshot.Timestamp)
	encode.EncodeString(&buf, &snapshot.SnapshotId)
	encode.EncodeMap(&buf, types.ToInterfaceMap(snapshot.SchemaDependencies))

	return buf.Bytes()
}

func (snapshot *BackupSnapshot) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	timestamp, err := decode.DecodeTime(buf)
	if err != nil {
		return err
	}
	snapshotId, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	var schemaDependenciesMap map[string]string
	if flags&(1<<0) != 0 {
		schemaDependencies, err := decode.DecodeMap(buf)
		if err != nil {
			return err
		}

		schemaDependenciesMap, err = types.FromInterfaceMap[string](schemaDependencies)
		if err != nil {
			return err
		}
	}

	snapshot.Timestamp = *timestamp
	snapshot.SnapshotId = *snapshotId
	snapshot.SchemaDependencies = schemaDependenciesMap
	return nil
}

// BackupSnapshotSchemaData defines the info saved from each schema in a snapshot
// that serves to rebuild the schema data.
//
// BatchSize -> The max-size for all batches used to save the schema data.
// ChunkSize -> The max-size for all chunks used to save the schema data.
// Data -> A string of paths that represents all the batch files needed to rebuild the schema data.
/*type BackupSnapshotSchemaData struct {
	BatchSize int      `json:"batchSize"`
	ChunkSize int      `json:"chunkSize"`
	Data      []string `json:"data"`
}*/
