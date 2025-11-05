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
	Schemas            map[string]string
	Data               map[string]BackupSnapshotSchemaData
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
	if len(snapshot.Schemas) > 0 {
		flags |= 1 << 1
	}
	if len(snapshot.Data) > 0 {
		flags |= 1 << 2
	}

	buf.WriteByte(flags)
	encode.EncodeTime(&buf, &snapshot.Timestamp)
	encode.EncodeString(&buf, &snapshot.SnapshotId)
	encode.EncodeMap(&buf, types.ToInterfaceMap(snapshot.SchemaDependencies))
	encode.EncodeMap(&buf, types.ToInterfaceMap(snapshot.Schemas))
	encode.EncodeStructMap(&buf, snapshot.Data)

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
	var schemasMap map[string]string
	if flags&(1<<1) != 0 {
		schemas, err := decode.DecodeMap(buf)
		if err != nil {
			return err
		}

		schemasMap, err = types.FromInterfaceMap[string](schemas)
		if err != nil {
			return err
		}
	}
	dataMap := make(map[string]BackupSnapshotSchemaData)
	if flags&(1<<2) != 0 {
		schemaData, err := decode.DecodeStructMap[*BackupSnapshotSchemaData](buf)
		if err != nil {
			return err
		}

		for k, v := range schemaData {
			dataMap[k] = *v
		}
	}

	snapshot.Timestamp = *timestamp
	snapshot.SnapshotId = *snapshotId
	snapshot.SchemaDependencies = schemaDependenciesMap
	snapshot.Schemas = schemasMap
	snapshot.Data = dataMap
	return nil
}

// BackupSnapshotSchemaData defines the info saved from each schema in a snapshot
// that serves to rebuild the schema data.
//
// BatchSize -> The max-size for all batches used to save the schema data.
// ChunkSize -> The max-size for all chunks used to save the schema data.
// Data -> A string of paths that represents all the batch files needed to rebuild the schema data.
type BackupSnapshotSchemaData struct {
	BatchSize int
	ChunkSize int
	Data      []string
}

func (schemaData BackupSnapshotSchemaData) EncodeToBytes() []byte {
	var buf bytes.Buffer

	encode.EncodeInt(&buf, &schemaData.BatchSize)
	encode.EncodeInt(&buf, &schemaData.ChunkSize)
	encode.EncodePrimitiveSlice(&buf, schemaData.Data)

	return buf.Bytes()
}

func (schemaData *BackupSnapshotSchemaData) DecodeFromBytes(data []byte) (*BackupSnapshotSchemaData, error) {
	buf := bytes.NewBuffer(data)

	batchSize, err := decode.DecodeInt(buf)
	if err != nil {
		return nil, err
	}
	chunkSize, err := decode.DecodeInt(buf)
	if err != nil {
		return nil, err
	}
	dataSlice, err := decode.DecodePrimitiveSlice[string](buf)
	if err != nil {
		return nil, err
	}

	return &BackupSnapshotSchemaData{
		BatchSize: *batchSize,
		ChunkSize: *chunkSize,
		Data:      dataSlice,
	}, nil
}
