package entities

import (
	"bytes"
	"crypto/sha256"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"historydb/src/internal/utils/types"
	"time"
)

var BACKUPSNAPSHOT_VERSION int64 = 1

// BackupSnapshot defines a struct which contains all the data relative to a backup snapshot
//
// Timestamp -> The timestamp when the snapshot was taken
// SnapshotId -> The snapshot identificator
// SchemaDependencies -> Map that links every schema dependency with its dependencies backup files
// Schemas -> Map that links every schema with its schema backup files
// Data -> Map that links every scheam with its data files
// Routines -> Map that links every routine with its backup files
type BackupSnapshot struct {
	Version            int64                               `json:"version"`
	Timestamp          time.Time                           `json:"timestamp"`
	SnapshotId         string                              `json:"snapshotId"`
	Message            string                              `json:"message"`
	SchemaDependencies map[string]string                   `json:"schemaDependencies"`
	Schemas            map[string]string                   `json:"schemas"`
	Data               map[string]BackupSnapshotSchemaData `json:"data"`
	Routines           map[string]string                   `json:"routines"`
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
	if len(snapshot.Routines) > 0 {
		flags |= 1 << 3
	}

	buf.WriteByte(flags)
	encode.EncodeInt(&buf, &BACKUPSNAPSHOT_VERSION)
	encode.EncodeTime(&buf, &snapshot.Timestamp)
	encode.EncodeString(&buf, &snapshot.SnapshotId)
	encode.EncodeString(&buf, &snapshot.Message)
	encode.EncodeMap(&buf, types.ToInterfaceMap(snapshot.SchemaDependencies))
	encode.EncodeMap(&buf, types.ToInterfaceMap(snapshot.Schemas))
	encode.EncodeStructMap(&buf, snapshot.Data)
	encode.EncodeMap(&buf, types.ToInterfaceMap(snapshot.Routines))

	return buf.Bytes()
}

func (snapshot *BackupSnapshot) DecodeFromBytes(data []byte) error {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	version, err := decode.DecodeInt(buf)
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
	message, err := decode.DecodeString(buf)
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
	var routinesMap map[string]string
	if flags&(1<<3) != 0 {
		routines, err := decode.DecodeMap(buf)
		if err != nil {
			return err
		}

		routinesMap, err = types.FromInterfaceMap[string](routines)
		if err != nil {
			return err
		}
	}

	snapshot.Version = *version
	snapshot.Timestamp = *timestamp
	snapshot.SnapshotId = *snapshotId
	snapshot.Message = *message
	snapshot.SchemaDependencies = schemaDependenciesMap
	snapshot.Schemas = schemasMap
	snapshot.Data = dataMap
	snapshot.Routines = routinesMap
	return nil
}

// BackupSnapshotSchemaData defines the info saved from each schema in a snapshot
// that serves to rebuild the schema data.
//
// BatchSize -> The max-size for all batches used to save the schema data.
// ChunkSize -> The max-size for all chunks used to save the schema data.
// Data -> A string of paths that represents all the batch files needed to rebuild the schema data.
type BackupSnapshotSchemaData struct {
	BatchSize int64    `json:"batchSize"`
	ChunkSize int64    `json:"chunkSize"`
	Data      []string `json:"data"`
}

func (schemaData BackupSnapshotSchemaData) EncodeToBytes() []byte {
	var buf bytes.Buffer

	var flags byte
	if len(schemaData.Data) > 0 {
		flags |= 1 << 0
	}

	buf.WriteByte(flags)
	encode.EncodeInt(&buf, &schemaData.BatchSize)
	encode.EncodeInt(&buf, &schemaData.ChunkSize)
	encode.EncodePrimitiveSlice(&buf, schemaData.Data)

	return buf.Bytes()
}

func (schemaData *BackupSnapshotSchemaData) DecodeFromBytes(data []byte) (*BackupSnapshotSchemaData, error) {
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	batchSize, err := decode.DecodeInt(buf)
	if err != nil {
		return nil, err
	}
	chunkSize, err := decode.DecodeInt(buf)
	if err != nil {
		return nil, err
	}
	var dataSlice []string
	if flags&(1<<0) != 0 {
		dataSlice, err = decode.DecodePrimitiveSlice[string](buf)
		if err != nil {
			return nil, err
		}
	}

	return &BackupSnapshotSchemaData{
		BatchSize: *batchSize,
		ChunkSize: *chunkSize,
		Data:      dataSlice,
	}, nil
}
