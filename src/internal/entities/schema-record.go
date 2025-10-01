package entities

type RecordType string

const (
	SQLRecord RecordType = "SQLRecord"
)

var LIMIT_RECORD_SIZE = 5 * 1024 * 1024 // 5 MB
var MAX_BATCH_LENGTH = 100000

var SMALL_FILE_MAX_SIZE = 20 * 1024 * 1024 // 20 MB
var BIG_FILE_MAX_SIZE = 1024 * 1024 * 1024 // 1 GB

// SchemaRecordMetadata is a struct used to retrive basic info about the schema records in the DB.
type SchemaRecordMetadata struct {
	Count         int
	MaxRecordSize int
}

// SchemaRecordChunk is the main entity used to represent a chunk of schema records.
//
// Length() -> Retrieves the number of items the chunk hash.
// Hash() -> Returns the chunk signature
// Diff() -> Returns the differences that has our chunk comparing it with the parameter older chunk
// ApplyDiff() -> Returns a new chunk applying the differences to our chunk
// EncodeToBytes() -> Encodes the entity into a []byte
// DecodeFromBytes() -> Decode the entity from []byte
type SchemaRecordChunk interface {
	Length() int
	Hash() string
	Diff(chunk SchemaRecordChunk, isDiff bool) SchemaRecordChunkDiff
	ApplyDiff(diff SchemaRecordChunkDiff) SchemaRecordChunk
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}

// SchemaRecordChunkDiff is the main entity used to represent a chunk diff of schema records.
//
// Length() -> Retrieves the number of items the chunk hash.
// Hash() -> Returns the chunk signature
// EncodeToBytes() -> Encodes the entity into a []byte
// DecodeFromBytes() -> Decode the entity from []byte
type SchemaRecordChunkDiff interface {
	Length() int
	Hash() *string
	GetPrevRef() *string
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
	ApplyDiffFromEmpty() SchemaRecordChunk
}
