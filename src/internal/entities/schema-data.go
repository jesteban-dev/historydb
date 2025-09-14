package entities

var LIMIT_DATA_SIZE = 10 * 1024 * 1024 // 10 MB
var MAX_BATCH_LENGTH = 100000

var SMALL_FILE_SIZE = 20 * 1024 * 1024 // 20 MB
var BIG_FILE_SIZE = 1024 * 1024 * 1024 // 1 GB

type SchemaDataChunk interface {
	Hash() (string, error)
	HashContent() error
	Length() int
}

// SchemaData is an interface that represents a single data/row inside a database scheme.
type SchemaData interface {
	Hash() (string, error)
}

// ChunkCursor is an interface that represents a Cursor to make chunk queries to a database scheme.
type ChunkCursor interface{}
