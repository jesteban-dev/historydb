package entities

var LIMIT_RECORD_SIZE = 5 * 1024 * 1024 // 5 MB
var MAX_BATCH_LENGTH = 100000

var SMALL_FILE_MAX_SIZE = 20 * 1024 * 1024 // 20 MB
var BIG_FILE_MAX_SIZE = 1024 * 1024 * 1024 // 1 GB

type SchemaRecordMetadata struct {
	Count         int
	MaxRecordSize int
}

type SchemaRecordChunk interface {
}
