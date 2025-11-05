package sql

type SQLChunkCursor struct {
	Offset int
	LastPK interface{}
}

type SQLRecordChunk struct {
	Content []SQLRecord
}

type SQLRecord map[string]interface{}
