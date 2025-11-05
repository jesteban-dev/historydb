package dtos

import "historydb/src/internal/entities"

type BatchChunkInfo struct {
	Hash   string
	Cursor entities.ChunkCursor
}
