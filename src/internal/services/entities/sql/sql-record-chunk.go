package sql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/utils/crypto"
	"historydb/src/internal/utils/decode"
	"historydb/src/internal/utils/encode"
	"historydb/src/internal/utils/pointers"
	"math"
)

type SQLChunkCursor struct {
	Offset int
	LastPK interface{}
}

var SQLRECORDCHUNK_VERSION int64 = 1

type SQLRecordChunk struct {
	hash    string
	Version int64
	Content []SQLRecord
}

func (chunk *SQLRecordChunk) Length() int {
	return len(chunk.Content)
}

func (chunk *SQLRecordChunk) Hash() string {
	hash := sha256.Sum256(chunk.encodeData())
	chunk.hash = hex.EncodeToString(hash[:])
	return chunk.hash
}

func (chunk *SQLRecordChunk) GetRecordType() entities.RecordType {
	return entities.SQLRecord
}

func (chunk *SQLRecordChunk) Diff(recordChunk entities.SchemaRecordChunk, isDiff bool) entities.SchemaRecordChunkDiff {
	oldChunk := recordChunk.(*SQLRecordChunk)

	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", recordChunk.Hash())
	} else {
		prevRef = recordChunk.Hash()
	}

	diff := SQLRecordChunkDiff{
		hash:    pointers.Ptr(chunk.Hash()),
		PrevRef: &prevRef,
	}
	records := []SQLRecordDiff{}
	for i := 0; i < int(math.Max(float64(len(oldChunk.Content)), float64(len(chunk.Content)))); i++ {
		if len(chunk.Content) > i && len(oldChunk.Content) > i && !crypto.CompareHashes(chunk.Content[i].Hash(), oldChunk.Content[i].Hash()) {
			records = append(records, SQLRecordDiff{PrevRef: pointers.Ptr(oldChunk.Content[i].Hash()), Record: chunk.Content[i].Content})
		} else if len(oldChunk.Content) <= i && len(chunk.Content) > i {
			records = append(records, SQLRecordDiff{PrevRef: nil, Record: chunk.Content[i].Content})
		}
	}
	diff.Content = records

	return &diff
}

func (chunk *SQLRecordChunk) DiffFromEmpty() entities.SchemaRecordChunkDiff {
	diff := SQLRecordChunkDiff{
		hash: pointers.Ptr(chunk.Hash()),
	}
	records := []SQLRecordDiff{}
	for _, record := range chunk.Content {
		records = append(records, SQLRecordDiff{PrevRef: nil, Record: record.Content})
	}
	diff.Content = records

	return &diff
}

func (chunk *SQLRecordChunk) DiffToEmpty(isDiff bool) entities.SchemaRecordChunkDiff {
	var prevRef string
	if isDiff {
		prevRef = fmt.Sprintf("diffs/%s", chunk.Hash())
	} else {
		prevRef = chunk.Hash()
	}

	return &SQLRecordChunkDiff{
		PrevRef: &prevRef,
	}
}

func (chunk *SQLRecordChunk) ApplyDiff(diff entities.SchemaRecordChunkDiff) entities.SchemaRecordChunk {
	updateChunk := *chunk
	chunkDiff := diff.(*SQLRecordChunkDiff)

	for _, v := range chunkDiff.Content {
		if v.PrevRef != nil {
			currentIndex := -1
			for i, j := range updateChunk.Content {
				if crypto.CompareHashes(j.Hash(), *v.PrevRef) {
					currentIndex = i
					break
				}
			}
			if currentIndex == -1 {
				return nil
			}

			if v.Record != nil {
				updateChunk.Content[currentIndex].Content = v.Record
			} else {
				updateChunk.Content = append(updateChunk.Content[:currentIndex], updateChunk.Content[currentIndex+1:]...)
			}
		} else {
			updateChunk.Content = append(updateChunk.Content, SQLRecord{Content: v.Record})
		}
	}

	return &updateChunk
}

func (chunk *SQLRecordChunk) EncodeToBytes() []byte {
	var buf bytes.Buffer
	var auxBuf bytes.Buffer
	encodedData := chunk.encodeData()

	encode.EncodeString(&auxBuf, &chunk.hash)
	auxBuf.Write(encodedData)

	encode.EncodeInt(&buf, pointers.Ptr(int64(len(auxBuf.Bytes())))) // Saves buffer size so then we can extract the full buffer
	buf.Write(auxBuf.Bytes())

	return buf.Bytes()
}

func (chunk *SQLRecordChunk) DecodeFromBytes(data []byte) error {
	// Here it uses the buffer reader from the buffer size, so there is not that integer in the buffer
	buf := bytes.NewBuffer(data)

	hash, err := decode.DecodeString(buf)
	if err != nil {
		return err
	}
	version, err := decode.DecodeInt(buf)
	if err != nil {
		return err
	}
	var contentSlice []SQLRecord
	content, err := decode.DecodeSlice[*SQLRecord](buf)
	if err != nil {
		return err
	}

	for _, v := range content {
		contentSlice = append(contentSlice, *v)
	}

	chunk.hash = *hash
	chunk.Version = *version
	chunk.Content = contentSlice
	return nil
}

func (chunk *SQLRecordChunk) encodeData() []byte {
	var buf bytes.Buffer
	encode.EncodeInt(&buf, &SQLRECORDCHUNK_VERSION)
	encode.EncodeSlice(&buf, chunk.Content)

	encodedData := buf.Bytes()

	hash := sha256.Sum256(encodedData)
	chunk.hash = hex.EncodeToString(hash[:])

	return buf.Bytes()
}

type SQLRecordChunkDiff struct {
	hash    *string
	PrevRef *string
	Content []SQLRecordDiff
}

func (diff *SQLRecordChunkDiff) Length() int {
	return len(diff.Content)
}

func (diff *SQLRecordChunkDiff) Hash() *string {
	return diff.hash
}

func (diff *SQLRecordChunkDiff) GetPrevRef() *string {
	return diff.PrevRef
}

func (diff *SQLRecordChunkDiff) GetRecordType() entities.RecordType {
	return entities.SQLRecord
}

func (diff *SQLRecordChunkDiff) ApplyDiffFromEmpty() entities.SchemaRecordChunk {
	var emptyChunk SQLRecordChunk
	chunk := emptyChunk.ApplyDiff(diff)
	return chunk
}

func (diff *SQLRecordChunkDiff) EncodeToBytes() []byte {
	var buf bytes.Buffer
	encodedData := diff.encodeData()

	encode.EncodeInt(&buf, pointers.Ptr(int64(len(encodedData)))) // Saves buffer size so then we can extract the full buffer
	buf.Write(encodedData)

	return buf.Bytes()
}

func (diff *SQLRecordChunkDiff) DecodeFromBytes(data []byte) error {
	// Here it uses the buffer reader from the buffer size, so there is not that integer in the buffer
	buf := bytes.NewBuffer(data)

	flags, err := buf.ReadByte()
	if err != nil {
		return err
	}
	var hash *string
	if flags&(1<<0) != 0 {
		hash, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	var prevRef *string
	if flags&(1<<1) != 0 {
		prevRef, err = decode.DecodeString(buf)
		if err != nil {
			return err
		}
	}
	var contentSlice []SQLRecordDiff
	if flags&(1<<2) != 0 {
		content, err := decode.DecodeSlice[*SQLRecordDiff](buf)
		if err != nil {
			return nil
		}

		for _, v := range content {
			contentSlice = append(contentSlice, *v)
		}
	}

	diff.hash = hash
	diff.PrevRef = prevRef
	diff.Content = contentSlice
	return nil
}

func (diff *SQLRecordChunkDiff) encodeData() []byte {
	var buf bytes.Buffer

	buf.WriteByte(diff.getByteFlags())
	encode.EncodeString(&buf, diff.hash)
	encode.EncodeString(&buf, diff.PrevRef)
	encode.EncodeSlice(&buf, diff.Content)

	return buf.Bytes()
}

func (diff *SQLRecordChunkDiff) getByteFlags() byte {
	var flags byte

	if diff.hash != nil {
		flags |= 1 << 0
	}
	if diff.PrevRef != nil {
		flags |= 1 << 1
	}
	if len(diff.Content) != 0 {
		flags |= 1 << 2
	}

	return flags
}
