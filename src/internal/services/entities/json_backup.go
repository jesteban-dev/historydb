package entities

type JSONRefData struct {
	PrevRef *string `json:"prevRef"`
}

type JSONSchemaData struct {
	JSONRefData
	SchemaType SchemaType `json:"schemaType"`
}

type JSONDataChunkRef struct {
	SchemaType SchemaType            `json:"schemaType"`
	Hash       string                `json:"hash"`
	PrevRef    *JSONDataChunkPrevRef `json:"prevRef"`
}

type JSONDataChunkPrevRef struct {
	Batch string `json:"batch"`
	Chunk string `json:"chunk"`
}
