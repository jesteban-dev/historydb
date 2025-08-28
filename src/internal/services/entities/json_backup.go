package entities

type JSONRefData struct {
	PrevRef *string `json:"prevRef"`
}

type JSONSchemaData struct {
	JSONRefData
	SchemaType SchemaType `json:"schemaType"`
}
