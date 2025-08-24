package entities

type JSONSchemaType struct {
	SchemaType SchemaType `json:"schemaType"`
	PrevRef    *string    `json:"prevRef"`
}
