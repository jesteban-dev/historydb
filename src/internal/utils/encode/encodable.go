package encode

type Encodable interface {
	EncodeToBytes() []byte
}
