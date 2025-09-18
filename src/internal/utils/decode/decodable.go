package decode

type Decodable[T any] interface {
	DecodeFromBytes(data []byte) (T, error)
}
