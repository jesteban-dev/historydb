package helpers

// ToInterfaceSlice converts an interface to a interface slice
func ToInterfaceSlice(v interface{}) []interface{} {
	if v == nil {
		return []interface{}{}
	}

	switch val := v.(type) {
	case []interface{}:
		return val
	default:
		return []interface{}{val}
	}
}

// Pointer converts a single variable into a pointer
func Pointer[T any](v T) *T {
	return &v
}
