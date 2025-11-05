package types

import "fmt"

func ToInterfaceMap[T any](m map[string]T) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func FromInterfaceMap[T any](m map[string]interface{}) (map[string]T, error) {
	out := make(map[string]T, len(m))
	for k, v := range m {
		t, ok := v.(T)
		if !ok {
			return nil, fmt.Errorf("value for key %q is not type %T", k, t)
		}
		out[k] = t
	}
	return out, nil
}
