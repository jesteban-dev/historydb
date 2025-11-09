package types

import "reflect"

func SeachInSlice[T comparable](slice []T, item T) bool {
	for _, i := range slice {
		if i == item {
			return true
		}
	}
	return false
}

func NormalizeSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

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

// DiffSlices is a function that given two slices of same type, it retrieves a slice with the items that are unique in
// slice1 and another slice with the items that are unique in slice2.
func DiffSlices[T any](slice1, slice2 []T) ([]T, []T) {
	var onlyInSlice1, onlyInSlice2 []T

	for _, item1 := range slice1 {
		found := false
		for _, item2 := range slice2 {
			if reflect.DeepEqual(item1, item2) {
				found = true
				break
			}
		}
		if !found {
			onlyInSlice1 = append(onlyInSlice1, item1)
		}
	}

	for _, item2 := range slice2 {
		found := false
		for _, item1 := range slice1 {
			if reflect.DeepEqual(item2, item1) {
				found = true
				break
			}
		}
		if !found {
			onlyInSlice2 = append(onlyInSlice2, item2)
		}
	}

	return onlyInSlice1, onlyInSlice2
}
