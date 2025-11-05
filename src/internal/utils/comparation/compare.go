package comparation

func AssignIfChanged[T comparable](dst **T, new, old *T) {
	if new == nil && old == nil {
		return
	} else if new == nil && old != nil {
		*dst = nil
	} else if new != nil && old == nil {
		*dst = new
	} else if *new != *old {
		*dst = new
	}
}

func AssignIfChangedSlice[T comparable](dst []T, new, old []T) {
	if new == nil && old == nil {
		return
	} else if new == nil && old != nil {
		dst = nil
	} else if new != nil && old == nil {
		dst = make([]T, len(new))
		copy(dst, new)
	} else if len(new) == len(old) {
		for i, v := range old {
			if new[i] != v {
				dst = make([]T, len(new))
				copy(dst, new)
				return
			}
		}
	}
}

func AssignIfNotNil[T comparable](dst, org *T) {
	if org != nil {
		*dst = *org
	}
}

func AssignSliceIfNotNil[T comparable](dst, org []T) {
	if org != nil {
		dst = make([]T, len(org))
		copy(dst, org)
	}
}
