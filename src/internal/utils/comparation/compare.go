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

func AssignIfChangedWithEquality[T any](dst **T, new, old *T, equalFunc func(a, b *T) bool) {
	if new == nil && old == nil {
		return
	} else if new == nil && old != nil {
		*dst = nil
	} else if new != nil && old == nil {
		*dst = new
	} else if !equalFunc(new, old) {
		*dst = new
	}
}

func AssignIfNotNil[T any](dst, org *T) {
	if org != nil {
		*dst = *org
	}
}
