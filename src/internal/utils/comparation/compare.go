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

func AssignIfNotNil[T comparable](dst, org *T) {
	if org != nil {
		*dst = *org
	}
}
