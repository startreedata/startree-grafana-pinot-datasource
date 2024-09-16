package pinotlib

type Set[T comparable] map[T]struct{}

func NewSet[T comparable](initSize int) Set[T] {
	return make(Set[T], initSize)
}

func (x Set[T]) Add(value T) {
	x[value] = struct{}{}
}

func (x Set[T]) Del(value T) {
	delete(x, value)
}

func (x Set[T]) Contains(value T) bool {
	_, ok := (x)[value]
	return ok
}

func (x Set[T]) Empty() bool {
	return len(x) == 0
}

func (x Set[T]) Len() int {
	return len(x)
}

func (x Set[T]) Values() []T {
	values := make([]T, 0, len(x))
	for k := range x {
		values = append(values, k)
	}
	return values
}

func (x Set[T]) Copy() Set[T] {
	s := make(Set[T], len(x))
	for _, v := range x.Values() {
		s.Add(v)
	}
	return s
}

func (x Set[T]) Union(y Set[T]) Set[T] {
	union := make(Set[T], len(x))
	for _, v := range x.Values() {
		union.Add(v)
	}
	for _, v := range y.Values() {
		union.Add(v)
	}
	return union
}

func (x Set[T]) Intersection(y Set[T]) Set[T] {
	intersection := make(Set[T])
	for _, v := range x.Values() {
		if y.Contains(v) {
			intersection.Add(v)
		}
	}
	return intersection
}
