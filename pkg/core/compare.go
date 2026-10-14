package core

import "fmt"

func CompareKeys(a, b any) int {
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok {
			return compareOrdered(x, y)
		}
	case int:
		if y, ok := b.(int); ok {
			return compareOrdered(x, y)
		}
	case int64:
		if y, ok := b.(int64); ok {
			return compareOrdered(x, y)
		}
	case float64:
		if y, ok := b.(float64); ok {
			return compareOrdered(x, y)
		}
	case bool:
		if y, ok := b.(bool); ok {
			switch {
			case x == y:
				return 0
			case !x:
				return -1
			default:
				return 1
			}
		}
	}

	return compareOrdered(fmt.Sprintf("%T|%V", a, a), fmt.Sprintf("%T|%v", b, b))
}

type ordered interface {
	~int | ~int64 | ~float64 | ~string
}

func compareOrdered[T ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
