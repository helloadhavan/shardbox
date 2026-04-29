package shardbox

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type Array struct {
	Values []any
	Dtype  string
}

func (a *Array) Filter(f func(any) bool) {
	var newValues []any
	for i := range a.Values {
		if f(a.Values[i]) {
			newValues = append(newValues, a.Values[i])
		}
	}
	a.Values = newValues
}

func (a *Array) Map(f func(any) any) {
	var newValues []any
	for i := range a.Values {
		newValues = append(newValues, f(a.Values[i]))
	}
	a.Values = newValues
}

func (a *Array) Get(i int) any {
	if len(a.Values)-1 <= i {
		return a.Values[i]
	} else {
		return fmt.Errorf("index %d out of bounds (len=%d)", i, len(a.Values))
	}
}

func (a *Array) Insert(index int, val any) {
	a.Values = append(a.Values, nil)
	copy(a.Values[index+1:], a.Values[index:])
	a.Values[index] = val
}

func (a *Array) Len() int {
	return len(a.Values)
}

func (a *Array) Swap(i, j int) {
	a.Values[i], a.Values[j] = a.Values[j], a.Values[i]
}

func (a *Array) Mean() (float64, error) {
	if a.Dtype == "" {
		seen := map[string]bool{}
		for _, val := range a.Values {
			seen[reflect.TypeOf(val).String()] = true
		}
		types := make([]string, 0, len(seen))
		for t := range seen {
			types = append(types, t)
		}
		sort.Strings(types)
		a.Dtype = strings.Join(types, "|")
	}

	if a.Dtype != "int" && a.Dtype != "int64" && a.Dtype != "float64" {
		return 0, fmt.Errorf("Dtype %q is not suitable for mean", a.Dtype)
	}

	var sum float64
	var count int

	for _, v := range a.Values {
		switch n := v.(type) {
		case int:
			sum += float64(n)
		case int64:
			sum += float64(n)
		case float64:
			sum += n
		default:
			return 0, fmt.Errorf("non-numeric value: %v", v)
		}
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("cannot compute mean of empty Array")
	}

	return sum / float64(count), nil
}

func (a *Array) Slice(start, end int) []any {
	return a.Values[start:end]
}

func (a *Array) Delete(i int) {
	a.Values = append(a.Values[:i], a.Values[i+1:]...)
}

func (a *Array) Join(other Array) {
	a.Values = append(a.Values, other.Values...)
}

func Copy(a Array) *Array {
	return &a
}
