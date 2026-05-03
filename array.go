package shardbox

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Array represents a one-dimensional collection of values with an optional
// inferred or user-defined data type.
type Array struct {
	Values []any
	Dtype  string
}

// Filter removes all elements for which the predicate function f returns false.
// The Array is modified in place.
func (a *Array) Filter(f func(any) bool) {
	var newValues []any
	for i := range a.Values {
		if f(a.Values[i]) {
			newValues = append(newValues, a.Values[i])
		}
	}
	a.Values = newValues
}

// Map applies the function f to every element in the Array and replaces
// the contents with the returned values.
func (a *Array) Map(f func(any) any) {
	var newValues []any
	for i := range a.Values {
		newValues = append(newValues, f(a.Values[i]))
	}
	a.Values = newValues
}

// Get returns the value at index i.
// If the index is out of bounds, an error is returned instead.
func (a *Array) Get(i int) any {
	if len(a.Values)-1 < i {
		return fmt.Errorf("index %d out of bounds (len=%d)", i, len(a.Values))
	}
	return a.Values[i]
}

// Insert inserts a value at the specified index, shifting elements to the right.
func (a *Array) Insert(index int, val any) error {
	if index < 0 || index > len(a.Values) {
		return fmt.Errorf("index %d out of bounds (len=%d)", index, len(a.Values))
	}

	a.Values = append(a.Values, nil)
	copy(a.Values[index+1:], a.Values[index:])
	a.Values[index] = val
	return nil
}

// Len returns the number of elements in the Array.
func (a *Array) Len() int {
	return len(a.Values)
}

// Swap exchanges the values at indices i and j.
// This allows Array to be used with sort utilities.
func (a *Array) Swap(i, j int) {
	a.Values[i], a.Values[j] = a.Values[j], a.Values[i]
}

// Mean computes the arithmetic mean of the Array.
// Supported numeric types are int, int64, and float64.
// An error is returned for non-numeric values or empty Arrays.
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

// Slice returns a subslice of the Array values from start (inclusive)
// to end (exclusive).
func (a *Array) Slice(start, end int) []any {
	return a.Values[start:end]
}

// Delete removes the element at index i from the Array.
func (a *Array) Delete(i int) {
	a.Values = append(a.Values[:i], a.Values[i+1:]...)
}

// Join appends all values from another Array to this Array.
func (a *Array) Join(other Array) {
	a.Values = append(a.Values, other.Values...)
}

// Median computes the median value of the Array using the provided
// sortFunc to derive sortable numeric keys.
// The Array is sorted in place.
func (a *Array) Median(sortFunc func(interface{}) int) float64 {
	n := a.Len()
	if n == 0 {
		return 0.0
	}

	sort.Slice(a.Values, func(i, j int) bool {
		return sortFunc(a.Values[i]) < sortFunc(a.Values[j])
	})

	if n%2 == 1 {
		return float64(sortFunc(a.Values[n/2]))
	}

	m1 := sortFunc(a.Values[n/2-1])
	m2 := sortFunc(a.Values[n/2])
	return float64(m1+m2) / 2.0
}

// Mode returns the most frequently occurring value in the Array.
// If the Array is empty, nil is returned.
func (a *Array) Mode() any {
	if len(a.Values) == 0 {
		return nil
	}

	score := map[any]int{}
	var mode any
	maxFreq := 0

	for _, val := range a.Values {
		score[val]++
		if score[val] > maxFreq {
			maxFreq = score[val]
			mode = val
		}
	}
	return mode
}

// Max returns the highest value returned by statFunc
// statFunc is a function to determine the value of any type
// If you have not created a custom stat function then use CommonStatFunc instead
func (a *Array) Max(statFunc func(any) float64) any {
	var m map[float64]any
	var maX float64 = 0
	for _, v := range a.Values {
		m[statFunc(v)] = v
	}
	for k, _ := range m {
		if k > maX {
			maX = k
		}
	}
	return m[maX]
}

// Min returns the lowest value returned by statFunc
// statFunc is a function to determine the value of any type
// If you have not created a custom stat function then use CommonStatFunc insted
func (a *Array) Min(statFunc func(any) float64) any {
	var m map[float64]any
	var miN float64 = 0
	for _, v := range a.Values {
		m[statFunc(v)] = v
	}
	for k, _ := range m {
		if k > miN {
			miN = k
		}
	}
	return m[miN]
}

func CommonStatFunc(inp any) float64 {
	switch v := inp.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		return float64(len(v))
	case bool:
		return 0.0
	case nil:
		return 0.0
	default:
		return 0.0
	}
}
