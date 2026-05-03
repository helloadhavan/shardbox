package shardbox

import "fmt"

// Header stores the column names for a Frame.
type Header struct {
	Names []string
}

// Frame represents a simple table-like data structure composed of
// named columns with equal-length Arrays.
type Frame struct {
	Header  Header
	Columns []Array
}

// NewFrame creates a new Frame with the given column names and
// initializes empty columns.
func NewFrame(names []string) *Frame {
	cols := make([]Array, len(names))
	for i := range names {
		cols[i] = Array{Values: []any{}}
	}

	return &Frame{
		Header:  Header{Names: append([]string(nil), names...)},
		Columns: cols,
	}
}

// AppendRow adds a new row of values to the Frame.
// Panics if the number of values does not match the number of columns.
func (f *Frame) AppendRow(values []any) {
	if len(values) != len(f.Columns) {
		panic("row length does not match number of columns")
	}

	for i, v := range values {
		f.Columns[i].Values = append(f.Columns[i].Values, v)
	}
}

// GetRow returns the values of row i as a slice.
func (f *Frame) GetRow(i int) []any {
	out := []any{}
	for _, col := range f.Columns {
		out = append(out, col.Get(i))
	}
	return out
}

// Rows returns the number of rows in the Frame.
func (f *Frame) Rows() int {
	if len(f.Columns) == 0 {
		return 0
	}
	return len(f.Columns[0].Values)
}

// Col returns the Array for the column with the given name.
// If the column does not exist, nil is returned.
func (f *Frame) Col(name string) *Array {
	for i, n := range f.Header.Names {
		if n == name {
			return &f.Columns[i]
		}
	}
	return nil
}

// Clone creates a deep copy of the Frame, including column values.
func (f *Frame) Clone() *Frame {
	nf := &Frame{
		Header: Header{
			Names: append([]string(nil), f.Header.Names...),
		},
		Columns: make([]Array, len(f.Columns)),
	}

	for i, col := range f.Columns {
		nf.Columns[i] = Array{
			Dtype:  col.Dtype,
			Values: append([]any(nil), col.Values...),
		}
	}

	return nf
}

// Truncate reduces the Frame to at most n rows.
// If n is negative or larger than the current row count, no truncation occurs.
func (f *Frame) Truncate(n int) {
	if n < 0 {
		n = 0
	}
	if n >= f.Rows() {
		return
	}

	for i := range f.Columns {
		f.Columns[i].Values = f.Columns[i].Values[:n]
	}
}

// PrintFrame prints the Frame in a tabular, human-readable format.
func (f *Frame) PrintFrame() {
	for _, name := range f.Header.Names {
		if len(name) < 12 {
			fmt.Printf("%-12s", name)
		} else {
			fmt.Printf("%-12s", name[:9]+"...")
		}
	}
	fmt.Println()

	for range f.Header.Names {
		fmt.Print("------------")
	}
	fmt.Println()

	for i := range f.Rows() {
		for _, col := range f.Columns {
			fmt.Printf("%-12v", col.Values[i])
		}
		fmt.Println()
	}

	fmt.Printf("(%d rows)\n", f.Rows())
}

// ColPtr is an alias of Col and returns a pointer to the named column Array.
func (f *Frame) ColPtr(name string) *Array {
	for i, n := range f.Header.Names {
		if n == name {
			return &f.Columns[i]
		}
	}
	return nil
}

func (f *Frame) appendCol(header_name string, col Array) {
	f.Columns = append(f.Columns, col)
	f.Header.Names = append(f.Header.Names, header_name)
}
