package shardbox

import (
	"fmt"
)

type Header struct {
	Names []string
}

type Frame struct {
	Header  Header
	Columns []Array
}

func NewFrame(names []string) *Frame {
	cols := make([]Array, len(names))
	for i := range names {
		cols[i] = Array{
			Values: []any{},
		}
	}

	return &Frame{
		Header:  Header{Names: append([]string(nil), names...)},
		Columns: cols,
	}
}

func (f *Frame) AppendRow(values []any) {
	if len(values) != len(f.Columns) {
		panic("row length does not match number of columns")
	}

	for i, v := range values {
		f.Columns[i].Values = append(f.Columns[i].Values, v)
	}
}

func (f *Frame) GetRow(i int) []any {
	out := []any{}
	for _, j := range f.Columns {
		out = append(out, j.Get(i))
	}
	return out
}

func (f *Frame) Rows() int {
	if len(f.Columns) == 0 {
		return 0
	}
	return len(f.Columns[0].Values)
}

func (f *Frame) Col(name string) *Array {
	for i, n := range f.Header.Names {
		if n == name {
			return &f.Columns[i]
		}
	}
	return nil
}

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

func (f *Frame) ColPtr(name string) *Array {
	for i, n := range f.Header.Names {
		if n == name {
			return &f.Columns[i]
		}
	}
	return nil
}
