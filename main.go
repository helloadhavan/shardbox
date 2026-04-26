package shardbox

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Header struct {
	Names []string
}

type Column struct {
	ID     string
	Values []any
}

type Frame struct {
	Header  Header
	Columns []Column
}

func NewFrame(names []string) *Frame {
	cols := make([]Column, len(names))
	for i, n := range names {
		cols[i] = Column{
			ID:     n,
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

func (f *Frame) Rows() int {
	if len(f.Columns) == 0 {
		return 0
	}
	return len(f.Columns[0].Values)
}

func (f *Frame) Col(name string) []any {
	for i := range f.Columns {
		if f.Columns[i].ID == name {
			return f.Columns[i].Values
		}
	}
	return nil
}

func (f *Frame) Clone() *Frame {
	nf := &Frame{
		Header: Header{
			Names: append([]string(nil), f.Header.Names...),
		},
		Columns: make([]Column, len(f.Columns)),
	}

	for i, c := range f.Columns {
		nf.Columns[i] = Column{
			ID:     c.ID,
			Values: append([]any(nil), c.Values...),
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

func Load(src any, filename string) Frame {
	if filename == "" && src == nil {
		return Frame{}
	}

	// Load from in-memory shardbox1
	if src != nil {
		if d, ok := src.([]map[string]any); ok {
			return fromSliceOfMaps(d)
		}
		return Frame{}
	}

	// Load from file
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		return loadJSON(filename)
	case ".jsonl":
		return loadJSONLFrame(filename)
	case ".csv":
		return loadCSV(filename)
	default:
		return Frame{}
	}
}

// fromSliceOfMaps builds a Frame from []map[string]any (e.g. unmarshalled JSON records)
func fromSliceOfMaps(d []map[string]any) Frame {
	if len(d) == 0 {
		return Frame{}
	}

	// Collect column names from the first row, sorted for determinism
	// since map iteration order in Go is random
	names := make([]string, 0, len(d[0]))
	for k := range d[0] {
		names = append(names, k)
	}
	sort.Strings(names)

	out := NewFrame(names)
	for _, row := range d {
		values := make([]any, len(names))
		for i, name := range names {
			values[i] = row[name]
		}
		out.AppendRow(values)
	}

	return *out
}

func loadJSON(filename string) Frame {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return Frame{}
	}

	var d []map[string]any
	if err := json.Unmarshal(raw, &d); err != nil {
		return Frame{}
	}

	return fromSliceOfMaps(d)
}

func loadJSONL(filename string, handle func(map[string]any) error) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	for {
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err := handle(row); err != nil {
			return err
		}
	}

	return nil
}

func loadJSONLFrame(filename string) Frame {
	var out *Frame
	var names []string

	err := loadJSONL(filename, func(row map[string]any) error {
		if out == nil {
			// Initialize frame on first row
			for k := range row {
				names = append(names, k)
			}
			sort.Strings(names)
			out = NewFrame(names)
		}

		values := make([]any, len(names))
		for i, name := range names {
			values[i] = row[name]
		}
		out.AppendRow(values)
		return nil
	})

	if err != nil || out == nil {
		return Frame{}
	}
	return *out
}

func loadCSV(filename string) Frame {
	f, err := os.Open(filename)
	if err != nil {
		return Frame{}
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil || len(records) == 0 {
		return Frame{}
	}

	names := records[0] // first row is the header
	out := NewFrame(names)

	for _, row := range records[1:] {
		values := make([]any, len(row))
		for i, v := range row {
			values[i] = v
		}
		out.AppendRow(values)
	}

	return *out
}
