package shardbox

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func Load(src any, filename string) Frame {
	if filename == "" && src == nil {
		return Frame{}
	}

	// Load from in-memory data
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
	case ".xml":
		return loadXML(filename)
	default:
		return Frame{}
	}
}

func fromSliceOfMaps(d []map[string]any) Frame {
	if len(d) == 0 {
		return Frame{}
	}

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

	names := records[0]
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

func loadXML(filename string) Frame {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return Frame{}
	}

	var xf xmlFrame
	if err := xml.Unmarshal(raw, &xf); err != nil || len(xf.Rows) == 0 {
		return Frame{}
	}

	// collect ordered column names from first row
	seen := map[string]bool{}
	names := []string{}
	for _, field := range xf.Rows[0].Fields {
		if !seen[field.Name] {
			names = append(names, field.Name)
			seen[field.Name] = true
		}
	}
	sort.Strings(names)

	out := NewFrame(names)

	for _, xrow := range xf.Rows {
		lookup := map[string]string{}
		for _, field := range xrow.Fields {
			lookup[field.Name] = field.Value
		}
		values := make([]any, len(names))
		for i, name := range names {
			values[i] = lookup[name]
		}
		out.AppendRow(values)
	}

	return *out
}
