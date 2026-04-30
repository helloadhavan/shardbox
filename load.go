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

// Load creates a Frame from either in-memory data or a file.
// If src is non-nil, it takes precedence over filename.
// Supported file formats: JSON, JSONL, CSV, XML.
func Load(src any, filename string) Frame {
	if filename == "" && src == nil {
		return Frame{}
	}

	if src != nil {
		if d, ok := src.([]map[string]any); ok {
			return fromSliceOfMaps(d)
		}
		return Frame{}
	}

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

// fromSliceOfMaps converts a slice of maps into a Frame.
// Column names are inferred from map keys and sorted.
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

// loadJSON loads a JSON file containing an array of objects into a Frame.
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

// loadJSONL streams a JSON Lines file and calls handle for each decoded row.
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

// loadJSONLFrame loads a JSON Lines file into a Frame.
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

// loadCSV loads a CSV file into a Frame.
// The first row is treated as the header.
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

// loadXML loads an XML file in the shardbox frame format into a Frame.
func loadXML(filename string) Frame {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return Frame{}
	}

	var xf xmlFrame
	if err := xml.Unmarshal(raw, &xf); err != nil || len(xf.Rows) == 0 {
		return Frame{}
	}

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
