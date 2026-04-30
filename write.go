package shardbox

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Write serializes a Frame to disk using a format inferred from the filename
// extension. Supported formats: JSON, JSONL, CSV and XML.
func Write(filename string, data Frame) error {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		return writeJSON(filename, data)
	case ".jsonl":
		return writeJSONL(filename, data)
	case ".csv":
		return writeCSV(filename, data)
	case ".xml":
		return writeXML(filename, data)
	default:
		return fmt.Errorf("unknown extension %q (supported .json, .jsonl, .csv, .xml)", ext)
	}
}

// writeCSV writes a Frame to a CSV file using the header names as columns.
func writeCSV(filename string, f Frame) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	if err := w.Write(f.Header.Names); err != nil {
		return err
	}

	for i := range f.Rows() {
		row := make([]string, len(f.Columns))
		for j, col := range f.Columns {
			row[j] = fmt.Sprintf("%v", col.Values[i])
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return w.Error()
}

// writeJSON writes a Frame as a pretty-printed JSON array of objects.
func writeJSON(filename string, f Frame) error {
	rows := frameToSliceOfMaps(f)

	raw, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, raw, 0644)
}

// writeJSONL writes a Frame as newline-delimited JSON objects.
func writeJSONL(filename string, f Frame) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)

	for _, row := range frameToSliceOfMaps(f) {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}

	return nil
}

// xmlRow represents a single row in XML output.
type xmlRow struct {
	Fields []xmlField `xml:"field"`
}

// xmlField represents a single named value in an XML row.
type xmlField struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

// xmlFrame is the root XML structure used for Frame serialization.
type xmlFrame struct {
	XMLName xml.Name `xml:"frame"`
	Rows    []xmlRow `xml:"row"`
}

// writeXML writes a Frame to an XML file using the shardbox XML schema.
func writeXML(filename string, f Frame) error {
	xf := xmlFrame{}

	for i := range f.Rows() {
		row := xmlRow{}
		for j, name := range f.Header.Names {
			row.Fields = append(row.Fields, xmlField{
				Name:  name,
				Value: fmt.Sprintf("%v", f.Columns[j].Values[i]),
			})
		}
		xf.Rows = append(xf.Rows, row)
	}

	raw, err := xml.MarshalIndent(xf, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, append([]byte(xml.Header), raw...), 0644)
}

// frameToSliceOfMaps converts a Frame into a slice of row maps keyed by column name.
func frameToSliceOfMaps(f Frame) []map[string]any {
	rows := make([]map[string]any, f.Rows())
	for i := range f.Rows() {
		row := make(map[string]any, len(f.Columns))
		for j, name := range f.Header.Names {
			row[name] = f.Columns[j].Values[i]
		}
		rows[i] = row
	}
	return rows
}
