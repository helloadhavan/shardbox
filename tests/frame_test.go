package tests

import (
	"encoding/json"
	"os"
	"shardbox/src"
	"testing"
)

// helpers

func makeTestFrame() *src.Frame {
	f := src.NewFrame([]string{"name", "age", "score"})
	f.AppendRow([]any{"alice", 30, 90.5})
	f.AppendRow([]any{"bob", 25, 85.0})
	f.AppendRow([]any{"carol", 35, 92.0})
	f.AppendRow([]any{"alex", 25, 82.0})
	f.AppendRow([]any{"steve", 35, 92.0})
	f.AppendRow([]any{"bob", 35, 82.0})
	return f
}

func writeTempFile(t *testing.T, ext, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "shardbox_*"+ext)
	if err != nil {
		t.Fatalf("could not create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("could not write temp file: %v", err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

// --- Array tests ---

func TestArrayFilter(t *testing.T) {
	a := src.Array{Values: []any{1, 2, 3, 4, 5}}
	a.Filter(func(v any) bool { return v.(int) > 2 })
	if len(a.Values) != 3 || a.Values[0].(int) != 3 {
		t.Errorf("unexpected filter result: %v", a.Values)
	}
}

func TestArrayMap(t *testing.T) {
	a := src.Array{Values: []any{1, 2, 3}}
	a.Map(func(v any) any { return v.(int) * 2 })
	if a.Values[2].(int) != 6 {
		t.Errorf("unexpected map result: %v", a.Values)
	}
}

func TestArrayDelete(t *testing.T) {
	a := src.Array{Values: []any{"a", "b", "c"}}
	a.Delete(1)
	if len(a.Values) != 2 || a.Values[1] != "c" {
		t.Errorf("unexpected delete result: %v", a.Values)
	}
}

func TestArrayJoin(t *testing.T) {
	a := src.Array{Values: []any{1, 2}}
	b := src.Array{Values: []any{3, 4}}
	a.Join(b)
	if len(a.Values) != 4 || a.Values[3] != 4 {
		t.Errorf("unexpected join result: %v", a.Values)
	}
}

func TestArrayMeanWrongDtype(t *testing.T) {
	a := src.Array{Values: []any{"a", "b"}, Dtype: "string"}
	if _, err := a.Mean(); err == nil {
		t.Error("expected error for non-numeric dtype")
	}
}

// --- Frame tests ---

func TestFrameAppendAndRows(t *testing.T) {
	f := makeTestFrame()
	if f.Rows() != 3 {
		t.Errorf("expected 3 rows, got %d", f.Rows())
	}
}

func TestFrameAppendRowPanicsOnMismatch(t *testing.T) {
	f := src.NewFrame([]string{"a", "b"})
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on row length mismatch")
		}
	}()
	f.AppendRow([]any{1})
}

func TestFrameCol(t *testing.T) {
	f := makeTestFrame()
	col := f.Col("age")
	if col == nil || col.Values[0] != 30 {
		t.Errorf("unexpected col result: %v", col)
	}
}

func TestFrameColPtr(t *testing.T) {
	f := makeTestFrame()
	ptr := f.ColPtr("name")
	if ptr == nil {
		t.Fatal("expected non-nil ColPtr")
	}
	ptr.Values[0] = "ALICE"
	if f.Col("name").Values[0] != "ALICE" {
		t.Error("ColPtr mutation did not affect frame")
	}
}

func TestFrameClone(t *testing.T) {
	f := makeTestFrame()
	c := f.Clone()
	c.Columns[0].Values[0] = "mutated"
	if f.Col("name").Values[0] == "mutated" {
		t.Error("clone shares memory with original")
	}
}

func TestFrameTruncate(t *testing.T) {
	f := makeTestFrame()
	f.Truncate(1)
	if f.Rows() != 1 {
		t.Errorf("expected 1 row after truncate, got %d", f.Rows())
	}
}

// --- Load tests ---

func TestLoadCSV(t *testing.T) {
	path := writeTempFile(t, ".csv", "name,age\nalice,30\nbob,25\n")
	frame := src.Load(nil, path)
	if frame.Rows() != 2 || frame.Col("name").Values[0] != "alice" {
		t.Errorf("unexpected CSV load result")
	}
}

func TestLoadJSON(t *testing.T) {
	data := []map[string]any{{"name": "alice", "age": float64(30)}}
	raw, _ := json.Marshal(data)
	frame := src.Load(nil, writeTempFile(t, ".json", string(raw)))
	if frame.Rows() != 1 {
		t.Errorf("expected 1 row, got %d", frame.Rows())
	}
}

func TestLoadJSONL(t *testing.T) {
	content := `{"name":"alice","age":30}` + "\n" + `{"name":"bob","age":25}` + "\n"
	frame := src.Load(nil, writeTempFile(t, ".jsonl", content))
	if frame.Rows() != 2 {
		t.Errorf("expected 2 rows, got %d", frame.Rows())
	}
}

func TestLoadFromSliceOfMaps(t *testing.T) {
	data := []map[string]any{{"x": 1, "y": 2}, {"x": 3, "y": 4}}
	frame := src.Load(data, "")
	if frame.Rows() != 2 {
		t.Errorf("expected 2 rows, got %d", frame.Rows())
	}
}
