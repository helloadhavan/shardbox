package tests

import (
	"awesomeProject1/shardbox"
	"testing"
)

func Test0(t *testing.T) {
	f := shardbox.NewFrame([]string{"id", "name", "age"})

	if len(f.Header.Names) != 3 {
		t.Fatalf("expected 3 header names, got %d", len(f.Header.Names))
	}

	if len(f.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(f.Columns))
	}

	for i, col := range f.Columns {
		if col.ID != f.Header.Names[i] {
			t.Fatalf("column id %q does not match header %q", col.ID, f.Header.Names[i])
		}
	}
}

func Test1(t *testing.T) {
	f := shardbox.NewFrame([]string{"id", "name"})

	f.AppendRow([]any{1, "alice"})
	f.AppendRow([]any{2, "bob"})

	if rows := f.Rows(); rows != 2 {
		t.Fatalf("expected 2 rows, got %d", rows)
	}
}

func Test2(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on row length mismatch")
		}
	}()

	f := shardbox.NewFrame([]string{"id", "name"})
	f.AppendRow([]any{1}) // wrong length
}

func Test3(t *testing.T) {
	f := shardbox.NewFrame([]string{"id", "name"})

	f.AppendRow([]any{1, "alice"})
	f.AppendRow([]any{2, "bob"})

	names := f.Col("name")
	if names == nil {
		t.Fatal("expected column 'name', got nil")
	}

	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected column values: %v", names)
	}
}

func Test4(t *testing.T) {
	f := shardbox.NewFrame([]string{"x"})
	f.AppendRow([]any{10})
	f.AppendRow([]any{20})

	clone := f.Clone()

	// Modify clone
	clone.Columns[0].Values[0] = 999

	// Original should not change
	if f.Columns[0].Values[0] == 999 {
		t.Fatal("clone modification affected original frame")
	}
}

func Test5(t *testing.T) {
	f := shardbox.NewFrame([]string{"a", "b"})

	f.AppendRow([]any{1, 2})
	f.AppendRow([]any{3, 4})
	f.AppendRow([]any{5, 6})

	f.Truncate(2)

	if rows := f.Rows(); rows != 2 {
		t.Fatalf("expected 2 rows after truncate, got %d", rows)
	}

	a := f.Col("a")
	if a[1] != 3 {
		t.Fatalf("unexpected value after truncate: %v", a)
	}
}

func Test6(t *testing.T) {
	f := shardbox.NewFrame([]string{"x"})
	f.AppendRow([]any{1})

	f.Truncate(10) // should be no-op

	if rows := f.Rows(); rows != 1 {
		t.Fatalf("expected 1 row, got %d", rows)
	}
}
