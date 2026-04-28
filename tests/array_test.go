package tests

import (
	"shardbox"
	"testing"
)

func TestArrayGet(t *testing.T) {
	arr := &shardbox.Array{Values: []any{10, 20, 30}, Dtype: "int"}

	if arr.Get(0) != 10 {
		t.Errorf("expected 10, got %v", arr.Get(0))
	}
	if arr.Get(2) != 30 {
		t.Errorf("expected 30, got %v", arr.Get(2))
	}
}

func TestArrayLen(t *testing.T) {
	arr := &shardbox.Array{Values: []any{1, 2, 3}, Dtype: "int"}

	if arr.Len() != 3 {
		t.Errorf("expected length 3, got %d", arr.Len())
	}
}

func TestArrayInsert(t *testing.T) {
	arr := &shardbox.Array{Values: []any{1, 3, 4}, Dtype: "int"}
	arr.Insert(1, 2)

	if arr.Get(1) != 2 {
		t.Errorf("expected 2 at index 1, got %v", arr.Get(1))
	}
	if arr.Len() != 4 {
		t.Errorf("expected length 4, got %d", arr.Len())
	}
}

func TestArraySwap(t *testing.T) {
	arr := &shardbox.Array{Values: []any{1, 2, 3}, Dtype: "int"}
	arr.Swap(0, 2)

	if arr.Get(0) != 3 || arr.Get(2) != 1 {
		t.Errorf("expected [3, 2, 1], got %v", arr.Values)
	}
}

func TestArrayMean(t *testing.T) {
	arr := &shardbox.Array{Values: []any{2, 4, 6}, Dtype: "int"}
	mean, err := arr.Mean()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mean != 4.0 {
		t.Errorf("expected mean 4.0, got %v", mean)
	}
}

func TestArrayMeanInvalidDtype(t *testing.T) {
	arr := &shardbox.Array{Values: []any{"a", "b"}, Dtype: "string"}
	_, err := arr.Mean()

	if err == nil {
		t.Error("expected error for invalid dtype, got nil")
	}
}
