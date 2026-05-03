// Package shardbox contains simple data structures and helpers for working
// with tabular data.
//
// The main type in this package is Frame, which represents a table made up
// of named columns. Each column is stored as an Array, which holds the values
// for that column.
//
// Frames can be loaded from files or in-memory data using Load, and written
// back to disk using Write. Supported file formats include CSV, JSON,
// JSON Lines (JSONL), and XML.
//
// This package is intended as a learning project and
// straightforward behavior rather than performance or completness.
// Example:
// ```go
//
// ```
package shardbox
