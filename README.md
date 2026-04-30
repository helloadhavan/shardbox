# shardbox

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/helloadhavan/shardbox/refs/heads/main/logo_dark.png">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/helloadhavan/shardbox/refs/heads/main/logo_light.png">
  <img alt="shardbox logo" src="https://raw.githubusercontent.com/helloadhavan/shardbox/refs/heads/main/logo_light.png">
</picture>

---

## Overview

**shardbox** is a DataFrame lib written in Go. It provides basic DataFrame fuctions such as column and row operations, cloning, truncating, and file IO (CSV, JSON, and JSONL)—and is designed to be a foundational framework for data manipulation. The goal is to eventually allow compiling this system to WebAssembly (wasm), enabling browser and edge device interoperability.

---

## Project Idea & Vision

Many data scientists and engineers use DataFrames in Python (pandas) or R. However, Go lacks a DataFrame library that is easy for beginners and easy to compile to wasm for web apps.

This project started as a learning exercise to better understand Go's type system and file handling. The planned future is:
- Build out core DataFrame features (row/column add/drop, printing, importing).
- Increase file format support and safety.
- Refactor as a Go package for easy integration.
- Enable wasm compilation so that `shardbox` can run in browsers or serverless environments.

---

## Features

- Create DataFrames from column names
- Append rows and access by column name
- Clone and truncate DataFrames
- Print DataFrames in a simple table view
- Load DataFrames from:
    - **CSV** files
    - **JSON** files (array of objects)
    - **JSONL** (newline-delimited JSON)
    - **XML** (Data in tags)
    - Go in-memory `[]map[string]any` data structures

---

## Quickstart Example

```go
package main

import "github.com/helloadhavan/shardbox"

func main() {
    // Create a frame with columns
    f := shardbox.NewFrame([]string{"name", "age", "country"})
    f.AppendRow([]any{"Alice", 30, "India"})
    f.AppendRow([]any{"Bob", 22, "USA"})

    f.PrintFrame()
    //    name        age         country
    //    ------------ ------------ ------------
    //    Alice       30          India
    //    Bob         22          USA
    //    (2 rows)

    // Load from CSV
    frame := shardbox.Load(nil, "data.csv")
    frame.PrintFrame()
}
```

---

## Planned Roadmap

- [x] Basic data structure & CLI output
- [x] CSV / JSON / JSONL file loading
- [x] Basic DataFrame operations (filter, map, select)
- [ ] Type safety improvements
- [ ] File save improvements
- [ ] Performance benchmarking
- [ ] Go package + wasm build

---

## Contributing

Pull requests, suggestions, and issues are welcome! See [`frame.go`](main.go) for implementation details.

Beginner contributions are especially encouraged! If you're new to Go, feel free to read the code, suggest improvements, or add documentation.

---

## License

[MIT](LICENSE)

---

## Acknowledgements

Started as a Go learning project—there are many improvements possible. Inspired by pandas and DuckDB.
