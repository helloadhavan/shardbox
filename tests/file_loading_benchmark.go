package tests

import (
	"fmt"
	"shardbox/src"
	"time"
)

func main() {
	// load a 92 MB file
	var then time.Time = time.Now()
	frame := src.Load(nil, "/home/adhavan/PycharmProjects/just for fun/issue_fix_000.jsonl")
	frame.Truncate(8)
	var now time.Time = time.Now()
	fmt.Println(now.Sub(then)) // 650-720 ms on a 4 core intel cpu with 32 GB of ram
}
