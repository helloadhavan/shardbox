package tests

import (
	"awesomeProject1/shardbox"
	"fmt"
	"time"
)

func main() {
	// load a 92 MB file
	var then time.Time = time.Now()
	frame := shardbox.Load(nil, "/home/adhavan/PycharmProjects/just for fun/issue_fix_000.jsonl")
	frame.Truncate(8)
	var now time.Time = time.Now()
	fmt.Println(now.Sub(then))
}
