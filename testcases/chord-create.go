package main

import (
	"kvstore"
	"fmt"
	"time"
)

func main() {
	var l kvstore.LocalNode
	l.Address = "127.0.0.1:30000"
	l.Port = ":30000"
	l.Init(kvstore.DefaultConfig())
	fmt.Println("Created Local Node")
	l.Create()
	fmt.Println("Created Ring")
	time.Sleep(1200 * time.Second)
}
