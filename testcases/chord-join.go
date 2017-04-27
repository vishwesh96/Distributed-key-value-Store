package main

import (
	"kvstore"
	// "crypto/sha1"
	"fmt"
	// "hash"
	"os"
	// "log"
	"time"
)

func main() {
	var k kvstore.LocalNode
	time.Sleep(3 * time.Second)
	k.Address = "127.0.0.1:3000"+os.Args[1]
	k.Port = ":3000"+os.Args[1]
	k.Init(kvstore.DefaultConfig())
	fmt.Println("Created Local Node")
	k.Join("127.0.0.1:30000")
	time.Sleep(1000 * time.Second)
}