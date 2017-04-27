package main

import (
	"kvstore"
	"strconv"
	"os"
	"math/rand"
)

func main() {
	key:="key"
	known_address := "127.0.0.1:3000"
	var result string
	for i:=0;i<100;i++{
		j,_ := strconv.Atoi(os.Args[1])
		kvstore.Client_remoteRead(known_address+strconv.Itoa(rand.Intn(j)),key+strconv.Itoa(i),&result)
	}
}