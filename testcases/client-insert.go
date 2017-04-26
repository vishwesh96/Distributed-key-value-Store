package main

import (
	"kvstore"
	"strconv"
	"os"
)

func main() {
	key:="key"
	val:="value"
	known_address:="127.0.0.1:300"+os.Args[1]
	for i:=0;i<100;i++{
		kvstore.Client_remoteWrite(known_address,key+strconv.Itoa(i),val+strconv.Itoa(i))
	}
}