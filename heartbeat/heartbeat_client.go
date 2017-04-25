package main

import (
	"fmt"
	"log"
	"time"
	"net/rpc"
)


type RPC_client struct {
	client *rpc.Client
}

func (t *RPC_client) update_mssg(time_chan <-chan time.Time) error {
	fmt.Println("Timer Started")
	var ip string
	for tim := range time_chan {
		ip="10.202.5.11"
        fmt.Println("Tick at", tim, " From ", ip)
		Async_Call := t.client.Go("Server_Func.Heartbeat_stub", tim,&ip,nil)
		err := Async_Call.Error
		if err != nil {
    		log.Println("Async Call error:", err) 
    		return nil
		} 
    }
	return nil
}

func main() {

	fmt.Println("Give Server IP:Port_num")
    var server_addr string
    fmt.Scanln(&server_addr)
    t, err := rpc.DialHTTP("tcp", server_addr)
    if err != nil {
        log.Fatal("dialing:", err)
    }
    client := RPC_client{t}
    ticker := time.NewTicker(1* time.Second)
    client.update_mssg(ticker.C)
   
}
