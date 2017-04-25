package main

import (
    "fmt"
//    "errors"
    "time"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"net/http"
)
type HBEAT struct{ 
	Previous_hbeat int
	New_hbeat int
}
func(new_struct *HBEAT) newHBEATchan() {
	new_struct.Previous_hbeat=-100
	new_struct.New_hbeat=0
}
type Server_Func interface {
	Heartbeat_stub(rec_time time.Time, ip *string) error
}

func(hbeat *HBEAT) Heartbeat_stub(rec_time time.Time, ip *string) error {
	fmt.Print("Received Heartbeat from ")
	hbeat.Previous_hbeat=hbeat.New_hbeat
	hbeat.New_hbeat=rec_time.Minute()
	fmt.Print(*ip)
	err:=Heartbeat(rec_time, ip)
	return err
}

func Heartbeat(rec_time time.Time, ip *string) error {
	fmt.Print(*ip)
	fmt.Print(" Active at Time: ")
	fmt.Println(rec_time)
	return nil
	//There would be a mutex lock controlled variable for each worker, which would be set to 1
	// i.e.worker on if the code reaches here
}
func registerHBEAT(server *rpc.Server, iface Server_Func) {
	// registers Arith interface by name of `Arithmetic`.
	// If you want this name to be same as the type name, you
	// can use server.Register instead.
	server.RegisterName("Server_Func",iface)
}
func startHTTPserver(done_chan chan<- string) {
	log.Println("HTTP Server started")
	iface:=new(HBEAT)
	server_http := rpc.NewServer()
	registerHBEAT(server_http,iface)
	
	// registers an HTTP handler for RPC messages on rpcPath, and a debugging handler on debugPath
	server_http.HandleHTTP("/", "/debug")

	// Listen for incoming tcp packets on specified port.
	l, e := net.Listen("tcp", ":6000")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// JSON RPC marshalling
	
	http.Serve(l,nil)
    
    done_chan <- "ServerClose"
}
func startTCPserver(done_chan chan<- string) {
	log.Println("TCP Server started")
	iface := new(HBEAT)
	iface.newHBEATchan()
	server_tcp := rpc.NewServer()
	registerHBEAT(server_tcp,iface)
	
	// Listen for incoming tcp packets on specified port.
	l1, e1 := net.Listen("tcp", ":5000")
	if e1 != nil {
		log.Fatal("listen error:", e1)
	}

	// This statement links rpc server to the socket, and allows rpc server to accept
	// rpc request coming from that socket.
	for {
        conn, err := l1.Accept()
        if err != nil {
            log.Fatal(err)
        }

        go server_tcp.ServeCodec(jsonrpc.NewServerCodec(conn))
    }
    done_chan <- "ServerClose"
}
func main() {
    done := make(chan string)
	//go startTCPserver(done)
	go startHTTPserver(done)
    fmt.Println(<-done)
    //fmt.Println(<-done)
}