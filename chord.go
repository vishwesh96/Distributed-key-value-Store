package kvstore

import (
	"crypto/sha1"
	// "fmt"
	"hash"
	"time"
)

type Config struct {
	HashFunc      func() hash.Hash // Hash function to use
	StabilizeMin  time.Duration    // Minimum stabilization time
	StabilizeMax  time.Duration    // Maximum stabilization time
	NumSuccessors int              // Number of successors to maintain
	NumReplicas	  int 			   // Number of replicas
	// Delegate      Delegate         // Invoked to handle ring events
	hashBits      int              // Bit size of the hash function
}
type Node_RPC interface{
	FindSuccessor_stub(key string, reply *string) error
	GetPredecessor_stub(emp_arg struct{}, reply *string) error
	Notify_stub(message string, emp_reply *struct{}) error
	Ping_stub(emp_arg struc{},emp_reply *struct{}) error
}


// Represents a node, local or remote
type Node struct {
	Id   []byte // Unique Chord Id
	Address string // Host identifier
}

type LocalNode struct {
	Node
	successors  []*Node
	finger      []*Node
	// last_finger int
	predecessor *Node
	config Config
	// stabilized  time.Time
	// timer       *time.Timer
}

func DefaultConfig() Config {
	return Config{
		sha1.New, // SHA1
		time.Duration(1 * time.Second),
		time.Duration(3 * time.Second),
		3,   // 3 successors
		3,   // 3 Replicas
		// nil, // No delegate
		16, // 16bit hash function
	}
}
func registerServer(server *rpc.Server, iface Server_Func) {
	// registers Arith interface by name of `Arithmetic`.
	// If you want this name to be same as the type name, you
	// can use server.Register instead.
	server.RegisterName("Node_RPC",iface)
}
func (ln *LocalNode) Init(config Config) {
	// Generate an Id
	ln.config = config
	ln.Id = GenHash(ln.config, ln.Address)
	// Initialize all state
	ln.successors = make([]*Node, ln.config.NumSuccessors)
	ln.finger = make([]*Node, ln.config.hashBits)
 	// // Register with the RPC mechanism
	done := make(chan string)
	go startHTTPserver(done,ln.address)
    fmt.Println(<-done)
	// ln.ring.transport.Register(&ln.Node, ln)
}

func startHTTPserver(done_chan chan<- string, address string) {
	log.Println("HTTP Server started for node ",address)
	iface:=new(Node_RPC)
	server_http := rpc.NewServer()
	registerServer(server_http,iface)
	// registers an HTTP handler for RPC messages on rpcPath, and a debugging handler on debugPath
	server_http.HandleHTTP("/", "/debug")

	// Listen for incoming tcp packets on specified port.
	l, e := net.Listen("tcp", ":6000")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	http.Serve(l,nil)
    done_chan <- "ServerClose"
}

func GenHash(conf Config, address string) []byte {
	// Use the hash funciton
	hash := conf.HashFunc()
	hash.Write([]byte(address))

	// Use the hash as the Id
	return hash.Sum(nil)
}

func (ln *LocalNode) Create() {
	ln.config.hashBits = ln.config.HashFunc().Size() * 8 //??

	ln.successors[0] = &ln.Node
	ln.predecessor = nil
}

func (ln *LocalNode) Join(address string) {
	// var n Node
	// n.address
}


func (ln *LocalNode) FindSuccessor_stub(key string, reply *string) error {
	err := ln.FindSuccessor(key,reply)
	return err
}
func (ln *LocalNode) GetPredecessor_stub(emp_arg struct{}, reply *string) error {
	err := ln.GetPredecessor(reply)
	return err
}
func (ln *LocalNode) GetSuccessor_stub(emp_arg struct{}, reply *string) error {
	err := ln.GetSuccessor(reply)
	return err
}
func (ln *LocalNode) Notify_stub(message string, emp_reply *struct{}) error {
	err := ln.Notify(message)
	return err
}
func (ln *LocalNode) Ping_stub(emp_arg struc{},emp_reply *struct{}) error {
	err:=ln.Ping()
	return err
}

func (ln *LocalNode) FindSuccessor(key string, reply *string) error{
	id_hash := GenHash(ln.config, key)
	my_hash := ln.Id
	succ_hash := ln.successors[0].Id
	if (bytes.compare(id_hash,my_hash)>0 && bytes.compare(id_hash,succ_hash)<=0) {
		*reply = ln.successors[0].address
		return nil
	}
	err := remote_FindSuccessor(ln.successors[0].address, key, reply)
	return err
}

func (ln *LocalNode) GetPredecessor(reply *string) (error) {
	if (ln.predecessor == nil) {
		return errors.New("Predecessor not found")
	}
	*reply=ln.predecessor.Address
	return nil
}

func (ln *LocalNode) GetSuccessor(reply *string) (error) {
	for _,successor := range ln.successors {
		if (successor!=nil) {
			*reply=successor.Address
			return nil
		}
	}
	
	return errors.New("Successor not found")
}

func (ln *LocalNode) Notify(message string) (error) {
	flag := false

	if (ln.predecessor == nil) {
		flag = true
	}
	if (!flag) {
		pred_hash := GenHash(ln.config, ln.predecessor.Address)
		new_hash := GenHash(ln.config, message)
		my_hash := ln.Id
		if (bytes.compare(new_hash, pred_hash) > 0 && bytes.compare(new_hash, my_hash) < 0) {
			flag = true
		}
	}

	if (flag) {
		ln.predecessor = &Node{GenHash(ln.config, message), message}
	}
	return nil
}

func (ln *LocalNode) Ping() (error) {
	return nil
}

// Remote Function Calls
func (ln *LocalNode) remote_FindSuccessor (address string, key string, reply *string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_FindSuccessor:", err)
        return err
    }
    err := t.Call("Node_RPC.FindSuccessor_Stub",key,reply)
	if err != nil {
    	log.Println("sync Call error in remote_FindSuccessor:", err) 
    	return err
	}
	return nill

}
func (ln *LocalNode) remote_GetPredecessor (address string, reply *string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_GetPredecessor:", err)
        return err
    }
    emp_Arg:=new(struct{})
    err := t.Call("Node_RPC.GetPredecessor_Stub",emp_Arg,reply)
	if err != nil {
    	log.Println("sync Call error in remote_GetPredecessor:", err) 
    	return err
	}
	return nill	
}
func (ln *LocalNode) remote_GetSuccessor (address string, reply *string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_GetSuccessor:", err)
        return err
    }
    emp_Arg:=new(struct{})
    err := t.Call("Node_RPC.GetSuccessor_Stub",emp_Arg,reply)
	if err != nil {
    	log.Println("sync Call error in remote_GetSuccessor:", err) 
    	return err
	}
	return nill	
}
func (ln *LocalNode) remote_Notify (address string, message string) error {
		var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_Notify:", err)
        return err
    }
    emp_reply:=new(struct{})
    err := t.Call("Node_RPC.Notify_Stub",message,&emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_Notify:", err) 
    	return err
	}
	return nill	
}
func (ln *LocalNode) remote_Ping (address string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_Ping:", err)
        return err
    }
    emp_reply:=new(struct{})
    emp_args:=new(struct{})
    err := t.Call("Node_RPC.Ping_Stub",emp_args,&emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_Ping:", err) 
    	return err
	}
	return nill	
}
func (ln *LocalNode) check_predecessor() {
	if (ln.predecessor != nil) {
		err := remote_Ping(ln.predecessor.address)
		if (err!=nil) {
			predecessor = nil
		}
	}

}

func (ln *LocalNode) stabilize() {
	
}