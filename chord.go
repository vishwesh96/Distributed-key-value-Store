package kvstore

import (
	"crypto/sha1"
	// "fmt"
	"hash"
	"time"
	"errors"
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
	data		[]map[string]string
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
	predecessor = nil
	s_address, e := FindSuccessor_Stub(address, ln.Address)
	var succ Node
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	successors[0] = succ 
	remote_StabilizeReplicasJoin(s_address,ln.Id,data)			//call StabilizeReplicasJoin and set ln.Address as predecessor of s_address
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
	succ_hash := successors[0].Id
	if (bytes.compare(id_hash,my_hash)>0 && bytes.compare(id_hash,succ_hash)<=0) {
		return (successors[0].address, nil)
	}
	s_address, e := FindSuccessor_Stub(successors[0].address, key)
	*reply = s_address
	return e
}

func (ln *LocalNode) GetPredecessor(reply *string) (error) {
	*reply=predecessor.Address
	return nil
}

func (ln *LocalNode) GetSuccessor(reply *string) (error) {
	*reply=successors[0].Address
	return nil
}

func (ln *LocalNode) Notify(message string) (error) {
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
	err := Ping_Stub(predecessor.address)
	if (err!=nil) {
		predecessor = nil
	}

}

func (ln *LocalNode) stabilize() {
	
}

func (ln *LocalNode) SplitMap(data map[string]string, id []byte) map[string]string{			//deletes from data and inserts in to new_map and returns
	var new_map map[string]string
	for key,val := range data{
		if(GenHash(ln.config,key).compare(id)<=0){
			new_map[key] = val
			delete(data,key)
		}
	}
	return new_map	
}

func (ln *LocalNode) AddMap(target map[string]string, source map[string]string) error{
	for key,val := range source{
		_ , ok = target[key]
		if ok == true {
			return errors.New("Key already in target map")
		}else{
			target[key] = val
		}
	}
	return nil
}

//RPC
func (ln *LocalNode) StabilizeReplicasJoin(id []byte, data_pred []map[string]string) error {

	if len(data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	new_map = SplitMap(data[0],id)
	
	data_pred[0] = new_map
	data_pred[1] = data[1]
	data_pred[2] = data[2]

	data[2] = data[1]
	data[1] = new_map	

	remote_SendReplicasSuccessor(successors[0].Address,id,1)			
	remote_SendReplicasSuccessor(successors[1].Address,id,2)			
}

//RPC
func (ln *LocalNode) SendReplicasSuccessor(id []byte,replica_number int) error {
	if len(data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	if replica_number == 1 {
		new_map = SplitMap(data[1],id)
		data[2] = new_map
	} else if replica_number = 2 {
		new_map = SplitMap(data[2],id)
	}
}

