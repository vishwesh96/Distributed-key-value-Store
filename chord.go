package kvstore

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"log"
	"net"
	"net/http"
	"net/rpc"
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
type RPC_Join struct {
	id []byte
	replica_number int
}
type RPC_Leave struct {
	pred_data map[string]string
	replica_number int
}
type Node_RPC interface{
	FindSuccessor_stub(key string, reply *string) error
	GetPredecessor_stub(emp_arg struct{}, reply *string) error
	Notify_stub(message string, emp_reply *struct{}) error
	Ping_stub(emp_arg struct{},emp_reply *struct{}) error
	StabilizeReplicasJoin_stub(id []byte, data_pred []map[string]string) error 
	SendReplicasSuccessorJoin_stub(args RPC_Join, emp_reply *struct{}) error 
	SendReplicasSuccessorLeave_stub(args RPC_Leave, emp_reply *struct{}) error
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
func registerServer(server *rpc.Server, iface Node_RPC) {
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
	go ln.startHTTPserver(done,ln.Address)
    fmt.Println(<-done)
	// ln.ring.transport.Register(&ln.Node, ln)
}

func (ln *LocalNode) startHTTPserver(done_chan chan<- string, address string) {
	log.Println("HTTP Server started for node ",address)
	iface:=ln
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

func (ln *LocalNode) Join(address string) error{
	// var n Node
	// n.address
	ln.predecessor = nil
	s_address := ""
	e := ln.remote_FindSuccessor(address, ln.Address, &s_address)
	if (e!= nil) {

		return e;
	}
	succ := new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[0] = succ 
	e = ln.remote_StabilizeReplicasJoin(s_address,ln.Id,ln.data)			//call StabilizeReplicasJoin and set ln.Address as predecessor of s_address
	return e
}

func (ln *LocalNode) Leave(address string) error{
	// add relevant code 
	e := ln.StabilizeReplicasLeave()			//assuming successor exists
	return e
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
func (ln *LocalNode) Ping_stub(emp_arg struct{},emp_reply *struct{}) error {
	err:=ln.Ping()
	return err
}
func(ln *LocalNode) StabilizeReplicasJoin_stub(id []byte, data_pred []map[string]string) error {
	err:= ln.StabilizeReplicasJoin(id,data_pred)
	return err
} 
func(ln *LocalNode)	SendReplicasSuccessorJoin_stub(args RPC_Join, emp_reply *struct{}) error {
	err:= ln.SendReplicasSuccessorJoin(args.id,args.replica_number)
	return err
}
func(ln *LocalNode)	SendReplicasSuccessorLeave_stub(args RPC_Leave, emp_reply *struct{}) error {
	err:= ln.SendReplicasSuccessorLeave(args.pred_data,args.replica_number)
	return err	
}

func (ln *LocalNode) FindSuccessor(key string, reply *string) error{
	id_hash := GenHash(ln.config, key)
	my_hash := ln.Id
	succ_hash := ln.successors[0].Id
	if (bytes.Compare(id_hash,my_hash)>0 && bytes.Compare(id_hash,succ_hash)<=0) {
		*reply = ln.successors[0].Address
		return nil
	}
	err := ln.remote_FindSuccessor(ln.successors[0].Address, key, reply)
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
		if (bytes.Compare(new_hash, pred_hash) > 0 && bytes.Compare(new_hash, my_hash) < 0) {
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
    err = t.Call("Node_RPC.FindSuccessor_Stub",key,reply)
	if err != nil {
    	log.Println("sync Call error in remote_FindSuccessor:", err) 
    	return err
	}
	return nil

}
func (ln *LocalNode) remote_GetPredecessor (address string, reply *string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_GetPredecessor:", err)
        return err
    }
    emp_Arg:=new(struct{})
    err = t.Call("Node_RPC.GetPredecessor_Stub",emp_Arg,reply)
	if err != nil {
    	log.Println("sync Call error in remote_GetPredecessor:", err) 
    	return err
	}
	return nil	
}
func (ln *LocalNode) remote_GetSuccessor (address string, reply *string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_GetSuccessor:", err)
        return err
    }
    emp_Arg:=new(struct{})
    err = t.Call("Node_RPC.GetSuccessor_Stub",emp_Arg,reply)
	if err != nil {
    	log.Println("sync Call error in remote_GetSuccessor:", err) 
    	return err
	}
	return nil	
}
func (ln *LocalNode) remote_Notify (address string, message string) error {
		var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_Notify:", err)
        return err
    }
    emp_reply:=new(struct{})
    err = t.Call("Node_RPC.Notify_Stub",message,&emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_Notify:", err) 
    	return err
	}
	return nil	
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
    err = t.Call("Node_RPC.Ping_Stub",emp_args,&emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_Ping:", err) 
    	return err
	}
	return nil	
}
func (ln *LocalNode) remote_StabilizeReplicasJoin(address string, id []byte, data_pred []map[string]string) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_StabilizeReplicasJoin:", err)
        return err
    }
    err = t.Call("Node_RPC.StabilizeReplicasJoin_stub",id,data_pred)
	if err != nil {
    	log.Println("sync Call error in remote_StabilizeReplicasJoin:", err) 
    	return err
	}
	return nil			
}

func (ln *LocalNode) remote_SendReplicasSuccessorJoin(address string, id []byte,replica_number int) error {
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_SendReplicasSuccessorJoin:", err)
        return err
    }
    emp_reply:=new(struct{})
    var args RPC_Join
    args.id=id
    args.replica_number=replica_number
    err = t.Call("Node_RPC.SendReplicasSuccessorJoin",args,emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_SendReplicasSuccessorJoin:", err) 
    	return err
	}
	return nil	
}
func (ln *LocalNode) remote_SendReplicasSuccessorLeave(address string, pred_data map[string]string,replica_number int) error{
	var complete_address = address+":6000"
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_SendReplicasSuccessorLeave:", err)
        return err
    }
    emp_reply:=new(struct{})
    var args RPC_Leave
    args.pred_data=pred_data
    args.replica_number=replica_number
    err = t.Call("Node_RPC.SendReplicasSuccessorLeave_stub",args,emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_SendReplicasSuccessorLeave:", err) 
    	return err
	}
	return nil	
}

func (ln *LocalNode) check_predecessor() {
	if (ln.predecessor != nil) {
		err := ln.remote_Ping(ln.predecessor.Address)
		if (err!=nil) {
			ln.predecessor = nil
		}
	}

}

func (ln *LocalNode) stabilize() {
	
}

func (ln *LocalNode) SplitMap(data map[string]string, id []byte) map[string]string{			//deletes from data and inserts in to new_map and returns
	var new_map map[string]string
	for key,val := range data{
		if(bytes.Compare(GenHash(ln.config,key),id)<=0){
			new_map[key] = val
			delete(data,key)
		}
	}
	return new_map	
}

func (ln *LocalNode) AddMap(target map[string]string, source map[string]string) error{
	for key,val := range source{
		_ , ok := target[key]
		if ok == true {
			return errors.New("Key already in target map")
		}else{
			target[key] = val
		}
	}
	return nil
}

func (ln *LocalNode) StabilizeReplicasJoin(id []byte, data_pred []map[string]string) error {

	if len(ln.data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	new_map := ln.SplitMap(ln.data[0],id)

	data_pred[0] = new_map
	data_pred[1] = ln.data[1]
	data_pred[2] = ln.data[2]

	ln.data[2] = ln.data[1]
	ln.data[1] = new_map	

	e0 := ln.remote_SendReplicasSuccessorJoin(ln.successors[0].Address,id,1)	
	if e0 != nil{
		return e0
	}		
	e1 := ln.remote_SendReplicasSuccessorJoin(ln.successors[1].Address,id,2)			
	if e1 != nil{
		return e1
	}
	return nil
}

func (ln *LocalNode) SendReplicasSuccessorJoin(id []byte,replica_number int) error {
	if len(ln.data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	if replica_number == 1 {
		new_map := ln.SplitMap(ln.data[1],id)
		ln.data[2] = new_map
	} else if replica_number == 2 {
		ln.SplitMap(ln.data[2],id)
	}
	return nil
}


func (ln *LocalNode) StabilizeReplicasLeave() error {
	e0 := ln.remote_SendReplicasSuccessorLeave(ln.successors[0].Address,ln.data[2],0)
	if e0 != nil{
		return e0
	}
	e1 := ln.remote_SendReplicasSuccessorLeave(ln.successors[1].Address,ln.data[1],1)
	if e1 != nil{
		return e1
	}
	e2 := ln.remote_SendReplicasSuccessorLeave(ln.successors[2].Address,ln.data[0],2)
	if e2 != nil{
		return e2
	}
	return nil
}

func (ln *LocalNode) SendReplicasSuccessorLeave(pred_data map[string]string,replica_number int) error{
	var e error
	switch replica_number {
		case 0 :
		{
			e = ln.AddMap(ln.data[0],ln.data[1])
			ln.data[1] = ln.data[2]
			ln.data[2] = pred_data
		}
		case 1 : 
		{
			e = ln.AddMap(ln.data[1],ln.data[2])
			ln.data[2] = pred_data
		}
		case 2 : 
		{
			e = ln.AddMap(ln.data[2],pred_data)
		}
		default :
		{
			//TODO
		}
	
	}
	return e
}
