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
	"strconv"
	"math/rand"
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


// Represents a node, local or remote
type Node struct {
	Id   []byte // Unique Chord Id
	Address string // Host identifier
}

type LocalNode struct {
	Node
	Port 		string
	successors  []*Node
	finger      []*Node
	data		[]map[string]string
	// last_finger int
	predecessor *Node
	config Config
	// stabilized  time.Time
	timer       *time.Timer
}

func DefaultConfig() Config {
	return Config{
		sha1.New, // SHA1
		time.Duration(500 * time.Millisecond),
		time.Duration(2 * time.Second),
		3,   // 3 successors
		2,   // 3 Replicas
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
	ln.data = make([]map[string]string,ln.config.NumReplicas+1)
 	// // Register with the RPC mechanism
	done := make(chan string)
	go ln.startHTTPserver(done,ln.Address)
    // fmt.Println(<-done)
    fmt.Println("Initialised localNode")
	// ln.ring.transport.Register(&ln.Node, ln)
}

func (ln *LocalNode) startHTTPserver(done_chan chan<- string, address string) {
	log.Println("HTTP Server started for node ",address)
	// iface:=ln
	var iface Node_RPC
	iface = ln
	server_http := rpc.NewServer()
	registerServer(server_http,iface)
	// registers an HTTP handler for RPC messages on rpcPath, and a debugging handler on debugPath
	server_http.HandleHTTP("/", "/debug")

	// Listen for incoming tcp packets on specified port.
	l, e := net.Listen("tcp", ln.Port)
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
	ln.successors[1] = &ln.Node
	ln.successors[2] = &ln.Node
	ln.predecessor = nil

	ln.Schedule()
}

func (ln *LocalNode) Join(address string) error{
	// var n Node
	// n.address

	fmt.Println("Joining "+address)
	ln.predecessor = nil
	s_address := ""
	e := ln.remote_FindSuccessor(address, ln.Address, &s_address)
	if (e!= nil) {
		return e;
	}

	fmt.Println("Found successor "+s_address)
	succ := new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[0] = succ 
	fmt.Println("Successor 0 Updated: " + ln.successors[0].Address)

	e = ln.remote_GetSuccessor(ln.successors[0].Address, &s_address)
	if (e!= nil) {
		return e;
	}
	succ = new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[1] = succ 
	fmt.Println("Successor 1 Updated: " + ln.successors[1].Address)

	e = ln.remote_GetSuccessor(ln.successors[1].Address, &s_address)
	if (e!= nil) {
		return e;
	}
	succ = new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[2] = succ 
	fmt.Println("Successor 2 Updated: " + ln.successors[2].Address)

	e = ln.remote_StabilizeReplicasJoin(s_address,ln.Id,&ln.data)			//call StabilizeReplicasJoin and set ln.Address as predecessor of s_address
	return e
}

func (ln *LocalNode) Leave(address string) error{
	// add relevant code 
	e := ln.StabilizeReplicasLeave()			//assuming successor exists
	return e
}
func(ln *LocalNode) Heartbeat(rx_param hbeat, reply *hbeat ) error {
	fmt.Println(rx_param.Node_info.Address, " Active at Time: ", rx_param.Rx_time)
	(*reply).Node_info=ln.Node
	(*reply).Rx_time=time.Now()
	return nil
}
func (ln *LocalNode) FindSuccessor(key string, reply *string) error{
	id_hash := GenHash(ln.config, key)
	my_hash := ln.Id
	succ_hash := ln.successors[0].Id
	if (ln.Address == ln.successors[0].Address) {
		*reply = ln.Address
		return nil
	}
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
		fmt.Println("Predecessor Updated: " + ln.predecessor.Address)
	}
	return nil
}

func (ln *LocalNode) Ping() (error) {
	return nil
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

func (ln *LocalNode) StabilizeReplicasJoin(id []byte, data_pred *[]map[string]string) error {

	if len(ln.data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	
	new_map := ln.SplitMap(ln.data[0],id)

	(*data_pred)[0] = new_map
	(*data_pred)[1] = ln.data[1]
	(*data_pred)[2] = ln.data[2]

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

func (ln *LocalNode) check_predecessor() {
	if (ln.predecessor != nil) {
		err := ln.remote_Ping(ln.predecessor.Address)
		if (err!=nil) {
			ln.predecessor = nil
			fmt.Println("Predecessor Updated: nil")
		}
	}

}

func (ln *LocalNode) Stabilize() {
	ln.timer = nil

	fmt.Println("Stabilize called")

	defer ln.Schedule()

	if err := ln.checkNewSuccessor(); err != nil {
		fmt.Printf("Stabilize error: %s", err)
		return
	}

	if (ln.successors[0].Address == ln.Address) {
		return
	}
	
	if err := ln.remote_Notify(ln.successors[0].Address, ln.Address); err!=nil {
		fmt.Printf("Stabilize error: %s", err)
		return
	}

	ln.check_predecessor()
	
}

func (ln *LocalNode) checkNewSuccessor() error {
	successor := ""
	err := ln.GetSuccessor(&successor)
	if (err!=nil) {
		return err
	}

	predAddress := ""
	err = ln.remote_GetPredecessor(successor, &predAddress)

	if (err!=nil) {
		return nil
	}

	pred_hash := GenHash(ln.config, predAddress)
	succ_hash := GenHash(ln.config, successor)
	my_hash := ln.Id

	ln.successors[0].Address = successor
	ln.successors[0].Id = succ_hash


	if (bytes.Compare(pred_hash, my_hash)>0 && bytes.Compare(pred_hash, succ_hash)<0) {
		new_succ := new(Node)
		new_succ.Address = predAddress
		new_succ.Id = pred_hash
		ln.successors[0] = new_succ
		fmt.Println("Successor 0 Updated: " + ln.successors[0].Address)
		s_address := ""
		e := ln.remote_GetSuccessor(ln.successors[0].Address, &s_address)
		if (e!= nil) {
			return e
		}
		succ := new(Node)
		succ.Address = s_address
		succ.Id = GenHash(ln.config,s_address)
		ln.successors[1] = succ 
		fmt.Println("Successor 1 Updated: " + ln.successors[1].Address)

		e = ln.remote_GetSuccessor(ln.successors[1].Address, &s_address)
		if (e!= nil) {
			return e
		}
		succ = new(Node)
		succ.Address = s_address
		succ.Id = GenHash(ln.config,s_address)
		ln.successors[2] = succ 
		fmt.Println("Successor 2 Updated: " + ln.successors[2].Address)

	}

	return nil
}

func (ln *LocalNode) Schedule() {
	// Setup our stabilize timer
	ln.timer = time.AfterFunc(randStabilize(ln.config), ln.Stabilize)
}

func randStabilize(conf Config) time.Duration {
	min := conf.StabilizeMin
	max := conf.StabilizeMax
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

func (ln * LocalNode) ReadKey(key string, val *string) error{
	var leader string
	e := ln.FindSuccessor(key, &leader)
	if e!=nil {
		return e
	}
	e = ln.remote_ReadKeyLeader(leader,key,val)
	return e
}

func (ln *LocalNode) ReadKeyLeader(key string,val *string){
}
	
func (ln *LocalNode) WriteKey(key string, val string) error{
	var leader string
	e := ln.FindSuccessor(key, &leader)
	if e!=nil {
		return e
	}
	e = ln.remote_WriteKeyLeader(leader,key,val)

	return e
}

func (ln * LocalNode) WriteKeyLeader(key string, val string) error{
		ln.data[0][key] = val
		//check successor exists
		e0 := ln.remote_WriteKeySuccessor(ln.successors[0].Address,key,val,1)
		if e0!= nil {
			return e0
		}
		e1 := ln.remote_WriteKeySuccessor(ln.successors[1].Address,key,val,2)
		if e1!= nil {
			return e1
		}
		return nil
}

func (ln * LocalNode) WriteKeySuccessor(key string, val string, replica_number int) error{
	if len(ln.data) < replica_number {
		return errors.New("Not enough replicas")
	}
	ln.data[replica_number][key] = val
	return nil
}

func (ln *LocalNode) DeleteKey(key string) error{
	var leader string
	e := ln.FindSuccessor(key, &leader)
	if e!=nil {
		return e
	}
	e = ln.remote_DeleteKeyLeader(leader,key)
	return e
}

func (ln * LocalNode) DeleteKeyLeader(key string) error{
		_ , ok := ln.data[0][key]
		if ok == false{
			return errors.New("Key not present in Leader")
		}
		delete(ln.data[0],key)
		//check successor exists
		e0 := ln.remote_DeleteKeySuccessor(ln.successors[0].Address,key,1)
		if e0!= nil {
			return e0
		}
		e1 := ln.remote_DeleteKeySuccessor(ln.successors[1].Address,key,2)
		if e1!= nil {
			return e1
		}
		return nil
}

func (ln * LocalNode) DeleteKeySuccessor(key string, replica_number int) error{
	if len(ln.data) < replica_number {
		return errors.New("Not enough replicas")
	}
	 _ , ok := ln.data[replica_number][key]
	if ok == false{
		return errors.New("Key not present in replica" + strconv.Itoa(replica_number))
	}
	delete(ln.data[replica_number],key)
	return nil
}
