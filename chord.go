package kvstore

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"hash"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"math/rand"
	"sync"
	"strconv"
	"os"
)

type Config struct {
	HashFunc      func() hash.Hash // Hash function to use
	StabilizeMin  time.Duration    // Minimum stabilization time
	StabilizeMax  time.Duration    // Maximum stabilization time
	HeartBeatTime time.Duration
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
	shutdown 	bool
	mux			*sync.Mutex
	Prev_read int
	logfile *os.File
}

func DefaultConfig() Config {
	return Config{
		sha1.New, // SHA1
		time.Duration(1 * time.Second),
		time.Duration(2 * time.Second),
		time.Duration(500 * time.Millisecond),
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
	ln.shutdown = false
	ln.mux = &sync.Mutex{}
	var err error
	ln.logfile, err = os.OpenFile(ln.Address+".log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
    	log.Fatal("error opening file: %v", err)
	}
	defer ln.logfile.Close()

	log.SetOutput(ln.logfile)
	log.Println("Log for the node "+ln.Address)
	// Initialize all state

	ln.successors = make([]*Node, ln.config.NumSuccessors)
	ln.finger = make([]*Node, ln.config.hashBits)
	ln.data = make([]map[string]string,ln.config.NumReplicas+1)
	for i:=0;i<3;i++{
		ln.data[i] = make(map[string]string)
	}
 	// // Register with the RPC mechanism
	done := make(chan string)
	go ln.startHTTPserver(done,ln.Address)
    log.SetOutput(os.Stderr)
    log.Println("Initialised localNode")
    log.SetOutput(ln.logfile)
    log.Println("Initialised localNode")
	
	// ln.ring.transport.Register(&ln.Node, ln)
}

func (ln *LocalNode) startHTTPserver(done_chan chan<- string, address string) {
	log.SetOutput(os.Stderr)
	log.Println("HTTP Server started for node ", address)
	log.SetOutput(ln.logfile)
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
		log.SetOutput(os.Stderr)
    	log.Fatal("listen error:", e)
    	log.SetOutput(ln.logfile)
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
	ln.mux.Lock()
	ln.successors[0] = &ln.Node
	ln.successors[1] = &ln.Node
	ln.successors[2] = &ln.Node
	ln.predecessor = nil
	ln.mux.Unlock()
	ln.Schedule()
}

func (ln *LocalNode) Join(address string) error{
	// var n Node
	// n.address
	log.SetOutput(os.Stderr)
	log.Println("Joining "+address)
    log.SetOutput(ln.logfile)
	log.Println("Joining "+address)
	ln.predecessor = nil
	s_address := ""
	e := ln.remote_FindSuccessor(address, ln.Address, &s_address)
	if (e!= nil) {
		return e;
	}

	log.SetOutput(os.Stderr)
	log.Println("Found successor "+s_address)
    log.SetOutput(ln.logfile)
	log.Println("Found successor "+s_address)
	succ := new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[0] = succ 
	log.SetOutput(os.Stderr)
	log.Println("Successor 0 Updated: " + ln.successors[0].Address)
    log.SetOutput(ln.logfile)
	log.Println("Successor 0 Updated: " + ln.successors[0].Address)

	e = ln.remote_GetSuccessor(ln.successors[0].Address, &s_address)
	if (e!= nil) {
		return e;
	}
	succ = new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[1] = succ 
	log.SetOutput(os.Stderr)
	log.Println("Successor 1 Updated: " + ln.successors[1].Address)
    log.SetOutput(ln.logfile)
	log.Println("Successor 1 Updated: " + ln.successors[1].Address)

	e = ln.remote_GetSuccessor(ln.successors[1].Address, &s_address)
	if (e!= nil) {
		return e;
	}
	succ = new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[2] = succ 
	log.SetOutput(os.Stderr)
	log.Println("Successor 2 Updated: " + ln.successors[2].Address)
    log.SetOutput(ln.logfile)
	log.Println("Successor 2 Updated: " + ln.successors[2].Address)
	var ret_args RPC_StabJoin 

	e = ln.remote_StabilizeReplicasJoin(ln.successors[0].Address,ln.Id,&ret_args)			//call StabilizeReplicasJoin and set ln.Address as predecessor of s_address
	if (e!= nil) {
		return e;
	}
	for i:=0;i<3;i++{
		CopyMap(ln.data[i],ret_args.Data_pred[i])
	}
	ln.Stabilize()
	return nil
}

func (ln *LocalNode) Leave() error{
	// add relevant code 
	e := ln.StabilizeReplicasLeave()			//assuming successor exists
	if (e!=nil) {
		return e
	}
	ln.mux.Lock()
	if (ln.timer!=nil) {
		ln.timer.Stop()	
	}
	ln.shutdown = true
	if (ln.predecessor != nil) {
		addr := ln.predecessor.Address
		ln.mux.Unlock()
		ln.remote_SkipSuccessor(addr)
	}else {
		ln.mux.Unlock()
	}

	return nil
}
func(ln *LocalNode) Heartbeat(rx_param Hbeat, reply *Hbeat ) error {
	//log.Println(rx_param.Node_info.Address, " Active at Time: ", rx_param.Rx_time)
	(*reply).Node_info=ln.Node
	(*reply).Rx_time=time.Now()
	return nil
}
func (ln *LocalNode) FindSuccessor(key string, reply *string) error{
	ln.mux.Lock()
	id_hash := GenHash(ln.config, key)
	my_hash := ln.Id
	succ_hash := ln.successors[0].Id
	if (ln.Address == ln.successors[0].Address) {
		*reply = ln.Address
		ln.mux.Unlock()
		return nil
	}
	if (betweenRightIncl(my_hash, succ_hash, id_hash)) {
		*reply = ln.successors[0].Address
		ln.mux.Unlock()
		return nil
	}
	addr := ln.successors[0].Address
		ln.mux.Unlock()
	err := ln.remote_FindSuccessor(addr, key, reply)
	return err
}

func (ln *LocalNode) GetPredecessor(reply *string) (error) {
		ln.mux.Lock()

	if (ln.predecessor == nil) {
		ln.mux.Unlock()
		return errors.New("Predecessor not found")
	}
	*reply=ln.predecessor.Address
		ln.mux.Unlock()
	return nil
}

func (ln *LocalNode) GetSuccessor(reply *string) (error) {
		ln.mux.Lock()
	for _,successor := range ln.successors {
		if (successor!=nil) {
			*reply=successor.Address
			ln.mux.Unlock()
			return nil
		}
	}
	ln.mux.Unlock()
	return errors.New("Successor not found")
}

func (ln *LocalNode) GetRemoteData(replica_number int,reply *map[string]string) (error) {
		ln.mux.Lock()
	*reply=ln.data[replica_number]
		ln.mux.Unlock()
	return nil
}

func (ln *LocalNode) Notify(message string) (error) {
		ln.mux.Lock()
	flag := false

	if (ln.predecessor == nil) {
		flag = true
	}
	if (!flag) {
		pred_hash := GenHash(ln.config, ln.predecessor.Address)
		new_hash := GenHash(ln.config, message)
		my_hash := ln.Id
		if (between(pred_hash, my_hash, new_hash)) {
			flag = true
		}
	}

	if (flag) {
		ln.predecessor = &Node{GenHash(ln.config, message), message}
		log.SetOutput(os.Stderr)
		log.Println("Predecessor Updated: " + ln.predecessor.Address)
		log.SetOutput(ln.logfile)
		log.Println("Predecessor Updated: " + ln.predecessor.Address)
	}
		ln.mux.Unlock()
	return nil
}

func (ln *LocalNode) Ping() (error) {
	return nil
}

func (ln *LocalNode) SkipSuccessor() error{
		ln.mux.Lock()
	ln.successors[0] = ln.successors[1]
	if (ln.successors[0] != nil) {
		log.SetOutput(os.Stderr)
		log.Println("Successor 0 Updated: " + ln.successors[0].Address)
		log.SetOutput(ln.logfile)
		log.Println("Successor 0 Updated: " + ln.successors[0].Address)	
	} else {
		ln.mux.Unlock()
        return errors.New("Empty Successive Successor")
    }
	
	ln.successors[1] = ln.successors[2]
	if (ln.successors[1] != nil) {
		log.SetOutput(os.Stderr)
		log.Println("Successor 1 Updated: " + ln.successors[1].Address)
		log.SetOutput(ln.logfile)
		log.Println("Successor 1 Updated: " + ln.successors[1].Address)
	} else {
		ln.mux.Unlock()
        return errors.New("Empty Successive Successor")
    }
	
	var s_address string
	e := ln.remote_GetSuccessor(ln.successors[1].Address, &s_address)
	if (e!= nil) {
		ln.mux.Unlock()
		return e;
	}
	succ := new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	ln.successors[2] = succ 
	log.SetOutput(os.Stderr)
	log.Println("Successor 2 Updated: " + ln.successors[2].Address)
	log.SetOutput(ln.logfile)	
	log.Println("Successor 2 Updated: " + ln.successors[2].Address)
		ln.mux.Unlock()
	return nil
}

func (ln *LocalNode) SplitMap(data map[string]string, id []byte, pred_id []byte) map[string]string{			//deletes from data and inserts in to new_map and returns
	new_map := make(map[string]string)
	for key,val := range data{
		if bytes.Compare(ln.Id,ln.successors[0].Id) == 0{
			if(betweenRightIncl(ln.Id,id,GenHash(ln.config,key))){
			// if(bytes.Compare(,id)<=0 && bytes.Compare(GenHash(ln.config,key),ln.Id) > 0){
				new_map[key] = val
				delete(data,key)
			}	
		} else{
			if(betweenRightIncl(pred_id,id,GenHash(ln.config,key))){
			// if(bytes.Compare(GenHash(ln.config,key),id)<=0 && bytes.Compare(GenHash(ln.config,key),pred_id) > 0){
				new_map[key] = val
				delete(data,key)
			}
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

func (ln *LocalNode) StabilizeReplicasJoin(id []byte, ret_args *RPC_StabJoin) error {

	var e0 error
	var e1 error
	var new_map map[string]string

	if len(ln.data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	if ln.predecessor != nil{
		new_map = ln.SplitMap(ln.data[0],id,ln.predecessor.Id)
		// log.Println("NEW MAP")
		// PrintMap(new_map)
	}else{
		new_map = ln.SplitMap(ln.data[0],id,nil)
	}

	for i:=0;i<3;i++{
		(*ret_args).Data_pred[i] = make(map[string]string)		
	}

	if (bytes.Compare(ln.Id,ln.successors[0].Id) == 0){				//If only one node
		CopyMap(((*ret_args).Data_pred[0]),new_map)
		CopyMap(((*ret_args).Data_pred[1]),ln.data[0])	
		CopyMap(ln.data[1],new_map)	

	}else if (bytes.Compare(ln.Id,ln.successors[1].Id) == 0){
		CopyMap(((*ret_args).Data_pred[0]),new_map)
		CopyMap(((*ret_args).Data_pred[1]),ln.data[1])
		CopyMap(((*ret_args).Data_pred[2]),ln.data[0])

		CopyMap(ln.data[2],ln.data[1])
		CopyMap(ln.data[1],new_map)	

		if ln.predecessor != nil{
	 		e0 = ln.remote_SendReplicasSuccessorJoin(ln.successors[0].Address,id,ln.predecessor.Id,1)	
		}else{
	 		e0 = ln.remote_SendReplicasSuccessorJoin(ln.successors[0].Address,id,nil,1)			
		}
		if e0 != nil{
			return e0
		}		
		if ln.predecessor != nil{
			e1 = ln.remote_SendReplicasSuccessorJoin(ln.successors[1].Address,id,ln.predecessor.Id,2)			
		}else{
			e1 = ln.remote_SendReplicasSuccessorJoin(ln.successors[1].Address,id,nil,2)
		}

		if e1 != nil{
			return e1
		}
	}else{
		CopyMap(((*ret_args).Data_pred[0]),new_map)
		CopyMap(((*ret_args).Data_pred[1]),ln.data[1])
		CopyMap(((*ret_args).Data_pred[2]),ln.data[2])

		CopyMap(ln.data[2],ln.data[1])
		CopyMap(ln.data[1],new_map)	

		if ln.predecessor != nil{
	 		e0 = ln.remote_SendReplicasSuccessorJoin(ln.successors[0].Address,id,ln.predecessor.Id,1)	
		}else{
	 		e0 = ln.remote_SendReplicasSuccessorJoin(ln.successors[0].Address,id,nil,1)			
		}
		if e0 != nil{
			return e0
		}		
		if ln.predecessor != nil{
			e1 = ln.remote_SendReplicasSuccessorJoin(ln.successors[1].Address,id,ln.predecessor.Id,2)			
		}else{
			e1 = ln.remote_SendReplicasSuccessorJoin(ln.successors[1].Address,id,nil,2)
		}

		if e1 != nil{
			return e1
		}
	}
	// log.Println("StabilizeReplicasJoin")
	// ln.PrintAllMaps()

	return nil
}

func (ln *LocalNode) SendReplicasSuccessorJoin(id []byte,pred_id []byte, replica_number int) error {
	if len(ln.data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	if replica_number == 1 {
		new_map := ln.SplitMap(ln.data[1],id,pred_id)
		CopyMap(ln.data[2],new_map)
	} else if replica_number == 2 {
		ln.SplitMap(ln.data[2],id,pred_id)
	}

	return nil
}


func (ln *LocalNode) StabilizeReplicasLeave() error {
	var e0 error
	var e1 error
	var e2 error
	if(bytes.Compare(ln.Id,ln.successors[1].Id)==0){
		e0 = ln.remote_SendReplicasSuccessorLeave(ln.successors[0].Address,ln.data[2],0)
		if e0 != nil{
			return e0
		}		
	}else if (bytes.Compare(ln.Id,ln.successors[2].Id)==0){
		e0 = ln.remote_SendReplicasSuccessorLeave(ln.successors[0].Address,nil,0)
		if e0 != nil{
			return e0
		}
		e1 = ln.remote_SendReplicasSuccessorLeave(ln.successors[1].Address,nil,1)
		if e1 != nil{
			return e1
		}
	}else {
		e0 = ln.remote_SendReplicasSuccessorLeave(ln.successors[0].Address,ln.data[2],0)
		if e0 != nil{
			return e0
		}
		e1 = ln.remote_SendReplicasSuccessorLeave(ln.successors[1].Address,ln.data[1],1)
		if e1 != nil{
			return e1
		}
		e2 = ln.remote_SendReplicasSuccessorLeave(ln.successors[2].Address,ln.data[0],2)
		if e2 != nil{
			return e2
		}	
	}	
	return nil
}

func (ln *LocalNode) SendReplicasSuccessorLeave(pred_data map[string]string,replica_number int) error{
	var e error
	switch replica_number {
		case 0 :
		{
			e = ln.AddMap(ln.data[0],ln.data[1])
			CopyMap(ln.data[1],ln.data[2])
			CopyMap(ln.data[2],pred_data)
		}
		case 1 : 
		{
			e = ln.AddMap(ln.data[1],ln.data[2])
			CopyMap(ln.data[2],pred_data)
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
		ln.mux.Lock()
	if (ln.predecessor != nil) {
		addr := ln.predecessor.Address
		ln.mux.Unlock()
		err := ln.remote_Ping(addr)
		ln.mux.Lock()
		if (err!=nil) {
			ln.predecessor = nil
			log.SetOutput(os.Stderr)
			log.Println("Predecessor Updated: nil")
			log.SetOutput(ln.logfile)
			log.Println("Predecessor Updated: nil")
		}
	}

		ln.mux.Unlock()
}

func (ln *LocalNode) Stabilize() {
	ln.timer = nil

	// fmt.Println("Stabilize called")
	
	ln.HeartBeatCheck()

	defer ln.Schedule()

	if (ln.successors[0]!=nil) {
		if err := ln.updateSuccessors(); err != nil {
			
			log.SetOutput(os.Stderr)
			log.Printf("Stabilize error: %s", err)
			log.SetOutput(ln.logfile)
			log.Printf("Stabilize error: %s", err)
			return
		}
	}

	if err := ln.checkNewSuccessor(); err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("Stabilize error: %s", err)
		log.SetOutput(ln.logfile)	
		log.Printf("Stabilize error: %s", err)
		return
	}

	if (ln.successors[0]!=nil && ln.successors[0].Address == ln.Address) {
		ln.check_predecessor()
		if (ln.predecessor!=nil) {
			ln.successors[0] = ln.predecessor
			log.SetOutput(os.Stderr)
			log.Println("Successor 0 Updated: " + ln.successors[0].Address)
			log.SetOutput(ln.logfile)
			log.Println("Successor 0 Updated: " + ln.successors[0].Address)
		}
		return
	}

	if err := ln.updateSuccessors(); err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("Stabilize error: %s", err)
		log.SetOutput(ln.logfile)	
		log.Printf("Stabilize error: %s", err)
		return
	}
	
	if err := ln.remote_Notify(ln.successors[0].Address, ln.Address); err!=nil {
		log.SetOutput(os.Stderr)
		log.Printf("Stabilize error: %s", err)
		log.SetOutput(ln.logfile)	
		log.Printf("Stabilize error: %s", err)
		return
	}

	ln.check_predecessor()
	
}

func (ln *LocalNode) HeartBeatCheck() {
	Hbeat_start := time.Now()
	//Wait for 100 seconds to hear a ping
	reply_0:=new(Hbeat)
	reply_1:=new(Hbeat)
	reply_2:=new(Hbeat)
	err_0:=ln.Remote_Heartbeat(ln.successors[0].Address,reply_0)
	err_1:=ln.Remote_Heartbeat(ln.successors[1].Address,reply_1)
	err_2:=ln.Remote_Heartbeat(ln.successors[2].Address,reply_2)
		// ln.mux.Lock()
	var succ_data map[string]string
	for ((time.Since(Hbeat_start))*time.Second<ln.config.HeartBeatTime) {
	}

	if((err_0!=nil) || (reply_0==nil)) {
		log.SetOutput(os.Stderr)
		log.Println("Successor 0 not responding")
		log.SetOutput(ln.logfile)	
		log.Println("Successor 0 not responding")
		//Correcting Successor Relationships
		failNode := ln.successors[0].Address
		ln.successors[0]=ln.successors[1]
		if (ln.successors[0]!=nil) {
			log.SetOutput(os.Stderr)
			log.Println("Successor 0 Updated: " + ln.successors[0].Address)
			log.SetOutput(ln.logfile)	
			log.Println("Successor 0 Updated: " + ln.successors[0].Address)
		}else {
			log.SetOutput(os.Stderr)
			log.Fatal("Multiple Replica Failures, Aboting...")
			log.SetOutput(ln.logfile)	
			log.Fatal("Multiple Replica Failures, Aboting...")
		}
		ln.successors[1]=ln.successors[2]
		if (ln.successors[1]!=nil) {
			log.SetOutput(os.Stderr)
			log.Println("Successor 1 Updated: " + ln.successors[1].Address)
			log.SetOutput(ln.logfile)		
			log.Println("Successor 1 Updated: " + ln.successors[1].Address)
			if (ln.successors[1].Address != failNode) {
				s_address := ""
				// ln.mux.Unlock()
				e := ln.remote_GetSuccessor(ln.successors[2].Address, &s_address)
				// ln.mux.Lock()
				if (e!= nil) {
					log.SetOutput(os.Stderr)
					log.Fatal("Cant get successor in Stabilize, Aborting...")
					log.SetOutput(ln.logfile)	
					log.Fatal("Cant get successor in Stabilize, Aborting...")
				}
				succ := new(Node)
				succ.Address = s_address
				succ.Id = GenHash(ln.config,s_address)
				if (ln.successors[2].Address!=s_address) {
					
					log.SetOutput(os.Stderr)
					log.Println("Successor 2 Updated: " + s_address)
					log.SetOutput(ln.logfile)		
					log.Println("Successor 2 Updated: " + s_address	)
				}
				ln.successors[2] = succ 

			}

		}
		
		//Successor 0 is down: Collect Data Also
		// ln.mux.Unlock()
		succ_err:=ln.remote_GetRemoteData(ln.successors[0].Address,1,&succ_data)
		// ln.mux.Lock()
		if(succ_err!=nil) {

			log.SetOutput(os.Stderr)
			log.Fatal("Unexepected Failure, Aborting...",succ_err)
			log.SetOutput(ln.logfile)		
			log.Fatal("Unexepected Failure, Aborting...",succ_err)
		}
		// ln.mux.Unlock()
		err00:=ln.remote_SendReplicasSuccessorLeave(ln.successors[0].Address,ln.data[1],0)
		// ln.mux.Lock()
		if(err00!=nil){

			log.SetOutput(os.Stderr)
			log.Fatal("Unexepected Failure, Aborting...",succ_err)
			log.SetOutput(ln.logfile)		
			log.Fatal("Unexepected Failure, Aborting...")
		}
		if (ln.successors[1].Address != failNode) {
			// ln.mux.Unlock()
			err01:=ln.remote_SendReplicasSuccessorLeave(ln.successors[1].Address,ln.data[0],1)
			// ln.mux.Lock()
			if(err01!=nil){

				log.SetOutput(os.Stderr)
				log.Fatal("Unexepected Failure, Aborting...",succ_err)
				log.SetOutput(ln.logfile)		
				log.Fatal("Unexepected Failure, Aborting...")
			}
		}
		if (ln.successors[2].Address != failNode) {
			// ln.mux.Unlock()
			err02:=ln.remote_SendReplicasSuccessorLeave(ln.successors[2].Address,succ_data,2)
			// ln.mux.Lock()
			if(err02!=nil){

				log.SetOutput(os.Stderr)
				log.Fatal("Unexepected Failure, Aborting...",succ_err)
				log.SetOutput(ln.logfile)		
				log.Fatal("Unexepected Failure, Aborting...")
			}
		}
	}
	if((err_1!=nil) || (reply_1==nil)) {
		
		log.SetOutput(os.Stderr)
		log.Println("Successor 1 not responding")
		log.SetOutput(ln.logfile)		
		log.Println("Successor 1 not responding")
		//Successor 1 is down
		failNode := ln.successors[1].Address
		ln.successors[1]=ln.successors[2]
		if (ln.successors[1]!=nil && ln.successors[1].Address != failNode) {
			
			log.SetOutput(os.Stderr)
			log.Println("Successor 1 Updated: " + ln.successors[2].Address)
			log.SetOutput(ln.logfile)			
			log.Println("Successor 1 Updated: " + ln.successors[2].Address)
			s_address := ""
			addr:=ln.successors[2].Address
		// ln.mux.Unlock()
			e := ln.remote_GetSuccessor(addr, &s_address)
		// ln.mux.Lock()
			if (e!= nil) {
				
				log.SetOutput(os.Stderr)
				log.Fatal("Cant get successor in Stabilize, Aboting...")
				log.SetOutput(ln.logfile)			
				log.Fatal("Cant get successor in Stabilize, Aboting...")
			}
			succ := new(Node)
			succ.Address = s_address
			succ.Id = GenHash(ln.config,s_address)
			if (ln.successors[1].Address!=s_address) {
				
				log.SetOutput(os.Stderr)
				log.Println("Successor 2 Updated: " + ln.successors[2].Address)
				log.SetOutput(ln.logfile)			
				log.Println("Successor 2 Updated: " + s_address	)
			}
			ln.successors[2] = succ
		}
		 

	}	
	if((err_2!=nil) || (reply_2==nil)) {
		log.SetOutput(os.Stderr)
		log.Println("Successor 2 not responding")
		log.SetOutput(ln.logfile)			
		log.Println("Successor 2 not responding")
		// s_address := ""
		// e := ln.remote_GetSuccessor(ln.successors[2].Address, &s_address)
		// if (e!= nil) {
		// 	log.Fatal("Cant get successor in Stabilize, Aboting...")
		// }
		// succ := new(Node)
		// succ.Address = s_address
		// succ.Id = GenHash(ln.config,s_address)
		// if (ln.successors[1].Address!=s_address) {
		// 	log.Println("Successor 2 Updated: " + s_address	)
		// }
		// ln.successors[2] = succ 

	}
		// ln.mux.Unlock()
}

func (ln *LocalNode) checkNewSuccessor() error {
		
	successor := ""
	err := ln.GetSuccessor(&successor)
	if (err!=nil) {
		return err
	}

		// ln.mux.Lock()
	predAddress := ""
		// ln.mux.Unlock()
	err = ln.remote_GetPredecessor(successor, &predAddress)
		// ln.mux.Lock()
	// log.Println("Failed Predecessor")
	if (err!=nil) {
		// ln.mux.Unlock()
		return nil
	}

	pred_hash := GenHash(ln.config, predAddress)
	succ_hash := GenHash(ln.config, successor)
	my_hash := ln.Id

	ln.successors[0].Address = successor
	ln.successors[0].Id = succ_hash


	if (between(my_hash, succ_hash, pred_hash)) {
		new_succ := new(Node)
		new_succ.Address = predAddress
		new_succ.Id = pred_hash
		ln.successors[0] = new_succ
		log.SetOutput(os.Stderr)
		log.Println("Successor 0 Updated: " + ln.successors[0].Address)
		log.SetOutput(ln.logfile)			
		log.Println("Successor 0 Updated: " + ln.successors[0].Address)
		

	}

		// ln.mux.Unlock()
	return nil
}

func (ln *LocalNode) updateSuccessors() error {
		// ln.mux.Lock()
	s_address := ""
	addr := ln.successors[0].Address
		// ln.mux.Unlock()
	e := ln.remote_GetSuccessor(addr, &s_address)
		// ln.mux.Lock()
	if (e!= nil) {
		// ln.mux.Unlock()
		return e
	}
	succ := new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	if (ln.successors[1].Address!=s_address) {
		log.SetOutput(os.Stderr)
		log.Println("Successor 1 Updated: " + s_address)
		log.SetOutput(ln.logfile)			
		
		log.Println("Successor 1 Updated: " + s_address	)
	}
	ln.successors[1] = succ 

	addr = ln.successors[1].Address
		// ln.mux.Unlock()
	e = ln.remote_GetSuccessor(addr, &s_address)
		// ln.mux.Lock()
	if (e!= nil) {
		// ln.mux.Unlock()
		return e
	}
	succ = new(Node)
	succ.Address = s_address
	succ.Id = GenHash(ln.config,s_address)
	if (ln.successors[2].Address!=s_address) {
		log.SetOutput(os.Stderr)
		log.Println("Successor 2 Updated: " + s_address)
		log.SetOutput(ln.logfile)			
		log.Println("Successor 2 Updated: " + s_address)		
	}
	ln.successors[2] = succ 

		// ln.mux.Unlock()
	return nil
}

func (ln *LocalNode) Schedule() {
	// Setup our stabilize timer
		// ln.mux.Lock()
	if !ln.shutdown {
		ln.timer = time.AfterFunc(randStabilize(ln.config), ln.Stabilize)
	}
		// ln.mux.Unlock()
}

func randStabilize(conf Config) time.Duration {
	min := conf.StabilizeMin
	max := conf.StabilizeMax
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

func between(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) == 1
	}

	// Handle the normal case
	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) == 1
}

// Checks if a key is between two ID's, right inclusive
func betweenRightIncl(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) >= 0
	}

	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) >= 0
}

	
func PrintMap(data map[string]string){
	for key,val := range data{
		log.Println("\tkey : " + key  + " value : " + val)
	}
}

func (ln *LocalNode) PrintAllMaps(){
	for i:=0;i<3;i++{
		log.SetOutput(os.Stderr)
		log.Println("Map number : " + strconv.Itoa(i))
		log.SetOutput(ln.logfile)			
		log.Println("Map number : " + strconv.Itoa(i))
		PrintMap(ln.data[i])
	}	
}

func CopyMap(target map[string]string, source map[string]string){

	for k := range target {
    	delete(target, k)
	}
	for k,v :=  range source{
		target[k] = v
	}
}

// func (ln *LocalNode) PrintSuccessorMaps(){
// 	if ln.successors[0].Address != ln.Address{
// 		log.Println("First Successor")
// 		ln.successors[0].PrintAllMaps()
// 	}
// 	if ln.successors[1].Address != ln.Address{
// 		log.Println("Second Successor")
// 		ln.successors[1].PrintAllMaps()
// 	} 
// 	if ln.successors[2].Address != ln.Address{
// 		log.Println("Third Successor")
// 		ln.successors[2].PrintAllMaps()
// 	} 
	
// }