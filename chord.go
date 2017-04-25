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

func (ln *LocalNode) Init(config Config) {
	// Generate an Id
	ln.config = config
	ln.Id = GenHash(ln.config, ln.Address)
	// Initialize all state
	ln.successors = make([]*Node, ln.config.NumSuccessors)
	ln.finger = make([]*Node, ln.config.hashBits)

	// // Register with the RPC mechanism
	// ln.ring.transport.Register(&ln.Node, ln)
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
	StabilizeReplicasJoin_Stub(s_address,ln.Address,ln.Id)			//call StabilizeReplicasJoin and set ln.Address as predecessor of s_address
}

//RPC
func (ln *LocalNode) FindSuccessor(key string) (string, error){
	id_hash := GenHash(ln.config, key)
	my_hash := ln.Id
	succ_hash := successors[0].Id
	if (bytes.compare(id_hash,my_hash)>0 && bytes.compare(id_hash,succ_hash)<=0) {
		return (successors[0].address, nil)
	}
	s_address, e := FindSuccessor_Stub(successors[0].address, key)
	return (s_address, e)
}

//RPC
func (ln *LocalNode) GetPredecessor() (string, error) {
	return predecessor.Address
}

//RPC
func (ln *LocalNode) Notify(address string) (error) {
	return nil
}

//RPC
func (ln *LocalNode) Ping(address string) (error) {
	return nil
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

func (ln *LocalNode) StabilizeReplicasJoin(id []byte) error {
	if len(data) != 3 {
		return errors.New("Doesn't have 3 replicas")
	}
	new_map = SplitMap(data[0],id)
	SendReplicasPredecessor_Stub(predecessor,new_map,data[1],data[2])		//send data[1] and data[2] to predecessor
	data[2] = data[1]
	data[1] = new_map	
	SendReplicasSuccessor_Stub(successors[0],id,1)			
	SendReplicasSuccessor_Stub(successors[1],id,2)			
}

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

func (ln *LocalNode) SendReplicasPredecessor(data0 map[string]string, data1 map[string]string, data2 map[string]string){
	data[0] = data0
	data[1] = data1
	data[2] = data2
}		