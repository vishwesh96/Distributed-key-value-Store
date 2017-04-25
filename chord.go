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

}
func (ln *LocalNode) remote_GetPredecessor (address string, reply *string) error {
	
}
func (ln *LocalNode) remote_GetPredecessor (address string, reply *string) error {
	
}
func (ln *LocalNode) remote_Notify (address string, message string) error {

}
func (ln *LocalNode) remote_Ping (address string) error {

}

func (ln *LocalNode) check_predecessor() {
	err := Ping_Stub(predecessor.address)
	if (err!=nil) {
		predecessor = nil
	}

}

func (ln *LocalNode) stabilize() {
	
}