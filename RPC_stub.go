package kvstore

import (
	"time"
	"fmt"
)
type RPC_Join struct {
	Id []byte
	Replica_number int
}
type RPC_Leave struct {
	Pred_data map[string]string
	Replica_number int
}
type RPC_RDKey struct {
	key string
	replica_number int
}
type RPC_WriteKey struct {
	key string
	val string
	replica_number int
}
type hbeat struct{ 
	Rx_time time.Time
	Node_info Node
}
type Node_RPC interface{
	FindSuccessor_Stub(key string, reply *string) error
	GetPredecessor_Stub(emp_arg struct{}, reply *string) error
	Notify_Stub(message string, emp_reply *struct{}) error
	Ping_Stub(emp_arg struct{},emp_reply *struct{}) error
	StabilizeReplicasJoin_Stub(id []byte, data_pred *[]map[string]string) error 
	SendReplicasSuccessorJoin_Stub(args RPC_Join, emp_reply *struct{}) error 
	SendReplicasSuccessorLeave_Stub(args RPC_Leave, emp_reply *struct{}) error
	Heartbeat_Stub(rx_param hbeat, reply *hbeat) error
	ReadKey_stub(args RPC_RDKey, val *string) error
	WriteKEy_Stub(args RPC_WriteKey,emp_reply *struct{})
}

func (ln *LocalNode) FindSuccessor_Stub(key string, reply *string) error {
	fmt.Println("lol")
	err := ln.FindSuccessor(key,reply)
	return err
}
func (ln *LocalNode) GetPredecessor_Stub(emp_arg struct{}, reply *string) error {
	err := ln.GetPredecessor(reply)
	return err
}
func (ln *LocalNode) GetSuccessor_Stub(emp_arg struct{}, reply *string) error {
	err := ln.GetSuccessor(reply)
	return err
}
func (ln *LocalNode) Notify_Stub(message string, emp_reply *struct{}) error {
	err := ln.Notify(message)
	return err
}
func (ln *LocalNode) Ping_Stub(emp_arg struct{},emp_reply *struct{}) error {
	err:=ln.Ping()
	return err
}
func(ln *LocalNode) StabilizeReplicasJoin_Stub(id []byte, data_pred *[]map[string]string) error {
	err:= ln.StabilizeReplicasJoin(id,data_pred)
	return err
} 
func(ln *LocalNode)	SendReplicasSuccessorJoin_Stub(args RPC_Join, emp_reply *struct{}) error {
	err:= ln.SendReplicasSuccessorJoin(args.Id,args.Replica_number)
	return err
}
func(ln *LocalNode)	SendReplicasSuccessorLeave_Stub(args RPC_Leave, emp_reply *struct{}) error {
	err:= ln.SendReplicasSuccessorLeave(args.Pred_data,args.Replica_number)
	return err	
}
func(ln *LocalNode) Heartbeat_Stub(rx_param hbeat, reply *hbeat) error {
	err:=ln.Heartbeat(rx_param, reply)
	return err
}
func(ln *LocalNode) ReadKey_stub(args RPC_RDKey,val *string) error {
	if args.replica_number=0 {
		err:=ln.ReadKeyLeader(args.key,val)
	} else {
		err:=ln.ReadKeyReplica(args.key,args.replica_number,val)
	}
	return err
}
func(ln *LocalNode) WriteKey_stub(args RPC_WriteKey,emp_reply *struct{}) error {
	if args.replica_number=0 {
		err:=ln.WriteKeyLeader(args.key,args.val)
	} else {
		err:=ln.WriteKeySuccessor(args.key,args.replica_number,args.val)
	}
	return err
}
func(ln *LocalNode) DeleteKey_stub(args RPC_RDKey,emp_reply *struct{}) error {
	if args.replica_number=0 {
		err:=ln.DeleteKeyLeader(args.key)
	} else {
		err:=ln.DeleteKeyReplica(args.key,args.replica_number)
	}
	return err
}