package kvstore

import (
	"time"
	// "fmt"
)
type RPC_Join struct {
	Id []byte
	Replica_number int
}
type RPC_Leave struct {
	Pred_data map[string]string
	Replica_number int
}

type RPC_StabJoin struct {
	Data_pred [3]map[string]string
}
type RPC_RDKey struct {
	Key string
	Replica_number int
}
type RPC_WriteKey struct {
	Key string
	Val string
	Replica_number int
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
	StabilizeReplicasJoin_Stub(id []byte,ret_args *RPC_StabJoin) error 
	SendReplicasSuccessorJoin_Stub(args RPC_Join, emp_reply *struct{}) error 
	SendReplicasSuccessorLeave_Stub(args RPC_Leave, emp_reply *struct{}) error
	Heartbeat_Stub(rx_param hbeat, reply *hbeat) error
	ReadKey_Stub(args RPC_RDKey, val *string) error
	WriteKey_Stub(args RPC_WriteKey,emp_reply *struct{}) error
	DeleteKey_Stub(args RPC_RDKey, emp_reply *struct{}) error
}

func (ln *LocalNode) FindSuccessor_Stub(key string, reply *string) error {
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
func(ln *LocalNode) StabilizeReplicasJoin_Stub(id []byte, ret_args *RPC_StabJoin) error {
	err:= ln.StabilizeReplicasJoin(id,ret_args)
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
func(ln *LocalNode) ReadKey_Stub(args RPC_RDKey,val *string) error {
	if args.Replica_number==0 {
		err:=ln.ReadKeyLeader(args.Key,val)
		return err
	} else {
		err:=ln.ReadKeyReplica(args.Key,args.Replica_number,val)
		return err
	}
}
func(ln *LocalNode) WriteKey_Stub(args RPC_WriteKey,emp_reply *struct{}) error {
	if args.Replica_number==0 {
		err:=ln.WriteKeyLeader(args.Key,args.Val)
		return err
	} else {
		err:=ln.WriteKeySuccessor(args.Key,args.Val,args.Replica_number)
		return err
	}
}
func(ln *LocalNode) DeleteKey_Stub(args RPC_RDKey,emp_reply *struct{}) error {
	if args.Replica_number==0 {
		err:=ln.DeleteKeyLeader(args.Key)
		return err
	} else {
		err:=ln.DeleteKeySuccessor(args.Key,args.Replica_number)
		return err
	}
}