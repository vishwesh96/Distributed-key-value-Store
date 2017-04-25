package kvstore

import (
	"time"
)
type RPC_Join struct {
	Id []byte
	Replica_number int
}
type RPC_Leave struct {
	Pred_data map[string]string
	Replica_number int
}
type hbeat struct{ 
	Rx_time time.Time
	Node_info Node
}
type Node_RPC interface{
	FindSuccessor_stub(key string, reply *string) error
	GetPredecessor_stub(emp_arg struct{}, reply *string) error
	Notify_stub(message string, emp_reply *struct{}) error
	Ping_stub(emp_arg struct{},emp_reply *struct{}) error
	StabilizeReplicasJoin_stub(id []byte, data_pred []map[string]string) error 
	SendReplicasSuccessorJoin_stub(args RPC_Join, emp_reply *struct{}) error 
	SendReplicasSuccessorLeave_stub(args RPC_Leave, emp_reply *struct{}) error
	Heartbeat_stub(rx_param hbeat, reply *hbeat) error
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
	err:= ln.SendReplicasSuccessorJoin(args.Id,args.Replica_number)
	return err
}
func(ln *LocalNode)	SendReplicasSuccessorLeave_stub(args RPC_Leave, emp_reply *struct{}) error {
	err:= ln.SendReplicasSuccessorLeave(args.Pred_data,args.Replica_number)
	return err	
}
func(ln *LocalNode) Heartbeat_stub(rx_param hbeat, reply *hbeat) error {
	err:=ln.Heartbeat(rx_param, reply)
	return err
}
