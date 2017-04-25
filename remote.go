package kvstore

import (
	"log"
	"net/rpc"
	"time"
)

// Remote Function Calls
func (ln *LocalNode) remote_FindSuccessor (address string, key string, reply *string) error {
    log.Println("Call")
	var complete_address = address
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
	var complete_address = address
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
	var complete_address = address
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
	var complete_address = address
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
	var complete_address = address
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
func (ln *LocalNode) remote_StabilizeReplicasJoin(address string, id []byte, data_pred *[]map[string]string) error {
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_StabilizeReplicasJoin:", err)
        return err
    }
    err = t.Call("Node_RPC.StabilizeReplicasJoin_Stub",id,data_pred)
	if err != nil {
    	log.Println("sync Call error in remote_StabilizeReplicasJoin:", err) 
    	return err
	}
	return nil			
}

func (ln *LocalNode) remote_SendReplicasSuccessorJoin(address string, id []byte,replica_number int) error {
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_SendReplicasSuccessorJoin:", err)
        return err
    }
    emp_reply:=new(struct{})
    var args RPC_Join
    args.Id=id
    args.Replica_number=replica_number
    err = t.Call("Node_RPC.SendReplicasSuccessorJoin_Stub",args,emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_SendReplicasSuccessorJoin:", err) 
    	return err
	}
	return nil	
}
func (ln *LocalNode) remote_SendReplicasSuccessorLeave(address string, pred_data map[string]string,replica_number int) error{
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_SendReplicasSuccessorLeave:", err)
        return err
    }
    emp_reply:=new(struct{})
    var args RPC_Leave
    args.Pred_data=pred_data
    args.Replica_number=replica_number
    err = t.Call("Node_RPC.SendReplicasSuccessorLeave_Stub",args,emp_reply)
	if err != nil {
    	log.Println("sync Call error in remote_SendReplicasSuccessorLeave:", err) 
    	return err
	}
	return nil	
}

func(ln *LocalNode) remote_Heartbeat(address string, rx_param hbeat, reply *hbeat ) error {
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_Heartbeat:", err)
        return err
    }
    var args hbeat
    args.Node_info=ln.Node
    args.Rx_time=time.Now()
    Async_Call := t.Go("Node_RPC.Heartbeat_Stub",args,reply,nil)
	err=Async_Call.Error
	if err != nil {
    	log.Println("sync Call error in remote_Heartbeat:", err) 
    	return err
	}
	return nil	
}

func(ln *LocalNode) remote_ReadKey(address string,key string,replica_number int,val *string) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_ReadKey:", err)
        return err
    }
    var args RPC_RDKey
    args.key=key
    args.replica_number=replica_number
    Async_Call := t.Go("Node_RPC.ReadKey_stub",args,reply,nil)
    err=Async_Call.Error
    if err != nil {
        log.Println("sync Call error in remote_ReadKey:", err) 
        return err
    }
    return nil     
}

func(ln *LocalNode) remote_WriteKey(address string,key string,val string,replica_number int) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_WriteKey:", err)
        return err
    }
    var args RPC_WriteKey
    args.key=key
    args.replica_number=replica_number
    args.val=val
    emp_reply := *struct{}
    err = t.Call("Node_RPC.WriteKey_stub",args,emp_reply)
    err=Async_Call.Error
    if err != nil {
        log.Println("sync Call error in remote_WriteKey:", err) 
        return err
    }
    return nil     
}

func(ln *LocalNode) remote_DeleteKey(address string,key string,replica_number int) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_DeleteKey:", err)
        return err
    }
    var args RPC_RDKey
    args.key=key
    args.replica_number=replica_number
    emp_reply := struct{}
    err = t.Call("Node_RPC.DeleteKey_stub",args,emp_reply)
    if err != nil {
        log.Println("sync Call error in remote_DeleteKey:", err) 
        return err
    }
    return nil     
}
