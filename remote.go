package kvstore

import (
	"log"
	"net/rpc"
	"time"
    // "fmt"
)

// Remote Function Calls
func (ln *LocalNode) remote_FindSuccessor (address string, key string, reply *string) error {
    log.Println("Call")
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_FindSuccessor:", err)
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
        log.Println("dialing error in remote_GetPredecessor:", err)
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
        log.Println("dialing error in remote_GetSuccessor:", err)
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
        log.Println("dialing error in remote_Notify:", err)
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
        log.Println("dialing error in remote_Ping:", err)
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

func (ln *LocalNode) remote_SkipSuccessor (address string) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_SkipSuccessor:", err)
        return err
    }
    emp_reply:=new(struct{})
    emp_args:=new(struct{})
    err = t.Call("Node_RPC.SkipSuccessor_Stub",emp_args,&emp_reply)
    if err != nil {
        log.Println("sync Call error in remote_SkipSuccessor:", err) 
        return err
    }
    return nil  
}
func (ln *LocalNode) remote_StabilizeReplicasJoin(address string, id []byte, ret_args *RPC_StabJoin) error {
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_StabilizeReplicasJoin:", err)
        return err
    }
    err = t.Call("Node_RPC.StabilizeReplicasJoin_Stub",id,ret_args)
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
        log.Println("dialing error in remote_SendReplicasSuccessorJoin:", err)
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
        log.Println("dialing error in remote_SendReplicasSuccessorLeave:", err)
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

func(ln *LocalNode) Remote_Heartbeat(address string, reply *Hbeat ) error {
	var complete_address = address
	t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_Heartbeat:", err)
        return err
    }
    var args Hbeat
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

func(ln *LocalNode) remote_GetRemoteData(address string, replica_number int,data_reply *map[string]string) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Fatal("dialing error in remote_GetRemoteData:", err)
        return err
    }
    err = t.Call("Node_RPC.GetRemoteData_Stub",replica_number,data_reply)
    if err != nil {
        log.Println("sync Call error in remote_GetRemoteData:", err) 
        return err
    }
    return nil     
}

func remote_ReadKey(address string,key string,replica_number int,val *string) (error, *rpc.Call) {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_ReadKey:", err)
        return err,nil
    }
    var args RPC_RDKey
    args.Key=key
    args.Replica_number=replica_number
    var Async_Call *rpc.Call
    if(replica_number!=4){
        Async_Call = t.Go("Node_RPC.ReadKey_Stub",args,val,nil)
        err=Async_Call.Error
    } else {
        err = t.Call("Node_RPC.ReadKey_Stub",args,val)     
        Async_Call = nil
    }
    if err != nil {
        log.Println("sync Call error in remote_ReadKey:", err) 
        return err,nil
    }
    return nil,Async_Call     
}

func remote_WriteKey(address string,key string,val string,replica_number int) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_WriteKey:", err)
        return err
    }
    var args RPC_WriteKey
    args.Key=key
    args.Replica_number=replica_number
    args.Val=val
    emp_reply := new(struct{})
    err = t.Call("Node_RPC.WriteKey_Stub",args,emp_reply)
    if err != nil {
        log.Println("sync Call error in remote_WriteKey:", err) 
        return err
    }
    return nil     
}

func remote_DeleteKey(address string,key string,replica_number int) error {
    var complete_address = address
    t, err := rpc.DialHTTP("tcp", complete_address)
    if err != nil {
        log.Println("dialing error in remote_DeleteKey:", err)
        return err
    }
    var args RPC_RDKey
    args.Key=key
    args.Replica_number=replica_number
    emp_reply := new(struct{})
    err = t.Call("Node_RPC.DeleteKey_Stub",args,emp_reply)
    if err != nil {
        log.Println("sync Call error in remote_DeleteKey:", err) 
        return err
    }
    return nil     
}
