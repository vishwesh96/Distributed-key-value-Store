package kvstore

import (
	"errors"
    "strconv"
    "log"
    "os"
    "container/list"
    "fmt"
)

type Hbeat struct{ 
	var Keys []string
	var ret_vals []string
	var args_vals []string
	var types []int
}
func Client_Transaction() error {

}
func Client_remoteRead(address string, key string, val *string) error {
	e,_ := remote_ReadKey(address,key,4,val)
	return e
}
func (ln *LocalNode) TransactionLeader(t transaction, val* string) error {
	var leaders []string = make([]string,len(t.Keys))
	var replies []Hbeat = make([]Hbeat,len(t.Keys))
	var e error
	temp_str:=new(str)
	for i:=range t.Keys {
		e=ln.FindSuccessor(t.Keys[i],temp_str)
		leaders[i]=*temp_str
	}
	for i:=range leaders {
		ln.Remote_Heartbeat(leaders[i],&replies[i]) // Prepare to commit
	}
	Hbeat_start := time.Now() //Start Timer
	for ((time.Since(Hbeat_start))*time.Second<ln.config.HeartBeatTime) {
	}
	for i:=range replies {
		if(replies[i]==nil) {
			//If even one value is nil, refrain to make changes, and abort
			return errors.New("All Nodes Busy")
		}
	}

	//Since everyone is ready go ahead with transaction
	for i:=range t.Keys {
		if(t.types[i]==0) {
				err,Async_Call:=remote_ReadKey(leaders[i],t.Keys[i],0,&t.ret_vals[i])
				if(err!=nil) {
					log.fatal("Unexepected Error in transaction")
				}
				<-Async_Call.Done
		} else {
			if(t.types[i]==1) {
				err := remote_WriteKey(leaders[i],t.Keys[i],t.args_vals[i],0)
				if(err!=nil) {
					log.fatal("Unexepected Error in transaction")
				}
			} else if (t.types[i]==2) {
				err:=remote_DeleteKey(leaders[i],t.Keys[i],0)
			}
		}		
	}
	*val=t.ret_vals[t.Keys-1]

}
func (ln * LocalNode) ReadKey(key string, val *string) error{
	leader := new(string)
	e := ln.FindSuccessor(key, leader)
	if e!=nil {
		return e
	}
	log.SetOutput(os.Stderr)
	log.Println("Found leader for key : "+key+" - " + *leader)
	log.SetOutput(ln.logfile)				
	log.Println("Found leader for key : "+key+" - " + *leader)
	if (*leader==ln.Address) {
		e := ln.ReadKeyLeader(key, val)
		return e
	}
	err,Async_Call:=remote_ReadKey(*leader,key,0,val)
	<-Async_Call.Done
	return err
}

func (ln *LocalNode) ReadKeyLeader(key string,val *string) error {
	//Trivial Load Balancing
	var to_read int
	to_read=(ln.Prev_read+1)%3
	ln.Prev_read = ln.Prev_read+1
	// ln.PrintAllMaps()
	log.SetOutput(os.Stderr)
	log.Println("Read Request for : "+ key + " at Node Address " + ln.Address)
	log.SetOutput(ln.logfile)				
	log.Println("Read Request for : "+ key + " at Node Address " + ln.Address)

	ln.Prev_read++
	if to_read==0 || ln.successors[0]==nil || ln.successors[0].Address==ln.Address{
		var ok bool
		*val, ok = ln.data[0][key]
		if ok == false{
			return errors.New("Key not present in the store")
		}
		log.SetOutput(os.Stderr)
		log.Println("Read key: "+ key + " value : " + *val +" Node Address " + ln.Address)
		log.SetOutput(ln.logfile)				
		log.Println("Read key: "+ key + " value : " + *val +" Node Address " + ln.Address)
	} else {
		if to_read==1 || ln.successors[1]==nil || ln.successors[1].Address==ln.Address {
			err,Async_Call:=remote_ReadKey(ln.successors[0].Address,key,1,val)
			<-Async_Call.Done
			return err
		} else {
			err,Async_Call:=remote_ReadKey(ln.successors[1].Address,key,2,val)
			<-Async_Call.Done
			return err
		}
	} 

	return nil
}

func (ln *LocalNode) ReadKeyReplica(key string, replica_num int, val *string) error{
	var ok bool
	*val, ok = ln.data[replica_num][key]
	// PrintMap(ln.data[replica_num])
	// ln.PrintAllMaps()
	if ok == false{
		return errors.New("Key not present in the store")
	}
	log.SetOutput(os.Stderr)
	log.Println("Read key: "+ key + " value : " + *val +" Node Address " + ln.Address)
	log.SetOutput(ln.logfile)				
	log.Println("Read key: "+ key + " value : " + *val+ " Node Address " + ln.Address)

	return nil
}	


func Client_remoteWrite(address string, key string, val string) error {
	e := remote_WriteKey(address,key,val,4)
	return e
}
	
func (ln *LocalNode) WriteKey(key string, val string) error{
	var leader string
	e := ln.FindSuccessor(key, &leader)
	if e!=nil {
		return e
	}
	log.SetOutput(os.Stderr)
	log.Println("Found leader for key : "+key+" - " + leader)
	log.SetOutput(ln.logfile)				
	log.Println("Found leader for key : "+key+" - " + leader)
	if (leader==ln.Address) {
		e = ln.WriteKeyLeader(key, val)
		return e
	}
	e = remote_WriteKey(leader,key,val,0)

	return e
}

func (ln * LocalNode) WriteKeyLeader(key string, val string) error{

	ln.data[0][key] = val
	log.SetOutput(os.Stderr)
	log.Println("Write key: "+ key + " value : " + val + " On Node Address " + ln.Address)
	log.SetOutput(ln.logfile)				
	log.Println("Write key: "+ key + " value : " + val + " On Node Address " + ln.Address)
	//check successor exists
	if (ln.successors[0]!=nil && ln.successors[0].Address!=ln.Address) {
		e0 := remote_WriteKey(ln.successors[0].Address,key,val,1)
		if e0!= nil {
			return e0
		}
	}
	
	if (ln.successors[1]!=nil && ln.successors[1].Address!=ln.Address) {
		e1 := remote_WriteKey(ln.successors[1].Address,key,val,2)
		if e1!= nil {
			return e1
		}
	}
	return nil
}

func (ln * LocalNode) WriteKeySuccessor(key string, val string, replica_number int) error{
	if len(ln.data) < replica_number {
		return errors.New("Not enough replicas")
	}
	ln.data[replica_number][key] = val
	log.SetOutput(os.Stderr)
	log.Println("Write key: "+ key + " value : " + val + " On Node Address " + ln.Address)
	log.SetOutput(ln.logfile)
	log.Println("Write key: "+ key + " value : " + val + " On Node Address " + ln.Address)
	return nil
}

func Client_remoteDelete(address string, key string) error {
	remote_DeleteKey(address,key,4)
	return nil
}
func (ln *LocalNode) DeleteKey(key string) error{
	var leader string
	e := ln.FindSuccessor(key, &leader)
	if e!=nil {
		return e
	}

	log.SetOutput(os.Stderr)
	log.Println("Found successor for key : "+key+" - " + leader)
	log.SetOutput(ln.logfile)
	log.Println("Found successor for key : "+key+" - " + leader)
	if (leader==ln.Address) {
		ln.DeleteKeyLeader(key)
		return nil
	}
	e = remote_DeleteKey(leader,key,0)
	return e
}

func (ln * LocalNode) DeleteKeyLeader(key string) error{
		_ , ok := ln.data[0][key]
		if ok == false{
			return errors.New("Key not present in Leader")
		}
		delete(ln.data[0],key)
		log.SetOutput(os.Stderr)
		log.Println("Delete key: "+ key)
		log.SetOutput(ln.logfile)
		log.Println("Delete key: "+ key)
		//check successor exists
		if (ln.successors[0]!=nil && ln.successors[0].Address!=ln.Address) {
			e0 := remote_DeleteKey(ln.successors[0].Address,key,1)
			if e0!= nil {
				return e0
			}
		}
		if (ln.successors[1]!=nil && ln.successors[1].Address!=ln.Address) {
			e1 := remote_DeleteKey(ln.successors[1].Address,key,2)
			if e1!= nil {
				return e1
			}
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
	log.SetOutput(os.Stderr)
	log.Println("Delete key: "+ key)
	log.SetOutput(ln.logfile)	
	log.Println("Delete key: "+ key)
	return nil
}
