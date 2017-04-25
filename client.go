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
	"math/rand"
)


func (ln * LocalNode) ReadKey(key string, val *string) error{
	var leader string
	e := ln.FindSuccessor(key, leader *string)
	if e!=nil {
		return e
	}
	ln.remote_ReadKey(leader,key,0,val)

}

func (ln *LocalNode) ReadKeyLeader(key string,val *string){
	//Trivial Load Balancing
	var to_read int
	if ln.prev_read==2 {
		to_read=0
	} else {
		to_read=ln.prev_read+1
	} 

	if to_read==0 {
		*val=ln.data[0][key]
	} else {
		if to_read==1 {
			remote_ReadKey(ln.successors[0].address,key,1,val)
		} else {
			remote_ReadKey(ln.successor[1].address,key,2,val)
		}
	} 

}

func (ln *LocalNode) ReadKeyReplica(key string, replica_num int, val *string){
	*val=ln.data[replica_num][key]
}	


	
func (ln *LocalNode) WriteKey(key string, val string) error{
	var leader string
	e := ln.FindSuccessor(key, &leader)
	if e!=nil {
		return e
	}
	e = ln.remote_WriteKey(leader,key,val,0)

	return e
}

func (ln * LocalNode) WriteKeyLeader(key string, val string) error{
	ln.data[0][key] = val
	//check successor exists
	e0 := ln.remote_WriteKey(ln.successors[0].Address,key,val,1)
	if e0!= nil {
		return e0
	}
	e1 := ln.remote_WriteKey(ln.successors[1].Address,key,val,2)
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
	e = ln.remote_DeleteKey(leader,key,0)
	return e
}

func (ln * LocalNode) DeleteKeyLeader(key string) error{
		_ , ok := ln.data[0][key]
		if ok == false{
			return errors.New("Key not present in Leader")
		}
		delete(ln.data[0],key)
		//check successor exists
		e0 := ln.remote_DeleteKey(ln.successors[0].Address,key,1)
		if e0!= nil {
			return e0
		}
		e1 := ln.remote_DeleteKey(ln.successors[1].Address,key,2)
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
