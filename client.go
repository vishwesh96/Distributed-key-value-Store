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