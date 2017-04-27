package kvstore

import (
	"errors"
    "strconv"
    "log"
    "os"
    "fmt"
)

func Client_Transaction(address string, val *string) error {

	fmt.Println("Enter 1 for Write,2 for Read, 3 for Delete and 4 to exit")
    var choice int 
    fmt.Scanln(&choice)
    var t Transaction
    t.init()
    for choice!=4 {
        switch choice {
            case 1:
    		fmt.Println("Enter Key to be written against")
    		var key string
    		var value string        
            fmt.Scanln(&key)
            fmt.Println("Enter value to be written")
            fmt.Scanln(&value)
            t.Types=append(t.Types,1)
            t.Keys=append(t.Keys,key)     
            t.Args_vals=append(t.Args_vals,value)
            t.Ret_vals=append(t.Ret_vals,"No return var")
            case 2:
    		fmt.Println("Enter Key to be read")
    		var key string
            fmt.Scanln(&key)
            t.Types=append(t.Types,0)
            t.Keys=append(t.Keys,key)
            t.Args_vals=append(t.Args_vals,"No args")
            t.Ret_vals=append(t.Ret_vals,"Return Var")
            case 3:
    		fmt.Println("Enter Key to be deleted")
    		var key string
            fmt.Scanln(&key)
            t.Types=append(t.Types,2)
            t.Keys=append(t.Keys,key)
          	t.Args_vals=append(t.Args_vals,"No args")
            t.Ret_vals=append(t.Ret_vals,"No return var")
          
            default:
            fmt.Println("Enter a valid Choice")
        }
       fmt.Println("Enter 1 for Write,2 for Read, 3 for Delete and 4 to exit")
       fmt.Scanln(&choice)
    
    }
   err :=remote_Transaction(address, t, val)
   return err
}

func Client_remoteRead(address string, key string, val *string) error {
	e,_ := remote_ReadKey(address,key,4,val)
	return e
}
func (ln *LocalNode) TransactionLeader(t Transaction, val *string) error {
	var leaders []string = make([]string,0)
	var all_leaders []string =make([]string,len(t.Keys))
	temp_str:=new(string)
	var e error
	var repeat bool
	for i:=range t.Keys {
		e=ln.FindSuccessor(t.Keys[i],temp_str)
		if(e!=nil) {
			log.Fatal("Unexepected Errors in Transaction when looking out for leaders")
		}
		for k:=range leaders {
			if(leaders[k]==*temp_str) {
				repeat=true
				break
			}
		}
		if((!repeat)&&(*temp_str!=ln.Address)&&(t.Types[i]!=0)) {
			leaders=append(leaders,*temp_str)
		}
		all_leaders[i]=*temp_str
	}
	var replies [](string) = make([](string),len(leaders))
	fmt.Println("Initiating Replies to ",len(leaders) ," leaders")
	for i:=range leaders {
		remote_BusyNode(leaders[i],&replies[i]) // Prepare to commit
	}
	//Hbeat_start := time.Now() //Start Timer
	//for ((time.Since(Hbeat_start))*time.Second<100*ln.config.HeartBeatTime) {
	//}
	for i:=range replies {
		if(replies[i]=="true") {
			//If even one value is nil, refrain to make changes, and abort
			fmt.Println("Initiating Freeing up replies to the ",len(leaders) ," leaders blocked by the client")
			for i:=range leaders {
				remote_FreeNode(leaders[i]) // Free up the resources reserved
			}
			return errors.New("Some Nodes Busy, Aborting...")
		}
	}

	//Since everyone is ready go ahead with Transaction
	for i:=range t.Keys {
		if(t.Types[i]==0) {	
				err,Async_Call:=remote_ReadKey(all_leaders[i],t.Keys[i],0,&(t.Ret_vals[i]))
				fmt.Println("Read Result :",t.Ret_vals[i])
				if(err!=nil) {
					fmt.Println("Initiating Freeing up replies to the ",len(leaders) ," leaders blocked by the client")
					for i:=range leaders {
						remote_FreeNode(leaders[i]) // Free up the resources reserved
					}
					log.Fatal("Unexepected Error in Transaction")
				}
				<-Async_Call.Done
		} else {
			if(t.Types[i]==1) {
				err := remote_WriteKey(all_leaders[i],t.Keys[i],t.Args_vals[i],0)
				if(err!=nil) {
					fmt.Println("Initiating Freeing up replies to the ",len(leaders) ," leaders blocked by the client")
					for i:=range leaders {
						remote_FreeNode(leaders[i]) // Free up the resources reserved
					}
					log.Fatal("Unexepected Error in Transaction")
				}
			} else if (t.Types[i]==2) {
				err:=remote_DeleteKey(all_leaders[i],t.Keys[i],0)
				if(err!=nil) {
					fmt.Println("Initiating Freeing up replies to the ",len(leaders) ," leaders blocked by the client")
					for i:=range leaders {
						remote_FreeNode(leaders[i]) // Free up the resources reserved
					}
					log.Fatal("Unexepected Error in Transaction")
				}
			}
		}		
	}
	fmt.Println("Initiating Freeing up replies to the ",len(leaders) ," leaders blocked by the client")
	for i:=range leaders {
		remote_FreeNode(leaders[i]) // Free up the resources reserved
	}
	*val=t.Ret_vals[len(t.Keys)-1]
	return nil
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
