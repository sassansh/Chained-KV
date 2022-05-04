package chainedkv

import (
	"cs.ubc.ca/cpsc416/a3/kvsshared"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"hash/fnv"
	"log"
	"math"
	"net/rpc"
	"time"
)

type ServerRPC Server

type NewServerFromFailure struct {
	ServerID uint8
	Success  bool
}

type DummyStruct struct {
	Dummy bool
}

// func send put to next server
// 1. grab the linked server object
// 2. var err =
// 2. for loop begin, end when err == nil
//    1. nextServer.send
//

func (s *ServerRPC) sendPutToNextServer(request kvsshared.KVSMessage, response *kvsshared.KVSMessage) {
	nextServer := s.nextServer
	err := errors.New("placeholder")
	for err != nil && s.nextServer.ServerID != 0 {
		err = nextServer.conn.Call("ServerRPC.PutFWDRPC", request, response)
		time.Sleep(time.Millisecond * 100)
	}
}

//===================================================================================================================
//Server Join
func (s *ServerRPC) JoinNewNextRPC(request NextServerJoined, response *NextServerJoinedResponse) error {
	s.trace = s.strace.ReceiveToken(request.Token)
	s.trace.RecordAction(NextServerJoining{request.NextServer.ServerID})

	s.nextServer = request.NextServer
	conn, err := rpc.Dial("tcp", s.nextServer.Addr)
	if err != nil {
		response.Success = false
		return err
	}
	s.nextServer.conn = conn

	s.trace.RecordAction(NewJoinedSuccessor{request.NextServer.ServerID})
	response.Token = s.trace.GenerateToken()
	response.Success = true

	return nil
}

//End Server Join
//===================================================================================================================

//Put as head
func (s *ServerRPC) Put(request kvsshared.KVSMessage, response *kvsshared.KVSMessage) error {
	log.Println("Receiving a put from", request.ClientId, "for [OpID:", request.OpId, "] Put(", request.Key,
		",", request.Value, ")")
	// Assume you are head since getting Put called

	//1. Receive
	// Read trace token from args and trace PutRecvd
	trace := s.strace.ReceiveToken(request.Token)
	trace.RecordAction(PutRecvd{
		ClientId: request.ClientId,
		OpId:     request.OpId,
		Key:      request.Key,
		Value:    request.Value,
	})

	//TODO as tail
	// 1. wait for opID to be ordered
	// 2. assign gID
	// 3. Once ordered, store put in valmap
	// 4. respond to client
	if s.nextServer.ServerID == 0 {
		//1. wait for opID to be ordered
		s.waitPrevOpID(request)

		s.updateClientOpID(request.ClientId, request.OpId)

		//2. assign gID
		gid := s.incrGIDPutAndReturn()
		request.GId = gid

		log.Println("Assigned gId:", gid, "to [OpID:", request.OpId, "] Put(", request.Key, ",", request.Value, ")")
		trace.RecordAction(PutOrdered{
			ClientId: request.ClientId,
			OpId:     request.OpId,
			GId:      gid,
			Key:      request.Key,
			Value:    request.Value,
		})

		//3. store put
		// Storing it locally
		log.Println("Storing value", request.Value, "for key", request.Key, "in my valmap")
		storedRequest := getServerMessage(request, trace)
		s.handlePutAsTail(storedRequest, trace)
		s.data.valmap[request.Key] = request.Value
		s.syn.kvsGroup.mutex.Lock()
		s.data.clientKVSMap[s.hashKVSMessage(request)] = storedRequest
		s.syn.kvsGroup.mutex.Unlock()

		//4. Respond to client?

		// This happens in getPutResult
		response.OpId = request.OpId
		response.ClientId = request.ClientId
		response.Value = request.Value
		response.Key = request.Key
		response.GId = gid

		s.notifyNextOpID(request)

		//handle as tail
	} else {
		// Process the put as if you are the HEAD

		// 1. Wait (if need be) & Order
		s.syn.opIDGroup.mutex.Lock()
		if request.OpId != 0 {
			s.waitForOrderedOpIDPrevPut(request)
		}
		s.updateClientOpIDNoLock(request.ClientId, request.OpId)

		gid := s.incrGIDPutAndReturn()
		request.GId = gid
		s.syn.opIDGroup.mutex.Unlock()

		trace.RecordAction(PutOrdered{
			ClientId: request.ClientId,
			OpId:     request.OpId,
			GId:      gid,
			Key:      request.Key,
			Value:    request.Value,
		})

		// 2. Store
		log.Println("Storing value", request.Value, "for key", request.Key, "in my valmap")
		s.data.valmap[request.Key] = request.Value
		s.syn.kvsGroup.mutex.Lock()
		s.data.clientKVSMap[s.hashKVSMessage(request)] = getServerMessage(request, trace)
		s.syn.kvsGroup.mutex.Unlock()

		// 3. Send
		trace.RecordAction(PutFwd{
			ClientId: request.ClientId,
			OpId:     request.OpId,
			GId:      gid,
			Key:      request.Key,
			Value:    request.Value,
		})
		request.Token = trace.GenerateToken()
		//TODO: process async
		log.Println("Passing on gId:", gid, " [OpID:", request.OpId, "] Put(", request.Key, ",",
			request.Value, ")", " to the next server:", s.nextServer.ServerID)
		s.sendPutToNextServer(request, response)

		// 4. Ack
		// Above call is technically synchronous, so we should have ack?

		// 5. Respond to client
		response.OpId = request.OpId
		response.ClientId = request.ClientId
		response.Value = request.Value
		response.Key = request.Key
		response.GId = gid
	}
	return nil
}

//===================================================================================================================
//Put Operations
func (s *ServerRPC) PutFWDRPC(request kvsshared.KVSMessage, response *kvsshared.KVSMessage) error {

	// If already have message, just return
	s.syn.kvsGroup.mutex.Lock()
	if _, ok := s.data.clientKVSMap[s.hashKVSMessage(request)]; ok {
		log.Println("Already have message for [Client:", request.ClientId, "OpID:", request.OpId,
			"] Not doing anything, returning")
		s.syn.kvsGroup.mutex.Unlock()
		return nil
	}
	s.syn.kvsGroup.mutex.Unlock()

	// Trace PutFwdRecvd
	trace := s.strace.ReceiveToken(request.Token)
	trace.RecordAction(PutFwdRecvd{
		ClientId: request.ClientId,
		OpId:     request.OpId,
		GId:      request.GId,
		Key:      request.Key,
		Value:    request.Value,
	})
	log.Println("Next server#", s.config.ServerId, "Received put request", request)

	if s.nextServer.ServerID == 0 {
		//1. wait for opID to be ordered

		s.waitPrevOpID(request)

		s.updateClientOpID(request.ClientId, request.OpId)

		//2. assign gID
		gid := s.incrGIDPutAndReturn()
		request.GId = gid

		log.Println("Assigned gId:", gid, "to [OpID:", request.OpId, "] Put(", request.Key, ",", request.Value, ")")

		// 3. Handle put as tail (mostly just trace)
		storedRequest := getServerMessage(request, trace)
		s.handlePutAsTail(storedRequest, trace)

		//4. store put
		// Storing it locally
		log.Println("Storing value", request.Value, "for key", request.Key, "in my valmap")

		s.data.valmap[request.Key] = request.Value
		//request.Token = trace.GenerateToken()
		s.syn.kvsGroup.mutex.Lock()
		s.data.clientKVSMap[s.hashKVSMessage(request)] = storedRequest
		s.syn.kvsGroup.mutex.Unlock()

		//5. Respond to client
		// This happens in getPutResult
		response.OpId = request.OpId
		response.ClientId = request.ClientId
		response.Value = request.Value
		response.Key = request.Key
		response.GId = gid

		s.notifyNextOpID(request)

		//handle as tail
	} else {
		// Process the put as if you are the MIDDLE

		// 1. Wait (if need be) & Order
		s.syn.opIDGroup.mutex.Lock()
		if request.OpId != 0 {
			s.waitForOrderedOpIDPrevPut(request)
		}
		s.updateClientOpIDNoLock(request.ClientId, request.OpId)

		gid := s.incrGIDPutAndReturn()
		request.GId = gid
		s.syn.opIDGroup.mutex.Unlock()

		//trace.RecordAction(PutOrdered{
		//	ClientId: request.ClientId,
		//	OpId:     request.OpId,
		//	GId:      gid,
		//	Key:      request.Key,
		//	Value:    request.Value,
		//})

		// 2. Store
		log.Println("Storing value", request.Value, "for key", request.Key, "in my valmap")
		s.data.valmap[request.Key] = request.Value
		s.syn.kvsGroup.mutex.Lock()
		s.data.clientKVSMap[s.hashKVSMessage(request)] = getServerMessage(request, trace)
		s.syn.kvsGroup.mutex.Unlock()

		// 3. Send
		trace.RecordAction(PutFwd{
			ClientId: request.ClientId,
			OpId:     request.OpId,
			GId:      gid,
			Key:      request.Key,
			Value:    request.Value,
		})
		request.Token = trace.GenerateToken()
		//TODO: process async
		log.Println("Passing on gId:", gid, " [OpID:", request.OpId, "] Put(", request.Key, ",",
			request.Value, ")", " to the next server:", s.nextServer.ServerID)
		go s.sendPutToNextServer(request, &kvsshared.KVSMessage{})

		// 4. Ack
		// Above call is technically synchronous, so we should have ack?

		// 5. Respond to client
		response.OpId = request.OpId
		response.ClientId = request.ClientId
		response.Value = request.Value
		response.Key = request.Key
		response.GId = gid
	}
	return nil
}

func (s *ServerRPC) notifyNextOpID(request kvsshared.KVSMessage) {
	s.syn.clientChanMutex.Lock()
	defer s.syn.clientChanMutex.Unlock()
	ret, found := s.data.clientChanMap[s.hashKVSMessage(request)]
	if !found {
		//Not in map, create (you've arrived before your previous msg has)
		ret = make(chan bool, 2)
		s.data.clientChanMap[s.hashKVSMessage(request)] = ret
	}
	// sent msg to next non-blocking
	s.syn.kvsGroup.cond.Broadcast()
	ret <- true
}

func getServerMessage(request kvsshared.KVSMessage, trace *tracing.Trace) ServerMessage {
	storedRequest := ServerMessage{
		ClientId:            request.ClientId,
		GId:                 request.GId,
		OpId:                request.OpId,
		Key:                 request.Key,
		Value:               request.Value,
		LatestSentPutId:     request.LatestSentPutId,
		LatestSentGetId:     request.LatestSentGetId,
		LatestReceivedPutId: request.LatestReceivedPutId,
		LatestReceivedGetId: request.LatestReceivedGetId,
		Token:               request.Token,
		Trace:               trace,
	}
	return storedRequest
}

// Cases
// 1. Req.opID = 0 -> first put from this client
// -- You know there is nothing in the map
// 2. Req.opID > 0, prevPutID = 0 -> 2nd put from this client
// -- You know the map should contain 0
// 3. Req. opID > 0, prePutId > 0

func (s *ServerRPC) putReqOpIDPrevMatches(clientID string, prevOpID uint32) bool {
	val, err := s.getClientOpIDNoLock(clientID)
	if err != nil {
		log.Println("LatestSentPutId from client", clientID, "not received yet. no previous opID "+
			"LatestSentPutId:", prevOpID, "Waiting...")
		return false
	}
	log.Println("Client", clientID, "last OpID:", val)
	return val == prevOpID
}

func (s *ServerRPC) waitForOrderedOpIDPrevPut(request kvsshared.KVSMessage) {
	for !s.putReqOpIDPrevMatches(request.ClientId, request.LatestSentPutId) {
		// We don't have the last Put OpID, waiting...
		log.Println("Client", request.OpId, "last OpID:", request.OpId)

		log.Println("LatestSentPutId from client", request.ClientId, "not received yet."+
			" LatestSentPutId:", request.LatestSentPutId, "Waiting...")

		s.syn.opIDGroup.cond.Wait()
	}
	// We have the last Put OpID, lets go...
	log.Println("LatestSentPutId from client", request.ClientId, "already received. "+
		". LatestSentPutId:", request.LatestSentPutId, "Good to go!")
}

func (s *ServerRPC) hashKVSMessage(message kvsshared.KVSMessage) uint64 {
	h := fnv.New64a()
	msg := fmt.Sprint(message.OpId, message.ClientId)
	h.Write([]byte(msg))
	return h.Sum64()
}

//End Put Operations
//===================================================================================================================

//===================================================================================================================
//Tail Server Operations

func (s *ServerRPC) SyncLatestOpID(request *kvsshared.LatestOpID, _ *kvsshared.LatestOpID) error {
	log.Println("I'm a new tail! Client", request.ClientId, "is telling me their latest OpID is",
		request.OpId)

	// Update client's latest OpID
	s.updateClientOpID(request.ClientId, request.OpId)

	return nil
}

//Tail server Get operations ------------------------------------------------
func (s *ServerRPC) Get(request kvsshared.KVSMessage, response *kvsshared.KVSMessage) error {
	//go s.startRun()
	log.Println("Client", request.ClientId, "is asking for the Value of Key: ", request.Key, ", opID:", request.OpId)
	val, err := s.getClientOpID(request.ClientId)
	if err != nil {
		log.Println("No processed OPIDs for this client")
	} else {
		log.Println("Current OPID:", val)
	}

	// Read trace token from args and trace GetRecvd
	trace := s.strace.ReceiveToken(request.Token)
	trace.RecordAction(GetRecvd{
		ClientId: request.ClientId,
		OpId:     request.OpId,
		Key:      request.Key,
	})

	s.waitPrevOpID(request)

	fmt.Println("client", request.ClientId, " opID", request.OpId, "Good to go!!!!!!")

	s.updateClientOpID(request.ClientId, request.OpId)

	// Increase gid by 1 and Order the Get, then trace it
	gid := s.incrGIDGetAndReturn()
	response.GId = gid

	trace.RecordAction(GetOrdered{
		ClientId: request.ClientId,
		OpId:     request.OpId,
		Key:      request.Key,
		GId:      gid,
	})

	// Find value of key, set response and trace
	value, ok := s.data.valmap[request.Key]

	if !ok {
		response.Value = ""
	} else {
		response.Value = value
	}

	response.OpId = request.OpId

	trace.RecordAction(GetResult{
		ClientId: request.ClientId,
		OpId:     request.OpId,
		Key:      request.Key,
		GId:      gid,
		Value:    response.Value,
	})

	response.Token = trace.GenerateToken()

	log.Println("Sending to client", request.ClientId, ", Key: ", request.Key, "Value", value, "gId", gid)
	s.notifyNextOpID(request)

	return nil
}

func (s *ServerRPC) waitPrevOpID(request kvsshared.KVSMessage) {
	if request.OpId != 0 {
		s.syn.clientChanMutex.Lock()
		toHash := kvsshared.KVSMessage{
			OpId:     request.OpId - 1,
			ClientId: request.ClientId,
		}
		ret, found := s.data.clientChanMap[s.hashKVSMessage(toHash)]
		if !found {
			//Not in map, create (you've arrived before your previous msg has)
			ret = make(chan bool, 2)
			s.data.clientChanMap[s.hashKVSMessage(toHash)] = ret
		}
		s.syn.clientChanMutex.Unlock()
		// wait for msg from prev
		<-ret
	}
}

//Tail Server Put Operations ------------------------------------------------
func (s *ServerRPC) handlePutAsTail(request ServerMessage, trace *tracing.Trace) {
	log.Println("Tail server#", s.config.ServerId, "received put request", request)

	// Check and wait if last opID is not 1 less than the current
	//s.waitForOrderedOpID(request)
	// Last OpID is good, lets process this Put

	s.syn.kvsGroup.mutex.Lock()
	defer s.syn.kvsGroup.mutex.Unlock()
	if s.prevServer.ServerID != 0 {
		trace.RecordAction(PutRecvd{
			ClientId: request.ClientId,
			OpId:     request.OpId,
			Key:      request.Key,
			Value:    request.Value,
		})

		trace.RecordAction(PutOrdered{
			ClientId: request.ClientId,
			OpId:     request.OpId,
			GId:      request.GId,
			Key:      request.Key,
			Value:    request.Value,
		})
	}

	trace.RecordAction(PutResult{
		ClientId: request.ClientId,
		OpId:     request.OpId,
		GId:      request.GId,
		Key:      request.Key,
		Value:    request.Value,
	})
	//TODO: Store token until we respond to client

	msg := kvsshared.KVSMessage{
		ClientId: request.ClientId,
		OpId:     request.OpId,
	}
	s.notifyNextOpID(msg)
	s.syn.kvsGroup.cond.Broadcast()

	//TODO: handle
}

func (s *ServerRPC) GetPutResult(request kvsshared.KVSMessage, response *kvsshared.KVSMessage) error {
	log.Println("Client", request.ClientId, "requesting put result for [OpID:", request.OpId, "] Put(", request.Key, ",",
		request.Value, ")")

	// Hash the request hashKVSMessage

	msg := s.hashKVSMessage(request)

	s.syn.kvsGroup.mutex.Lock()
	for {
		if ret, ok := s.data.clientKVSMap[msg]; ok {
			//trace := ret.Trace

			// Put result is at tail
			log.Println("Found put request for [OpID:", request.OpId, "] Put(", request.Key, ",",
				request.Value, ") from client", request.ClientId)
			response.GId = ret.GId
			response.Key = ret.Key
			response.Value = ret.Value
			response.OpId = ret.OpId
			response.Token = ret.Trace.GenerateToken()

			s.updateClientOpID(ret.ClientId, ret.OpId)

			break
		}
		// Put result not arrived yet
		log.Println("Waiting for put request for [OpID:", request.OpId, "] Put(", request.Key, ",",
			request.Value, ") from client", request.ClientId)
		s.syn.kvsGroup.cond.Wait()
	}
	log.Println("Responding to client", request.ClientId, " for put request for [OpID:", request.OpId, "] Put(", request.Key, ",",
		request.Value, ") from client", request.ClientId)

	s.syn.kvsGroup.cond.Broadcast()
	s.syn.kvsGroup.mutex.Unlock()
	return nil
}

//End Tail Server Operations
//===================================================================================================================

//===================================================================================================================
//Client OpID Operations

func (s *ServerRPC) updateClientOpIDNoLock(client string, opID uint32) {
	if _, ok := s.data.clientlastOpID[client]; !ok {
		s.data.clientlastOpID[client] = 0
		log.Println("Client", client, "setting opID to 0 in hashmap")
	}
	curr := s.data.clientlastOpID[client]
	if opID > curr {
		log.Println("Client", client, "setting opID to", opID, "in hashmap")
		s.data.clientlastOpID[client] = opID
	}
	s.syn.opIDGroup.cond.Broadcast()
}

func (s *ServerRPC) updateClientOpID(client string, opID uint32) {
	s.syn.opIDGroup.mutex.Lock()
	defer s.syn.opIDGroup.mutex.Unlock()
	s.updateClientOpIDNoLock(client, opID)
}

func (s *ServerRPC) getClientOpIDNoLock(client string) (uint32, error) {
	ret, ok := s.data.clientlastOpID[client]

	if ok {
		log.Println("Client", client, "found opID", ret, " in map")
		return ret, nil
	} else {
		log.Println("Client", client, "no OPID found, returning error")
		return 0, errors.New("No Client put")
	}
}

func (s *ServerRPC) getClientOpID(client string) (uint32, error) {
	s.syn.opIDGroup.mutex.RLock()
	defer s.syn.opIDGroup.mutex.RUnlock()
	return s.getClientOpIDNoLock(client)
}

//End Client OpID Operations
//===================================================================================================================

//===================================================================================================================
//Server GID Operations

func (s *ServerRPC) incrGIDPutAndReturn() uint64 {
	s.syn.gidMutex.Lock()
	defer s.syn.gidMutex.Unlock()
	s.data.gID += uint64(math.Pow(2, 18))
	return s.data.gID
}

func (s *ServerRPC) incrGIDGetAndReturn() uint64 {
	s.syn.gidMutex.Lock()
	defer s.syn.gidMutex.Unlock()
	s.data.gID++

	return s.data.gID
}

func (s *ServerRPC) getGID() uint64 {
	s.syn.gidMutex.Lock()
	defer s.syn.gidMutex.Unlock()

	return s.data.gID
}

func (s *ServerRPC) incrGIDNewTail() uint64 {
	s.syn.gidMutex.Lock()
	defer s.syn.gidMutex.Unlock()
	//Increase GID by 2^59 * (17 - ServerID)
	// eg serverID = 5, GID = GID+2^59*12
	s.data.gID += uint64(math.Pow(2, 59)) * (17 - uint64(s.config.ServerId))

	return s.data.gID
}

//End Server GID Operations
//===================================================================================================================

//===================================================================================================================
//Server Failure Handling Protocol

//TODO: failed to connect: trace, or give up?
func (s *ServerRPC) FailedServerNotify(request FailedServerNotification, response *FailedServerAck) error {
	go s.notifyErrChan(request.FailedServerId)
	s.syn.failMutex.Lock()
	defer s.syn.failMutex.Unlock()
	response.ServerId = s.config.ServerId
	response.FailedServerId = request.FailedServerId
	trace := s.strace.ReceiveToken(request.Token)
	trace.RecordAction(ServerFailRecvd{request.FailedServerId})

	fmt.Println("Next Server ID", request.NewServerId)

	if request.FailedServerId == s.config.ServerId {
		panic("You can't notify me that i've failed!!! bad coord")
	}

	if request.NewServerId == 0 {
		err := s.handleServerFailureNoNewServer(request)
		if err != nil {
			fmt.Println(err)
			response.Success = false
			return nil
		}
		response.Success = true
	} else {
		err := s.handleServerFailureNewServer(request)
		if err != nil {
			fmt.Println(err)
			response.Success = false
			return nil
		}

		if request.FailedServerId > s.config.ServerId {
			trace.RecordAction(NewFailoverSuccessor{request.NewServerId})
		} else if request.FailedServerId < s.config.ServerId {
			trace.RecordAction(NewFailoverPredecessor{request.NewServerId})
		}

		//Must wait until handshake received by other server
		response.Success = true
	}

	trace.RecordAction(ServerFailHandled{request.FailedServerId})
	response.Token = trace.GenerateToken()

	//Handle reconnection:
	//Trace
	//Remove old server from next/prev
	//Reconnect to prev server
	//Response

	//TODO: update internal state
	return nil
}

func (s *ServerRPC) ServerFailureHandshakeRPC(request NewServerFromFailure, response *NewServerFromFailure) error {
	if request.ServerID == s.config.ServerId {
		response.Success = true
	}
	return nil
}

func (s *ServerRPC) handleServerFailureNewServer(request FailedServerNotification) error {
	newServer := LinkedServer{
		ServerID: request.NewServerId,
		Addr:     request.NewServerAddr,
	}
	var conn *rpc.Client
	conn, err := rpc.Dial("tcp", newServer.Addr)
	newServer.conn = conn
	if err != nil {
		return err
	}
	newServerFromFailure := NewServerFromFailure{
		ServerID: s.config.ServerId,
		Success:  false,
	}
	var handshakeErr error
	if request.FailedServerId > s.config.ServerId {
		//failed was next
		s.nextServer = newServer
		handshakeErr = s.nextServer.conn.Call("ServerRPC.ServerFailureHandshakeRPC", newServerFromFailure, &newServerFromFailure)
		if !s.waitForNextServerRequest(newServer.ServerID) {
			//abort
			log.Println("Aborting......................................................")
			return nil
		}
	} else if request.FailedServerId < s.config.ServerId {
		s.prevServer = newServer
		handshakeErr = s.prevServer.conn.Call("ServerRPC.ServerFailureHandshakeRPC", newServerFromFailure, &newServerFromFailure)
		// Create request struct for syncing
		myLatestData := ServerFailLatestDataIHave{
			Data:     s.data.clientlastOpID,
			ServerId: s.config.ServerId,
		}

		log.Println("[REQUEST SYNC] Server#", s.config.ServerId, "sending request to predecessor:",
			newServer.ServerID, "for latest data")

		// Create response struct for syncing
		var serverSyncResponse ServerFailDataSycResponse

		otherErr := s.prevServer.conn.Call("ServerRPC.ServerFailureDataSyncRPC", myLatestData, &serverSyncResponse)

		if serverSyncResponse.Success {
			log.Println("[SYNC SUCCESS] Predecessor server#", newServer.ServerID, "synced data with me:", s.config.ServerId)
		} else {
			log.Println("[SYNC FAILED] Predecessor server#", newServer.ServerID,
				"FAILED (Success: False) to sync data with me:", s.config.ServerId)
		}

		if otherErr != nil {
			fmt.Println("Some other obscure error in server", s.config.ServerId)
			fmt.Println(otherErr)
		}
		fmt.Println("After data sync server#", s.config.ServerId)
	}

	if handshakeErr != nil {
		fmt.Println("Handshake Error")
		return handshakeErr
	}

	return nil
}

func (s *ServerRPC) waitForNextServerRequest(nextServerID uint8) bool {
	done := false
	for !done {
		select {
		case doneID := <-s.errChannel:
			if doneID == nextServerID {
				done = true
				log.Println("Done sedning")
				return true
			}
			if doneID == s.config.ServerId+nextServerID+20 {
				log.Println("Error detected")
				return false
			} else {
				go s.notifyErrChan(doneID)
			}
		}
	}
	return true
}

func (s *ServerRPC) handleServerFailureNoNewServer(request FailedServerNotification) error {
	if request.FailedServerId > s.config.ServerId {
		s.nextServer.Addr = ""
		s.nextServer.ServerID = 0
		err := s.nextServer.conn.Close()
		if err != nil {
			fmt.Println(err)
			return err
		}
		s.nextServer.conn = nil
		fmt.Println("I'm now the tail!", s.config.ServerId)
		s.incrGIDNewTail()
	} else if request.FailedServerId < s.config.ServerId {
		s.prevServer.Addr = ""
		s.prevServer.ServerID = 0
		err := s.prevServer.conn.Close()
		if err != nil {
			fmt.Println(err)
			return err
		}
		s.prevServer.conn = nil
		fmt.Println("I'm now the head!", s.config.ServerId)
	}
	return nil
}

func (s *ServerRPC) ServerFailureDataSyncRPC(request ServerFailLatestDataIHave, response *ServerFailDataSycResponse) error {
	log.Println("[ NEW SYNC ] Server#", s.config.ServerId, "got data sync request from server#", request.ServerId)
	// Start counter & lock KVS
	s.syn.kvsGroup.mutex.Lock()
	defer s.syn.kvsGroup.mutex.Unlock()
	messagesSent := 0

	// Log the other servers data
	if len(request.Data) > 0 {
		log.Println("[ SYNC ] Here is what the other (server", request.ServerId, ") has:")
		for clientId, lastOpId := range request.Data {
			log.Println("[ SYNC ] For client:", clientId, "their highest OpId is:", lastOpId)
		}
	} else {
		log.Println("[ SYNC ] Other (server", request.ServerId, ") has no data (empty map)")
	}

	// Log the current servers data
	if len(s.data.clientlastOpID) > 0 {
		log.Println("[ SYNC ] Here is what I (server", s.config.ServerId, ") have:")
		for clientId, lastOpId := range s.data.clientlastOpID {
			log.Println("[ SYNC ] For client:", clientId, "my highest OpId is:", lastOpId)
		}
	} else {
		log.Println("[ SYNC ] I (server", s.config.ServerId, ") have no data (empty map)")
	}

	// Compare other server's data with my data
	for clientId, myOpId := range s.data.clientlastOpID {
		// Check if other server knows about this client and has a lower OpId
		otherServerOpId, ok := request.Data[clientId]

		// Other server knows about this client but has lower OpId
		if ok && otherServerOpId < myOpId {
			// I have a higher OpId than the other server
			log.Println("[ SYNC", s.config.ServerId, "->", request.ServerId,
				"] I have a higher OpId than the other server (", myOpId, ">", otherServerOpId, ") for client", clientId)

			// Send the missing OpIds for this client to the other server
			for opIdToSend := otherServerOpId + 1; opIdToSend <= myOpId; opIdToSend++ {
				// Check if there is a request for this opId
				var err error
				messagesSent, err = s.resendOldRequests(request, clientId, opIdToSend, messagesSent)
				if err != nil {
					log.Println("Error here")
					go s.notifyErrChan(s.config.ServerId + request.ServerId + 20)
					return nil
				}
			}
		} else if !ok {
			// Other server does not know about this client
			log.Println("[ SYNC", s.config.ServerId, "->", request.ServerId,
				"] Other server does not know about client", clientId, ",sending all my OpIds for this client")

			// Send all OpIds for this client to the other server
			for opIdToSend := uint32(0); opIdToSend <= myOpId; opIdToSend++ {
				// Check if there is a request for this opId
				var err error
				messagesSent, err = s.resendOldRequests(request, clientId, opIdToSend, messagesSent)
				if err != nil {
					log.Println("Aborting resending... new failure detected")
					go s.notifyErrChan(s.config.ServerId + request.ServerId + 20)
					//s.errChannel <- request.ServerId + s.config.ServerId + 20
					return nil
				}
			}
		} else {
			// Other server knows about this client and all their OpIds
			log.Println("[ SYNC", s.config.ServerId, "->", request.ServerId,
				"] Other server knows about client", clientId, "but has same/higher(?) OpId", otherServerOpId,
				"my OpId is", myOpId, ", their OpId is", request.Data[clientId])
		}
	}

	// Set the response & Unlock
	//s.syn.kvsGroup.mutex.Unlock()
	response.Success = true
	log.Println("[ DONE SYNC", s.config.ServerId, "->", request.ServerId,
		"] Done syncing data (sent", messagesSent, "messages)")

	// Done syncing data
	go s.notifyErrChan(request.ServerId)
	return nil
}

func (s *ServerRPC) resendOldRequests(request ServerFailLatestDataIHave, clientId string, opIdToSend uint32, messagesSent int) (int, error) {
	messageForHash := kvsshared.KVSMessage{ClientId: clientId, OpId: opIdToSend}
	serverMessage, ok := s.data.clientKVSMap[s.hashKVSMessage(messageForHash)]

	if ok {
		// Convert ServerMessage to KVSMessage
		kvsMessage := kvsshared.KVSMessage{
			ClientId:            serverMessage.ClientId,
			GId:                 serverMessage.GId,
			OpId:                serverMessage.OpId,
			Key:                 serverMessage.Key,
			Value:               serverMessage.Value,
			LatestSentPutId:     serverMessage.LatestSentPutId,
			LatestSentGetId:     serverMessage.LatestSentGetId,
			LatestReceivedGetId: serverMessage.LatestReceivedGetId,
			LatestReceivedPutId: serverMessage.LatestReceivedPutId,
			Token:               serverMessage.Token,
		}
		// Send the request
		log.Println("[ SYNC", s.config.ServerId, "->", request.ServerId, "] Sending OpId", opIdToSend,
			"from client", clientId, "to other server")
		messagesSent++
		//s.sendPutToNextServer(kvsMessage, &kvsMessage)

		fmt.Println("Curr next server: ", s.nextServer.ServerID)
		err := s.nextServer.conn.Call("ServerRPC.PutFWDRPC", kvsMessage, &kvsMessage)
		if err != nil {
			log.Println("[ SYNC", s.config.ServerId, "->", request.ServerId, "] detected new failure of next server. "+
				"Aborting data sync...")
			go s.notifyErrChan(s.config.ServerId + request.ServerId + 20)
			return messagesSent, err
		}
		time.Sleep(time.Millisecond * 50)
	}
	return messagesSent, nil
}

func (s *ServerRPC) notifyErrChan(id uint8) {
	s.errChannel <- id
}

//End Server Failure Handling Protocol
//===================================================================================================================
