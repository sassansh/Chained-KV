package kvslib

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"cs.ubc.ca/cpsc416/a3/kvsshared"

	"github.com/DistributedClocks/tracing"
)

////////////////////////////////////////////////////// Tracing structs
// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type HeadReq struct {
	ClientId string
}

type HeadResRecvd struct {
	ClientId string
	ServerId uint8
}

type TailReq struct {
	ClientId string
}

type TailResRecvd struct {
	ClientId string
	ServerId uint8
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

////////////////////////////////////////////////////// Global vars

// Error messages that can occur when trying to use closed RPC client connections
const ErrShutdown = "connection is shut down"

////////////////////////////////////////////////////// KVS struct

type KVS struct {
	notifyCh   NotifyChannel
	clientID   string
	opID       uint32
	wg         sync.WaitGroup
	running    bool
	mtxRunning sync.RWMutex

	// Local addresses for connecting to head and tail servers
	localHeadServerIPPort string
	localTailServerIPPort string

	// Remote RPC client connections
	coord    *rpc.Client
	head     *rpc.Client
	tail     *rpc.Client
	headAddr string
	tailAddr string

	// Mutexes for updating the head and tail server address strings from within goroutines
	mtxHead      sync.Mutex
	mtxTail      sync.Mutex
	mtxPending   sync.Mutex
	mtxMsgTime   sync.Mutex
	pendingGets  uint32
	pendingPuts  uint32
	timeLastHead time.Time
	timeLastTail time.Time
	doneTimeout  chan bool

	// Trace for start and stop methods
	ktrace  *tracing.Trace
	ktracer *tracing.Tracer

	// Map for queueing
	keyBuffer map[string]kvsQueue
	mtxKeyBuf sync.Mutex

	// Get/Put opid metadata for server
	putOpHistory    [2]uint32
	mtxHistory      sync.Mutex
	lastReceivedPut uint32
	lastReceivedGet uint32
	mtxSyncd        sync.Mutex
	lastSyncdTail   string
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	trace := localTracer.CreateTrace()
	trace.RecordAction(KvslibStart{clientId}) // Note: KvslibStart must be the first recorded event in ktrace

	// Connect to coord
	coordServer, err := connectToServer(localCoordIPPort, coordIPPort)
	if err != nil {
		return nil, err
	}

	// Get head and tail server addresses from coord node
	headServerAddr, err := d.getHeadServerAddress(clientId, coordServer, trace, localTracer)
	if err != nil {
		return nil, err
	}
	tailServerAddr, err := d.getTailServerAddress(clientId, coordServer, trace, localTracer)
	if err != nil {
		return nil, err
	}

	// Connect to head and tail servers
	headServer, err := connectToServer(localHeadServerIPPort, headServerAddr)
	if err != nil {
		return nil, err
	}
	tailServer, err := connectToServer(localTailServerIPPort, tailServerAddr)
	if err != nil {
		return nil, err
	}

	// Set KVS properties
	d.notifyCh = make(NotifyChannel, chCapacity)
	d.opID = 0
	d.clientID = clientId
	d.localHeadServerIPPort = localHeadServerIPPort
	d.localTailServerIPPort = localTailServerIPPort
	d.coord = coordServer
	d.head = headServer
	d.tail = tailServer
	d.headAddr = headServerAddr
	d.tailAddr = tailServerAddr
	d.ktrace = trace
	d.ktracer = localTracer
	d.running = true
	d.keyBuffer = make(map[string]kvsQueue)
	d.putOpHistory[0] = 0
	d.putOpHistory[1] = 0
	d.lastReceivedGet = 0
	d.lastReceivedPut = 0
	d.pendingGets = 0
	d.pendingPuts = 0
	d.timeLastHead = time.Now()
	d.timeLastTail = time.Now()
	d.lastSyncdTail = tailServerAddr
	d.doneTimeout = make(chan bool, 1)

	go d.checkTimeout(d.doneTimeout)

	return d.notifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	if !d.getRunningState() {
		// Don't perform Get requests if KVSlib is stopped
		return 0, errors.New(ErrShutdown)
	}
	currOpID := d.opID
	req := Get{ClientId: clientId, OpId: currOpID, Key: key}
	d.opID++
	d.wg.Add(1)
	d.incrementPendingGets()

	// Make non-blocking req for the value associated with a key
	go d.getFromTail(req, tracer)
	return currOpID, nil
}

// Get helper
func (d *KVS) getFromTail(getReq Get, tracer *tracing.Tracer) {
	defer d.wg.Done()

	// Block if there is a pending Put request for the same key
	shouldBlock, done := d.getShouldBlock(getReq)
	for shouldBlock {
		<-done // Note: This returns for all threads blocking on it when the corresponding Put closes the channel
		shouldBlock, done = d.getShouldBlock(getReq)
	}

	// Get the current tail node
	tail, tailAddr, err := d.getTailServer("")
	if err != nil {
		log.Println("KVSLIB: Error getting tail server: ", err)
		return
	}

	// Tracing
	trace := tracer.CreateTrace()
	trace.RecordAction(getReq)

	// Send get request to tail server
	var res *kvsshared.KVSMessage
	req := kvsshared.KVSMessage{
		ClientId: getReq.ClientId,
		OpId:     getReq.OpId,
		Key:      getReq.Key,
		Token:    trace.GenerateToken(),
	}
	res, err = d.sendMessage(tail, tailAddr, "ServerRPC.Get", false, req)
	if err != nil {
		log.Println("KVSLIB: Error getting reply from server: ", err)
		return
	}

	// Update history
	d.updateLastAckedOpID(res.OpId, false)
	d.decrementPendingGets()
	d.updateTimeSinceTail(time.Now())

	// Trace and write result to client
	tracer.ReceiveToken(res.Token)
	trace.RecordAction(GetResultRecvd{
		OpId:  res.OpId,
		GId:   res.GId,
		Key:   res.Key,
		Value: res.Value,
	})
	log.Println("KVSLIB: Received reply for Get request: GId: ", res.GId, " OpId: ", res.OpId, " Value: ", res.Value)
	d.notifyCh <- ResultStruct{GId: res.GId, OpId: res.OpId, Result: res.Value}
}

/* Don't send get requests if there are pending put requests for the same key */
func (d *KVS) getShouldBlock(req Get) (bool, chan bool) {
	d.mtxKeyBuf.Lock()
	defer d.mtxKeyBuf.Unlock()
	kBuf := d.keyBuffer[req.Key]
	if !kBuf.initialized || kBuf.isEmpty() {
		return false, nil
	}
	front, _ := kBuf.peek()
	if req.OpId > front.opId {
		return true, front.done
	}
	return false, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	if !d.getRunningState() {
		// Don't perform Put requests if KVSlib is stopped
		return 0, errors.New(ErrShutdown)
	}
	currOpID := d.opID
	req := Put{ClientId: clientId, OpId: currOpID, Key: key, Value: value}
	prevOpId := d.updatePrevPutOpId(currOpID)
	d.opID++
	d.wg.Add(1)
	d.addPutToQueue(req)
	d.incrementPendingPuts()

	// Make non-blocking req
	go d.putAtHead(req, tracer, prevOpId)
	return currOpID, nil
}

// Put helper
func (d *KVS) putAtHead(putReq Put, tracer *tracing.Tracer, prevPutOpId uint32) {
	defer d.wg.Done()

	// Block if there is a pending Put request for the same key
	shouldBlock, prevDone := d.putShouldBlock(putReq)
	for shouldBlock {
		<-prevDone // Note: This returns for all threads blocking on it when the corresponding Put closes the channel
		shouldBlock, prevDone = d.putShouldBlock(putReq)
	}

	// Get the current head server from coord
	head, headAddr, err := d.getHeadServer("")
	if err != nil {
		log.Println("KVSLIB: Error getting head server RPC client:", err)
		return
	}

	// Trace
	trace := tracer.CreateTrace()
	trace.RecordAction(putReq)

	// Declare req structs
	var res *kvsshared.KVSMessage
	req := kvsshared.KVSMessage{
		ClientId:        d.clientID,
		OpId:            putReq.OpId,
		Key:             putReq.Key,
		Value:           putReq.Value,
		Token:           trace.GenerateToken(),
		LatestSentPutId: prevPutOpId,
	}

	// Start put request at head
	res, err = d.sendMessage(head, headAddr, "ServerRPC.Put", true, req)
	if err != nil {
		log.Println("KVSLIB: Error during Put request: ", err)
		return
	}
	d.updateTimeSinceHead(time.Now())

	// Get the current tail server from coord
	tail, tailAddr, err := d.getTailServer("")
	if err != nil {
		log.Println("KVSLIB: Error getting tail server RPC client:", err)
		return
	}

	// Get confirmation that put request succeeded from server
	res, err = d.sendMessage(tail, tailAddr, "ServerRPC.GetPutResult", false, *res)
	if err != nil {
		log.Println("KVSLIB: Error during Put request: ", err)
		return
	}

	// Update history
	d.updateLastAckedOpID(res.OpId, true)
	d.decrementPendingPuts()
	d.updateTimeSinceTail(time.Now())

	// Trace, unblock Gets, and write result to client
	tracer.ReceiveToken(res.Token)
	trace.RecordAction(PutResultRecvd{OpId: res.OpId, GId: res.GId, Key: res.Key})
	done := d.removePutFromQueue(putReq)
	close(done)
	d.notifyCh <- ResultStruct{GId: res.GId, OpId: res.OpId, Result: res.Value}
}

/* Add new put requests to a queue so that subsequent Get requests for the same key can block until the Put is done */
func (d *KVS) addPutToQueue(req Put) {
	d.mtxKeyBuf.Lock()
	defer d.mtxKeyBuf.Unlock()

	kBuf := d.keyBuffer[req.Key]
	if !kBuf.initialized {
		kBuf.init()
	}
	item := kvsQueueItem{req.OpId, make(chan bool, 1)}
	kBuf.enqueue(item)
	d.keyBuffer[req.Key] = kBuf
}

func (d *KVS) removePutFromQueue(req Put) chan bool {
	d.mtxKeyBuf.Lock()
	defer d.mtxKeyBuf.Unlock()
	kBuf := d.keyBuffer[req.Key]
	res, _ := kBuf.dequeue()
	d.keyBuffer[req.Key] = kBuf
	return res.done
}

/* Don't send put requests if there are earlier pending put requests for the same key */
func (d *KVS) putShouldBlock(req Put) (bool, chan bool) {
	d.mtxKeyBuf.Lock()
	defer d.mtxKeyBuf.Unlock()
	kBuf := d.keyBuffer[req.Key]
	front, _ := kBuf.peek()
	if req.OpId < front.opId {
		return true, front.done
	}
	return false, nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	d.wg.Wait()
	// Set running state to false as a safeguard to prevent goroutines from trying to reopen connections
	d.setRunningState(false)
	<-d.doneTimeout

	// Close connections and notify channel
	d.head.Close()
	d.tail.Close()
	d.coord.Close()
	close(d.notifyCh)

	// Trace and exit
	d.ktrace.RecordAction(KvslibStop{d.clientID}) // Note: KvslibStop must be the last recorded event in ktrace
}

////////////////////////////////////////////////////// RPC Functions

/* Create an RPC connection from localIPPort to remoteIPPort */
/* CITATION: The following post mentions that RPC Dial is calling net.Dial in the RPC source code:
https://github.com/golang/go/issues/31106 */
func connectToServer(localIPPort, remoteIPPort string) (*rpc.Client, error) {
	localAddr, err := net.ResolveTCPAddr("tcp", localIPPort)
	if err != nil {
		return nil, err
	}
	remoteAddr, err := net.ResolveTCPAddr("tcp", remoteIPPort)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", localAddr, remoteAddr)
	if err != nil {
		log.Println("KVSLIB: Error connecting to", remoteIPPort, ": ", err)
		return nil, err
	}
	conn.SetLinger(0)
	return rpc.NewClient(conn), err
}

/* Call one of the server's RPC methods; If sendToHead is false, then the method is called at the tail server */
func (d *KVS) sendMessage(client *rpc.Client, serverAddr, serviceMethod string, sendToHead bool, req kvsshared.KVSMessage) (res *kvsshared.KVSMessage, err error) {
	req.LatestReceivedGetId, req.LatestReceivedPutId = d.getLastAckedOpID()
	for {
		//log.Println("KVSLib: sendMessage, calling", req.OpId, serviceMethod)
		err := client.Call(serviceMethod, req, &res)
		//log.Println("KVSLib: sendMessage, called", req.OpId, serviceMethod)
		if err != nil {
			log.Println("KVSLib: err in message send", err)
			if d.getRunningState() == false {
				// Shouldn't be able to reach this block since all gets and puts must be successful
				return nil, errors.New("No longer running; This error should never happen...")
			}
			// If we reach this block, then the server has probably failed;
			// Check for server failure, update the connection, and re-send the message
			client, serverAddr, err = d.getServer(sendToHead, serverAddr)
			if err != nil {
				// This error case should never happen because the coord never fails and
				// getServer loops until it makes a successful connection
				log.Println("KVSlib: Shouldn't ever hit this line")
				//return nil, err
			}
			log.Println("KVSLib: Try to sync now")
			if !sendToHead {
				// Update the last acked message ids and send them to the server for failure recovery before resending msg
				req.LatestReceivedGetId, req.LatestReceivedPutId = d.getLastAckedOpID()
				_ = d.syncTailServer(client, serverAddr, req.ClientId, serviceMethod, req.LatestReceivedGetId, req.LatestReceivedPutId)
			}
		} else {
			// Successfully read from server
			return res, nil
		}
	}
}

////////////////////////////////////////////////////// Server Sync Helper Functions

/* Send the server the latest acked OpId */
func (d *KVS) syncTailServer(client *rpc.Client, serverAddr, clientId string, serviceMethod string, latestReceivedGetId, latestReceivedPutId uint32) error {
	d.mtxSyncd.Lock()
	defer d.mtxSyncd.Unlock()
	if serverAddr == d.lastSyncdTail {
		// Only sync the tail if it has not already been sync'd
		log.Println("KVSLib: Already Syncd", serverAddr)
		return nil
	}

	var lastOpId uint32
	if latestReceivedGetId > latestReceivedPutId {
		lastOpId = latestReceivedGetId
	} else {
		lastOpId = latestReceivedPutId
	}
	log.Println("KVSLib: Syncing new tail", serverAddr, " with last op id ", lastOpId)

	req := kvsshared.KVSMessage{OpId: lastOpId, ClientId: clientId}
	err := client.Call("ServerRPC.SyncLatestOpID", req, &req)
	log.Println("KVSLib: Done syncing new tail", serverAddr)
	if err != nil {
		log.Println("KVSLib: Sync error", err)
	}
	// Note: Updating this even when the call returns an error is fine if the only time an error occurs is when the new server has failed.
	d.lastSyncdTail = serverAddr
	return err
}

func (d *KVS) updateLastAckedOpID(opId uint32, putUpdate bool) {
	d.mtxHistory.Lock()
	defer d.mtxHistory.Unlock()
	if putUpdate {
		if d.lastReceivedPut < opId {
			log.Println("KVSLib: Set last Acked Put to ", opId)
			d.lastReceivedPut = opId
		}
	} else {
		if d.lastReceivedGet < opId {
			log.Println("KVSLib: Set last Acked Get to ", opId)
			d.lastReceivedGet = opId
		}
	}
}

func (d *KVS) getLastAckedOpID() (lastReceivedGet, lastReceivedPut uint32) {
	d.mtxHistory.Lock()
	defer d.mtxHistory.Unlock()
	return d.lastReceivedGet, d.lastReceivedPut
}

func (d *KVS) updatePrevPutOpId(opId uint32) uint32 {
	d.putOpHistory[0] = d.putOpHistory[1]
	d.putOpHistory[1] = opId
	return d.putOpHistory[0]
}

////////////////////////////////////////////////////// Server Connection Helper Functions

// Checks coord for the current head/tail server, updates d, and returns a connection
func (d *KVS) getServer(head bool, serverAddr string) (*rpc.Client, string, error) {
	// Temporary fix to receiving out of date server from coord
	// time.Sleep(time.Millisecond * 500)
	if head {
		return d.getHeadServer(serverAddr)
	}
	return d.getTailServer(serverAddr)
}

// Get the head/tail server address stored in d
func (d *KVS) getStoredServerAddr(head bool) string {
	if head {
		return d.getStoredHeadAddr()
	}
	return d.getStoredTailAddr()
}

// Checks coord for the current head server, updates d, and returns a connection
func (d *KVS) getHeadServer(prevServerAddr string) (*rpc.Client, string, error) {
	if !d.getRunningState() {
		// Avoid trying to connect if KVSlib is stopping
		return nil, "", errors.New(ErrShutdown)
	}
	d.mtxHead.Lock()
	defer d.mtxHead.Unlock()
	if prevServerAddr != d.headAddr && prevServerAddr != "" {
		// Another goroutine already set up a new connection, so return the stored connection
		return d.head, d.headAddr, nil
	}
	time.Sleep(time.Millisecond * 100)
	connected := false
	for !connected {
		headAddr, err := d.getHeadServerAddress(d.clientID, d.coord, d.ktrace, d.ktracer)
		if err != nil {
			if d.getRunningState() == true {
				log.Println("KVSLIB: Error getting tail address from coord", err)
			}
			return nil, "", err
		}
		if headAddr == d.headAddr {
			break
		}
		err = d.head.Close()
		if err != nil {
			log.Println("KVSLIB: Error closing head connection: ", headAddr)
			return nil, "", err
		}
		log.Println("KVSLIB: Connecting to new head server: ", headAddr)
		nextHead, err := connectToServer(d.localHeadServerIPPort, headAddr)
		if err != nil {
			// This block will only be hit if the server fails just after receiving the address from coord
			log.Println("KVSLIB: Error connecting to head server: ", err)
			continue
		}
		log.Println("KVSLIB: Connected to new head server: ", headAddr)
		d.headAddr = headAddr
		d.head = nextHead
		connected = true
	}
	return d.head, d.headAddr, nil
}

// Checks coord for the current tail server, updates d, and returns a connection
func (d *KVS) getTailServer(prevServerAddr string) (*rpc.Client, string, error) {
	if !d.getRunningState() {
		// Avoid trying to connect if KVSlib is stopping
		return nil, "", errors.New(ErrShutdown)
	}
	d.mtxTail.Lock()
	defer d.mtxTail.Unlock()
	if prevServerAddr != d.tailAddr && prevServerAddr != "" {
		// Another goroutine already set up a new connection, so return the stored connection
		return d.tail, d.tailAddr, nil
	}
	time.Sleep(time.Millisecond * 100)
	connected := false
	for !connected {
		tailAddr, err := d.getTailServerAddress(d.clientID, d.coord, d.ktrace, d.ktracer)
		if err != nil {
			if d.getRunningState() == true {
				log.Println("KVSLIB: Error getting tail address from coord", err)
			}
			return nil, "", err
		}
		if tailAddr == d.tailAddr {
			break
		}
		err = d.tail.Close()
		if err != nil {
			log.Println("KVSLIB: Error closing tail connection:", err)
		}
		log.Println("KVSLIB: Changing tail connection to ", tailAddr)
		nextTail, err := connectToServer(d.localTailServerIPPort, tailAddr)
		if err != nil {
			// This block will only be hit if the server fails just after receiving the address from coord
			log.Println("KVSLIB: Error connecting to tail server: ", err)
			continue
		}
		d.tailAddr = tailAddr
		d.tail = nextTail
		connected = true
	}
	return d.tail, d.tailAddr, nil
}

/* Returns a string with the IP/Port of the current head server and performs related tracing actions */
func (d *KVS) getHeadServerAddress(clientId string, coord *rpc.Client, trace *tracing.Trace, tracer *tracing.Tracer) (server string, err error) {
	trace.RecordAction(HeadReq{clientId})
	args := kvsshared.ServerAddrReq{ClientId: clientId, Token: trace.GenerateToken()}
	var res *kvsshared.ServerAddrResult
	err = coord.Call("CoordRPC.GetHeadServer", args, &res)
	if err != nil {
		return "", err
	}
	tracer.ReceiveToken(res.Token)
	trace.RecordAction(HeadResRecvd{ClientId: res.ClientId, ServerId: res.ServerId})
	return res.ServerAddress, nil
}

/* Returns a string with the IP/Port of the current tail server and performs related tracing actions */
func (d *KVS) getTailServerAddress(clientId string, coord *rpc.Client, trace *tracing.Trace, tracer *tracing.Tracer) (server string, err error) {
	trace.RecordAction(TailReq{clientId})
	args := kvsshared.ServerAddrReq{ClientId: clientId, Token: trace.GenerateToken()}
	var res *kvsshared.ServerAddrResult
	err = coord.Call("CoordRPC.GetTailServer", args, &res)
	if err != nil {
		return "", err
	}
	tracer.ReceiveToken(res.Token)
	trace.RecordAction(TailResRecvd{ClientId: res.ClientId, ServerId: res.ServerId})
	return res.ServerAddress, nil
}

func (d *KVS) getStoredHeadAddr() string {
	d.mtxHead.Lock()
	headAddr := d.headAddr
	d.mtxHead.Unlock()
	return headAddr
}

func (d *KVS) getStoredTailAddr() string {
	d.mtxTail.Lock()
	tailAddr := d.tailAddr
	d.mtxTail.Unlock()
	return tailAddr
}

func (d *KVS) getRunningState() bool {
	d.mtxRunning.RLock()
	running := d.running
	d.mtxRunning.RUnlock()
	return running
}

func (d *KVS) setRunningState(running bool) {
	d.mtxRunning.Lock()
	d.running = running
	d.mtxRunning.Unlock()
}

////////////////////////////////////////////////////// Timeout Helper Functions

/* Check for new head or tail server addresses if no messages have been received in awhile */
func (d *KVS) checkTimeout(done chan bool) {
	const timeout = 5
	for d.getRunningState() {
		time.Sleep(time.Second * time.Duration(timeout))
		pendingGets, pendingPuts := d.getPendingOpCount()
		timeSinceHead, timeSinceTail := d.getTimeSince()
		if pendingPuts > 0 {
			if time.Now().Sub(timeSinceHead) > timeout {
				log.Println("KVSLIB: time now and time since", time.Now(), timeSinceHead, time.Now().Sub(timeSinceHead))
				_, _, _ = d.getHeadServer("")
			}
		}
		if pendingGets > 0 || pendingPuts > 0 {
			if time.Now().Sub(timeSinceTail) > timeout {
				log.Println("KVSLIB: time now and time since for tail", time.Now(), timeSinceTail, time.Now().Sub(timeSinceTail))
				_, _, _ = d.getTailServer("")
			}
		}
	}
	done <- true
}

func (d *KVS) updateTimeSinceHead(msgTime time.Time) {
	d.mtxMsgTime.Lock()
	defer d.mtxMsgTime.Unlock()
	if msgTime.After(d.timeLastHead) {
		d.timeLastHead = msgTime
	}
}

func (d *KVS) updateTimeSinceTail(msgTime time.Time) {
	d.mtxMsgTime.Lock()
	defer d.mtxMsgTime.Unlock()
	if msgTime.After(d.timeLastTail) {
		d.timeLastTail = msgTime
	}
}

func (d *KVS) getTimeSince() (head, tail time.Time) {
	d.mtxMsgTime.Lock()
	defer d.mtxMsgTime.Unlock()
	return d.timeLastHead, d.timeLastTail
}

func (d *KVS) incrementPendingPuts() {
	d.mtxPending.Lock()
	defer d.mtxPending.Unlock()
	d.pendingPuts++
}

func (d *KVS) incrementPendingGets() {
	d.mtxPending.Lock()
	defer d.mtxPending.Unlock()
	d.pendingGets++
}

func (d *KVS) decrementPendingPuts() {
	d.mtxPending.Lock()
	defer d.mtxPending.Unlock()
	d.pendingPuts--
}

func (d *KVS) decrementPendingGets() {
	d.mtxPending.Lock()
	defer d.mtxPending.Unlock()
	d.pendingGets--
}

func (d *KVS) getPendingOpCount() (gets, puts uint32) {
	d.mtxPending.Lock()
	defer d.mtxPending.Unlock()
	return d.pendingGets, d.pendingPuts
}

////////////////////////////////////////////////////// Queue

type kvsQueueItem struct {
	opId uint32
	done chan bool
}

type kvsQueue struct {
	initialized bool
	tail        uint32
	buffer      []kvsQueueItem
}

func (q *kvsQueue) init() {
	q.tail = 0
	q.buffer = make([]kvsQueueItem, 0)
	q.initialized = true
}

func (q *kvsQueue) isEmpty() bool {
	if q.tail == 0 {
		return true
	}
	return false
}

func (q *kvsQueue) enqueue(item kvsQueueItem) {
	q.buffer = append(q.buffer, item)
	q.tail++
}

func (q *kvsQueue) dequeue() (kvsQueueItem, error) {
	if q.isEmpty() {
		return kvsQueueItem{}, errors.New("keyBuffer is empty")
	}
	res := q.buffer[0]
	q.tail--
	q.buffer = q.buffer[1:]
	return res, nil
}

func (q *kvsQueue) peek() (kvsQueueItem, error) {
	if q.isEmpty() {
		return kvsQueueItem{}, errors.New("keyBuffer is empty")
	}
	res := q.buffer[0]
	return res, nil
}
