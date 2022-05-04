package chainedkv

import (
	"container/list"
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"cs.ubc.ca/cpsc416/a3/kvsshared"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Actions to be recorded by coord (as part of ctrace, ktrace, and strace):

type CoordStart struct {
}

type ServerFail struct {
	ServerId uint8
}

type ServerFailHandledRecvd struct {
	FailedServerId   uint8
	AdjacentServerId uint8
}

type NewChain struct {
	Chain []uint8
}

type AllServersJoined struct {
}

type HeadReqRecvd struct {
	ClientId string
}

type HeadRes struct {
	ClientId string
	ServerId uint8
}

type TailReqRecvd struct {
	ClientId string
}

type TailRes struct {
	ClientId string
	ServerId uint8
}

type ServerJoiningRecvd struct {
	ServerId uint8
}

type ServerJoinedRecvd struct {
	ServerId uint8
}

type CoordConfig struct {
	ClientAPIListenAddr string
	ServerAPIListenAddr string
	LostMsgsThresh      uint8
	NumServers          uint8
	TracingServerAddr   string
	Secret              []byte
	TracingIdentity     string
}

type Coord struct {
	// Total number of servers expected to join
	numServers uint8

	// Map of all servers, indexed by server id (not necessarily in the chain)
	allServers     map[uint8]ServerInfo
	mtxAllServers  sync.RWMutex
	condAllServers *sync.Cond

	// Linked list of servers in the chain (real view of the chain, servers confirmed this view)
	serverChain     *list.List
	mtxServerChain  sync.RWMutex
	condServerChain *sync.Cond

	// Linked list of servers in the temp chain (temp view of the chain, servers not confirmed this view)
	// view is updated as servers fail
	serverChainTemp    *list.List
	mtxServerChainTemp sync.RWMutex

	// fchecker
	fcheckNotifyCh chan fchecker.FailureDetected
	lostMsgsThresh uint8
	fcheckIP       string

	// All servers joined
	allServersJoined     bool
	mtxAllServersJoined  sync.RWMutex
	condAllServersJoined *sync.Cond

	// Tracing
	ctracer *tracing.Tracer
	ctrace  *tracing.Trace
}

/// Helper Structs
type CoordRPC Coord

type ServerInfo struct {
	ServerId         uint8
	ServerAddr       string
	ServerListenAddr string
	ClientListenAddr string
	CoordListenAddr  string
	FcheckAddr       string
	Joined           bool
	Failed           bool
}

func NewCoord() *Coord {
	return &Coord{
		allServers:       make(map[uint8]ServerInfo),
		serverChain:      list.New(),
		serverChainTemp:  list.New(),
		allServersJoined: false,
	}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8,
	ctrace *tracing.Tracer) error {
	// Trace the start of the coord
	trace := ctrace.CreateTrace()
	trace.RecordAction(CoordStart{})

	// Log stuff
	log.Println("Coord started")
	log.Println("Waiting for", numServers, "servers to join...")

	// Register the mutexes with the conds
	c.condAllServers = sync.NewCond(&c.mtxAllServers)
	c.condAllServersJoined = sync.NewCond(&c.mtxAllServersJoined)
	c.condServerChain = sync.NewCond(&c.mtxServerChain)

	// Initialize fchecker
	fcheckNotifyCh, err := fchecker.Initialize()
	if err != nil {
		return errors.New("Failed to initialize fchecker")
	}

	// Parse IP from serverAPIListenAddr
	fcheckIp, _, _ := net.SplitHostPort(serverAPIListenAddr)

	// Set coord state
	c.ctracer = ctrace
	c.ctrace = trace
	c.fcheckNotifyCh = fcheckNotifyCh
	c.lostMsgsThresh = lostMsgsThresh
	c.numServers = numServers
	c.fcheckIP = fcheckIp + ":0"

	// Start listening for server API requests using RPC
	rpc.Register((*CoordRPC)(c))
	tcpAddrServerAPI, err := net.ResolveTCPAddr("tcp", serverAPIListenAddr)
	if err != nil {
		log.Fatal(err)
	}

	serverAPIListener, err := net.ListenTCP("tcp", tcpAddrServerAPI)
	if err != nil {
		log.Fatal(err)
	}

	go rpc.Accept(serverAPIListener)

	// Start listening for client API requests using RPC
	tcpAddrClientAPI, err := net.ResolveTCPAddr("tcp", clientAPIListenAddr)
	if err != nil {
		log.Fatal(err)
	}

	clientAPIListener, err := net.ListenTCP("tcp", tcpAddrClientAPI)
	if err != nil {
		log.Fatal(err)
	}

	go rpc.Accept(clientAPIListener)

	// Start listening for fchecker notifications
	go c.handleFcheckerNotifications()

	// Prevent program from exiting
	done := make(chan int, 1)
	<-done
	return nil
}

func (c *CoordRPC) Join(args ServerJoinRequest, reply *ServerJoinResponse) error {
	log.Println("Server", args.ServerId, "is trying to register with the coord")
	// Check if all servers have joined
	if c.allServersJoined {
		log.Println("Server", args.ServerId, " refused from registering. All servers have already joined")
		return errors.New("all servers have already joined")
	}

	// Add server to allServers map
	err := c.registerServer(args)
	if err != nil {
		return err
	}

	// Read trace token from args
	trace := c.ctracer.ReceiveToken(args.Token)

	// Record trace action for ServerJoiningRecvd
	trace.RecordAction(ServerJoiningRecvd{args.ServerId})

	// Wait for previous servers to join if ServerId is not 1 (first server)
	if args.ServerId != 1 {
		c.mtxAllServers.Lock()
		for !c.allServers[args.ServerId-1].Joined {
			c.condAllServers.Wait()
		}
		c.mtxAllServers.Unlock()
	}

	// Reply with tail server info (unless serverId == 1)
	c.mtxServerChain.Lock()
	defer c.mtxServerChain.Unlock()
	if args.ServerId == 1 {
		reply.TailServerId = 0
		reply.TailServerAddr = ""
	} else {
		TailServer := c.getTailServer()
		reply.TailServerId = TailServer.ServerId
		reply.TailServerAddr = TailServer.ServerListenAddr
	}
	reply.Token = trace.GenerateToken()

	return nil
}

func (c *CoordRPC) Joined(args ServerJoinedAck, reply *ServerJoinedResponse) error {
	log.Println("Server", args.ServerId, "attempting to join the chain")
	// Check if all servers have joined
	if c.allServersJoined {
		log.Println("Server", args.ServerId, " refused from joining chain. All servers have already joined")
		return errors.New("all servers have already joined")
	}

	// Read trace token from args
	trace := c.ctracer.ReceiveToken(args.Token)

	// Record trace action for ServerJoinedRecvd
	trace.RecordAction(ServerJoinedRecvd{args.ServerId})

	// Add server to chain
	err := c.addServerToChain(args.ServerId)
	if err != nil {
		return err
	}

	// Reply with success
	reply.Success = true

	return nil
}

func (c *CoordRPC) GetHeadServer(args kvsshared.ServerAddrReq, reply *kvsshared.ServerAddrResult) error {
	log.Println("Received GetHeadServer request from client", args.ClientId)
	// Read trace token from args
	trace := c.ctracer.ReceiveToken(args.Token)

	// Record trace action for HeadReqRecvd
	trace.RecordAction(HeadReqRecvd{args.ClientId})

	// Wait for all servers to join before replying
	c.mtxAllServersJoined.Lock()
	if !c.allServersJoined {
		log.Println("Waiting for all servers to join before replying to "+
			"GetHeadServer request from client", args.ClientId)
		c.condAllServersJoined.Wait()
		log.Println("All servers have joined, continuing reply to GetHeadServer request from client", args.ClientId)
	}
	c.mtxAllServersJoined.Unlock()

	// Get head server
	c.mtxServerChain.Lock()
	defer c.mtxServerChain.Unlock()
	headServer := c.getHeadServer()

	// Record trace action for HeadRes
	log.Println("Telling client", args.ClientId, "that head server is", headServer.ServerId)
	trace.RecordAction(HeadRes{args.ClientId, headServer.ServerId})

	// Reply with tail server info
	reply.ServerAddress = headServer.ClientListenAddr
	reply.ClientId = args.ClientId
	reply.ServerId = headServer.ServerId
	reply.Token = trace.GenerateToken()

	return nil
}

func (c *CoordRPC) GetTailServer(args kvsshared.ServerAddrReq, reply *kvsshared.ServerAddrResult) error {
	log.Println("Received GetTailServer request from client", args.ClientId)
	// Read trace token from args
	trace := c.ctracer.ReceiveToken(args.Token)

	// Record trace action for TailReqRecvd
	trace.RecordAction(TailReqRecvd{args.ClientId})

	// Wait for all servers to join before replying
	c.mtxAllServersJoined.Lock()
	if !c.allServersJoined {
		log.Println("Waiting for all servers to join before replying to "+
			"GetTailServer request from client", args.ClientId)
		c.condAllServersJoined.Wait()
		log.Println("All servers have joined, continuing reply to GetTailServer request from client", args.ClientId)
	}
	c.mtxAllServersJoined.Unlock()

	// Get tail server
	c.mtxServerChain.Lock()
	defer c.mtxServerChain.Unlock()
	tailServer := c.getTailServer()

	// Record trace action for TailRes
	log.Println("Telling client", args.ClientId, "that tail server is", tailServer.ServerId)
	trace.RecordAction(TailRes{args.ClientId, tailServer.ServerId})

	// Reply with tail server info
	reply.ServerAddress = tailServer.ClientListenAddr
	reply.ClientId = args.ClientId
	reply.ServerId = tailServer.ServerId
	reply.Token = trace.GenerateToken()

	return nil
}

func (c *CoordRPC) registerServer(serverInfo ServerJoinRequest) error {
	// Check if serverId is valid
	if serverInfo.ServerId <= 0 {
		log.Println("Server", serverInfo.ServerId, "refused from registering. Invalid serverId of <=0")
		return errors.New("serverId must be greater than 0")
	} else if serverInfo.ServerId > c.numServers {
		log.Println("Server", serverInfo.ServerId, "refused from registering. Invalid serverId of >numServers")
		return errors.New("serverId must be less than or equal to numServers: " + strconv.Itoa(int(c.numServers)))
	}

	// Check if serverId is already registered
	c.mtxAllServers.RLock()
	if _, ok := c.allServers[serverInfo.ServerId]; ok {
		c.mtxAllServers.RUnlock()
		return errors.New("serverId " + strconv.Itoa(int(serverInfo.ServerId)) + " is already registered")
	}
	c.mtxAllServers.RUnlock()

	// Register server
	log.Println("Registering server:", serverInfo.ServerId)
	c.mtxAllServers.Lock()
	c.allServers[serverInfo.ServerId] = ServerInfo{
		ServerId:         serverInfo.ServerId,
		ServerAddr:       serverInfo.ServerAddr,
		ServerListenAddr: serverInfo.ServerListenAddr,
		ClientListenAddr: serverInfo.ClientListenAddr,
		CoordListenAddr:  serverInfo.CoordListenAddr,
		FcheckAddr:       serverInfo.FcheckAddr,
		Joined:           false,
		Failed:           false,
	}

	// Print all server ids that have registered
	joinedServers := ""
	for _, server := range c.allServers {
		joinedServers += strconv.Itoa(int(server.ServerId)) + " "
	}
	log.Println("Registered servers so far:", joinedServers)

	c.mtxAllServers.Unlock()

	return nil
}

func (c *CoordRPC) addServerToChain(serverId uint8) error {
	// Check if serverId is already registered
	c.mtxAllServers.RLock()
	if _, ok := c.allServers[serverId]; !ok {
		c.mtxAllServers.RUnlock()
		log.Println("Server", serverId, "refused from joining the chain. Not registered")
		return errors.New("serverId not registered, invoke Join() RPC first")
	}

	// Check if serverId is already in chain
	if c.allServers[serverId].Joined {
		c.mtxAllServers.RUnlock()
		log.Println("Server", serverId, "refused from joining the chain. Already in the chain")
		return errors.New("serverId already in chain")
	}
	c.mtxAllServers.RUnlock()

	// Mark server as joined (copy, modify, replace)
	c.mtxAllServers.Lock()
	defer c.mtxAllServers.Unlock()
	if entry, ok := c.allServers[serverId]; ok {
		entry.Joined = true
		c.allServers[serverId] = entry
	}

	// Add server to chain
	log.Println("Adding server to chain:", serverId)
	c.mtxServerChain.Lock()
	defer c.mtxServerChain.Unlock()
	c.serverChain.PushBack(c.allServers[serverId])

	// Trace new chain
	traceNewChain(c.serverChain, c.ctrace)

	// Signal to servers that are waiting to Join()
	c.condAllServers.Broadcast()

	// Check if last server joining (all servers joined)
	if serverId == c.numServers {
		log.Println("Last server joined, all", c.numServers, "servers joined")
		// Mark all servers as joined
		c.mtxAllServersJoined.Lock()
		c.allServersJoined = true
		c.condAllServersJoined.Broadcast()
		c.mtxAllServersJoined.Unlock()

		// Copy server chain to serverChainTemp
		c.mtxServerChainTemp.Lock()
		for e := c.serverChain.Front(); e != nil; e = e.Next() {
			c.serverChainTemp.PushBack(e.Value.(ServerInfo))
		}
		c.mtxServerChainTemp.Unlock()

		// Trace action for AllServersJoined
		c.ctrace.RecordAction(AllServersJoined{})

		// Start monitoring all servers with fchecker
		c.startMonitoringAllServers()

		log.Println("Chain is now complete, coord is ready to serve get " +
			"tail/head requests and notify about failures")
	}

	return nil
}

func (c *CoordRPC) startMonitoringAllServers() {
	// Start monitoring all servers with fchecker
	log.Println("Starting to monitor all servers with fchecker")
	for i := uint8(1); i <= c.numServers; i++ {
		err := fchecker.AddNodeToMonitor(fchecker.MonitorNodeStruct{EpochNonce: uint64(rand.Int63()),
			HBeatLocalIPHBeatLocalPort: c.fcheckIP, HBeatRemoteIPHBeatRemotePort: c.allServers[i].FcheckAddr,
			LostMsgThresh: c.lostMsgsThresh})
		if err != nil {
			log.Println("Error starting monitoring of server:", i)
		}
		log.Println("Started monitoring of server:", i)
	}
	log.Println("All servers being monitored with fchecker")
}

func (c *Coord) handleFcheckerNotifications() {
	// Handle fchecker notifications
	for {
		select {
		case notification := <-c.fcheckNotifyCh:
			c.mtxServerChainTemp.Lock()
			// Find serverId of server that failed
			failedServerId := uint8(0)
			c.mtxAllServers.Lock()
			for i := uint8(1); i <= c.numServers; i++ {
				if c.allServers[i].FcheckAddr == notification.UDPIpPort {
					failedServerId = i
					log.Println("Detected failed server:", failedServerId)
					copyServer := c.allServers[i]
					copyServer.Failed = true
					c.allServers[i] = copyServer
					c.mtxAllServers.Unlock()
					break
				}
			}

			// Trace action for ServerFailed
			c.ctrace.RecordAction(ServerFail{failedServerId})

			// Kill process if all servers failed (theoretically impossible, at most N-1 server fails)
			if c.serverChainTemp.Len() == 1 {
				log.Println("All servers failed, killing process (theoretically impossible, " +
					"at most N-1 server fails)")
				os.Exit(1)
			}

			// Create a snapshot (chain copy) of chain before removing failed server
			oldChain := list.New()
			for e := c.serverChainTemp.Front(); e != nil; e = e.Next() {
				oldChain.PushBack(e.Value.(ServerInfo))
			}

			// Remove failed server from global temp chain
			for e := c.serverChainTemp.Front(); e != nil; e = e.Next() {
				if e.Value.(ServerInfo).ServerId == failedServerId {
					c.serverChainTemp.Remove(e)
					break
				}
			}
			c.mtxServerChainTemp.Unlock()

			// Asynchronously notify all servers about failed server
			go c.serverFailedHandler(failedServerId, oldChain)

		}
	}
}

func (c *Coord) serverFailedHandler(failedServerId uint8, oldChain *list.List) {
	// Notify adjacent servers that server failed
	success := c.notifyAdjacentServers(failedServerId, oldChain)

	// When notifyAdjacentServers returns, the chain is updated to reflect the failed server
	if success {
		c.removeServerFromChain(failedServerId, oldChain)
		log.Println("Broadcasting about new chain!")
		c.condServerChain.Broadcast()
	}
}

func (c *Coord) notifyAdjacentServers(failedServerId uint8, oldChain *list.List) bool {
	serverReconfiguredChan := make(chan bool)
	numServersReconfiguring := uint8(0)
	var failedServer *list.Element
	// Dummy server to use when failed server is head/tail
	dummyServer := ServerInfo{
		ServerId:         0,
		ServerAddr:       "",
		ServerListenAddr: "",
		ClientListenAddr: "",
		FcheckAddr:       "",
		Joined:           false,
	}

	// Find failed server in chain
	for e := oldChain.Front(); e != nil; e = e.Next() {
		if e.Value.(ServerInfo).ServerId == failedServerId {
			failedServer = e
			break
		}
	}

	// Notify adjacent servers
	if failedServer == oldChain.Front() {
		// Case 1: Failed server is first server in chain
		numServersReconfiguring = 1
		// Notify next server
		serverToNotify := failedServer.Next().Value.(ServerInfo)
		go c.notifyServerRetrier(serverToNotify, dummyServer, failedServerId, serverReconfiguredChan)
	} else if failedServer == oldChain.Back() {
		// Case 2: Failed server is last server in chain
		numServersReconfiguring = 1
		// Notify previous server
		serverToNotify := failedServer.Prev().Value.(ServerInfo)
		go c.notifyServerRetrier(serverToNotify, dummyServer, failedServerId, serverReconfiguredChan)
	} else {
		// Case 3: Failed server is in middle of chain
		numServersReconfiguring = 2
		// Notify previous server
		serverToNotify := failedServer.Prev().Value.(ServerInfo)
		newServer := failedServer.Next().Value.(ServerInfo)
		go c.notifyServerRetrier(serverToNotify, newServer, failedServerId, serverReconfiguredChan)
		// Notify next server
		serverToNotify = failedServer.Next().Value.(ServerInfo)
		newServer = failedServer.Prev().Value.(ServerInfo)
		go c.notifyServerRetrier(serverToNotify, newServer, failedServerId, serverReconfiguredChan)
	}

	// Wait for all servers to reconfigure, listening through channel
	bothServersReconfigured := true
	for i := uint8(0); i < numServersReconfiguring; i++ {
		success := <-serverReconfiguredChan
		if !success {
			bothServersReconfigured = false
		}
	}

	if bothServersReconfigured {
		// All servers reconfigured
		log.Println("Adjacent server(s) successfully reconfigured after failed server:", failedServerId)
		return true
	} else {
		// Failed to reconfigure Adjacent server(s), they died or responded with success: false
		log.Println("Adjacent server(s) failed to reconfigure after failed server:", failedServerId,
			". One of them set Success flag in response to false or they died or the server they were "+
				"trying to connect to died. Stopping this reconfiguration.")
		return false
	}

}

func (c *Coord) notifyServerRetrier(serverToNotify ServerInfo, newServer ServerInfo, failedServerId uint8,
	serverReconfiguredChan chan bool) {
	// Check if server to notify is alive
	c.mtxAllServers.RLock()
	if c.allServers[serverToNotify.ServerId].Failed {
		// Server is dead, don't notify
		c.mtxAllServers.RUnlock()
		log.Println("Server", serverToNotify.ServerId, "died while telling it about server", failedServerId,
			"'s failure. Not notifying it anymore.")
		serverReconfiguredChan <- false
		return
	}
	c.mtxAllServers.RUnlock()

	// Check if the new server is alive
	c.mtxAllServers.RLock()
	if newServer.ServerId != 0 && c.allServers[newServer.ServerId].Failed {
		// New server is dead, don't notify
		c.mtxAllServers.RUnlock()
		log.Println("New server", newServer.ServerId, "died while telling server", serverToNotify.ServerId,
			"to connect to it. Not notifying it anymore.")
		serverReconfiguredChan <- false
		return
	}
	c.mtxAllServers.RUnlock()

	// Send request to adjacent server
	err := c.notifyServer(serverToNotify, newServer, failedServerId, serverReconfiguredChan)
	if err != nil {
		// Connection error, retry
		time.Sleep(250 * time.Millisecond)
		c.notifyServerRetrier(serverToNotify, newServer, failedServerId, serverReconfiguredChan)
	} else {
		// No connection error, server reconfigured or replied with no success, stop retrying
		return
	}
}

func (c *Coord) notifyServer(serverToNotify ServerInfo, newServer ServerInfo, failedServerId uint8,
	serverReconfiguredChan chan bool) error {
	log.Println("Notifying server", serverToNotify.ServerId, "to connect to server", newServer.ServerId,
		"after server", failedServerId, "failed.")

	// Dial RPC connection to server
	server, err := rpc.Dial("tcp", serverToNotify.CoordListenAddr)
	if err != nil {
		log.Println("Failed to notify server", serverToNotify.ServerId, "that server", failedServerId,
			"failed. Dialing error:", err, ". Retrying...")
		return err
	}

	// Send RPC call to server
	var reply *FailedServerAck
	args := FailedServerNotification{
		FailedServerId: failedServerId,
		NewServerId:    newServer.ServerId,
		NewServerAddr:  newServer.ServerListenAddr,
		Token:          c.ctrace.GenerateToken(),
	}
	err = server.Call("ServerRPC.FailedServerNotify", args, &reply)
	if err != nil {
		log.Println("Failed to notify server", serverToNotify.ServerId, "that server", failedServerId,
			"failed. RPC Call error:", err, ". Retrying...")
		return err
	}

	// Check if server responded with success
	if !reply.Success {
		log.Println("Server", serverToNotify.ServerId, "replied that it failed to connect to server",
			newServer.ServerId, "to handle server", failedServerId, "failure (Success:", reply.Success,
			") Stopping retrying.")
		serverReconfiguredChan <- false
		return nil
	}

	// Read trace token from reply
	trace := c.ctracer.ReceiveToken(reply.Token)

	// Trace action for ServerFailHandledRecvd
	trace.RecordAction(ServerFailHandledRecvd{reply.FailedServerId, reply.ServerId})

	// Close RPC connection
	server.Close()

	// Notify that server has reconfigured
	log.Println("Server", serverToNotify.ServerId, "replied that it successfully handled server",
		failedServerId, "failure.", "Connected to server", newServer.ServerId, "...")
	serverReconfiguredChan <- true
	return nil
}

func (c *Coord) removeServerFromChain(failedServerId uint8, oldChain *list.List) {
	c.mtxServerChain.Lock()
	// Find server id of adjacent servers
	prevServerId := uint8(0)
	nextServerId := uint8(255)
	for e := oldChain.Front(); e != nil; e = e.Next() {
		if e.Value.(ServerInfo).ServerId == failedServerId {
			if e.Prev() != nil {
				prevServerId = e.Prev().Value.(ServerInfo).ServerId
			}
			if e.Next() != nil {
				nextServerId = e.Next().Value.(ServerInfo).ServerId
			}
			break
		}
	}

	// Find server ids that fall between adjacent servers
	serversToRemove := make([]uint8, 0)
	for e := c.serverChain.Front(); e != nil; e = e.Next() {
		if e.Value.(ServerInfo).ServerId > prevServerId && e.Value.(ServerInfo).ServerId < nextServerId {
			serversToRemove = append(serversToRemove, e.Value.(ServerInfo).ServerId)
		}
	}

	// Remove servers from chain
	for _, serverId := range serversToRemove {
		for e := c.serverChain.Front(); e != nil; e = e.Next() {
			if e.Value.(ServerInfo).ServerId == serverId {
				c.serverChain.Remove(e)
				break
			}
		}
	}

	// Trace new chain
	traceNewChain(c.serverChain, c.ctrace)

	c.mtxServerChain.Unlock()
}

func (c *CoordRPC) getHeadServer() ServerInfo {
	headServer := c.serverChain.Front().Value.(ServerInfo)
	// If head server is dead, wait for an alive one
	for c.allServers[headServer.ServerId].Failed {
		log.Println("Head server", headServer.ServerId, "is dead. Waiting for an alive one...")
		c.condServerChain.Wait()
		log.Println("Unlocking... New chain! Let's check the new head.")
		headServer = c.serverChain.Front().Value.(ServerInfo)
	}
	log.Println("Head server", headServer.ServerId, "is alive.")
	return headServer
}

func (c *CoordRPC) getTailServer() ServerInfo {
	time.Sleep(100 * time.Millisecond)
	tailServer := c.serverChain.Back().Value.(ServerInfo)
	// If tail server is dead, wait for an alive one
	for c.allServers[tailServer.ServerId].Failed {
		log.Println("Tail server", tailServer.ServerId, "is dead. Waiting for an alive one...")
		c.condServerChain.Wait()
		log.Println("Unlocking... New chain! Let's check the new tail.")
		tailServer = c.serverChain.Back().Value.(ServerInfo)
	}
	log.Println("Tail server", tailServer.ServerId, "is alive.")
	return tailServer
}

func traceNewChain(serverChain *list.List, trace *tracing.Trace) {
	// Trace new chain
	newChain := make([]uint8, serverChain.Len())
	for i, e := 0, serverChain.Front(); e != nil; i, e = i+1, e.Next() {
		newChain[i] = e.Value.(ServerInfo).ServerId
	}
	trace.RecordAction(NewChain{newChain})

	// Print new chain nicely to console
	ChainString := "Server chain: head -> "
	for e := serverChain.Front(); e != nil; e = e.Next() {
		ChainString += fmt.Sprintf("%d", e.Value.(ServerInfo).ServerId)
		if e.Next() != nil {
			ChainString += " -> "
		}
	}
	ChainString += " <- tail"
	log.Println(ChainString)
}
