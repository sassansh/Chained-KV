package chainedkv

import (
	"container/list"
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"cs.ubc.ca/cpsc416/a3/kvsshared"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"
)

type ServerStart struct {
	ServerId uint8
}

type ServerJoining struct {
	ServerId uint8
}

type NextServerJoining struct {
	NextServerId uint8
}

type NewJoinedSuccessor struct {
	NextServerId uint8
}

type ServerJoined struct {
	ServerId uint8
}

type ServerFailRecvd struct {
	FailedServerId uint8
}

type NewFailoverSuccessor struct {
	NewNextServerId uint8
}

type NewFailoverPredecessor struct {
	NewPrevServerId uint8
}

type ServerFailHandled struct {
	FailedServerId uint8
}

type PutRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwdRecvd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type ServerConfig struct {
	ServerId          uint8
	CoordAddr         string
	ServerAddr        string
	ServerServerAddr  string
	ServerListenAddr  string
	ClientListenAddr  string
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

type MyServerConfig struct {
	ServerId          uint8
	CoordAddr         string
	ServerAddr        string
	ServerServerAddr  string
	ServerListenAddr  string
	ClientListenAddr  string
	CoordListenAddr   string
	TracingServerAddr string
	fcheckAddr        string
	Secret            []byte
	TracingIdentity   string
}

type Server struct {
	data          Data
	syn           Syn
	prevServer    LinkedServer
	nextServer    LinkedServer
	config        MyServerConfig
	coord         *rpc.Client
	strace        *tracing.Tracer
	trace         *tracing.Trace
	errChannel    chan uint8
	isListRunning bool
}

type Data struct {
	valmap         map[string]string
	getReqs        chan rprq
	putReqs        chan rprq
	clientlastOpID map[string]uint32
	clientKVSMap   map[uint64]ServerMessage
	clientChanMap  map[uint64]chan bool
	getToRespond   *list.List
	gID            uint64
	processedGID   uint64
}

type ServerFailLatestDataIHave struct {
	Data     map[string]uint32
	ServerId uint8
}

type ServerFailDataSycResponse struct {
	Success bool
}

type Syn struct {
	gidMutex        sync.RWMutex
	failMutex       sync.RWMutex
	clientChanMutex sync.RWMutex
	opIDGroup       *CSyn
	kvsGroup        *CSyn
}

type CSyn struct {
	mutex sync.RWMutex
	cond  *sync.Cond
}

type LinkedServer struct {
	ServerID uint8
	Addr     string
	conn     *rpc.Client
}

type NextServerJoined struct {
	NextServer LinkedServer
	Token      tracing.TracingToken
}

type NextServerJoinedResponse struct {
	Success bool
	Token   tracing.TracingToken
}

type PutRequest struct {
	GID      uint64
	OPID     uint32
	Key      string
	Value    string
	KVSLibIP string
	Token    tracing.TracingToken
}

type PutResponse struct {
	Success bool
}

type GIDUpdate struct {
	GID     uint64
	success bool
}

type rprq struct {
	request  kvsshared.KVSMessage
	response *kvsshared.KVSMessage
	trace    *tracing.Trace
	done     chan bool
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {

	s.isListRunning = false
	s.data = Data{
		valmap:         make(map[string]string),
		clientlastOpID: make(map[string]uint32),
		clientKVSMap:   make(map[uint64]ServerMessage),
		clientChanMap:  make(map[uint64]chan bool),
		getToRespond:   list.New(),
		gID:            uint64(serverId) * uint64(math.Pow(2, 59)),
	}

	s.errChannel = make(chan uint8)
	st := &CSyn{}
	st.cond = sync.NewCond(&st.mutex)

	st2 := &CSyn{}
	st2.cond = sync.NewCond(&st2.mutex)
	s.syn.kvsGroup = st2

	st3 := &CSyn{}
	st3.cond = sync.NewCond(&st3.mutex)
	s.syn.opIDGroup = st3

	rpc.Register((*ServerRPC)(s))

	s.data.getReqs = make(chan rprq, 300000)
	s.data.putReqs = make(chan rprq, 300000)

	fmt.Println("here1")

	s.strace = strace
	s.prevServer = LinkedServer{
		ServerID: 0,
	}
	s.nextServer = LinkedServer{
		ServerID: 0,
	}
	s.config = MyServerConfig{
		ServerId:         serverId,
		CoordAddr:        coordAddr,
		ServerAddr:       serverAddr,
		ServerListenAddr: serverListenAddr,
		ClientListenAddr: clientListenAddr,
	}

	ip, _, err := net.SplitHostPort(s.config.ServerAddr)
	ip = fmt.Sprintf("%s:0", ip)

	fmt.Println("here2")
	_, err = s.setupRPCServer(ip)
	if err != nil {
		return err
	}

	laddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	raddr, err := net.ResolveTCPAddr("tcp", coordAddr)
	conn, err := net.DialTCP("tcp", laddr, raddr)
	conn.SetLinger(0)
	defer conn.Close()
	s.coord = rpc.NewClient(conn)

	fmt.Println("here3")

	coordListenAddr, err := s.setupRPCServer(serverListenAddr)
	s.config.CoordListenAddr = coordListenAddr

	_, err = s.setupRPCServer(clientListenAddr)
	fmt.Println("here4")
	s.trace = strace.CreateTrace()
	s.trace.RecordAction(ServerStart{ServerId: serverId})

	addr := make(chan string)
	go s.startFCheck(ip, addr)

	s.config.fcheckAddr = <-addr

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("here")
	go s.join()

	done := make(chan int, 1)
	<-done

	return errors.New("maybe")
}

//===================================================================================================================
//Setup

func (s *Server) setupRPCServer(addr string) (string, error) {
	tcpServerListenAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	serverListener, err := net.ListenTCP("tcp", tcpServerListenAddr)
	if err != nil {
		log.Fatal(err)
	}

	go rpc.Accept(serverListener)
	return tcpServerListenAddr.String(), err
}

func (s *Server) startFCheck(ip string, addr chan string) error {
	_, err := fchecker.Initialize()
	if err != nil {
		return err
	}

	fmt.Println("address:", ip)

	err = fchecker.StartAcknowledging(ip)
	if err != nil {
		fmt.Println(err)
		return err
	}

	addr <- fchecker.GetAckIpPort()

	done := make(chan int, 1)
	<-done
	return err
}

//===================================================================================================================

//===================================================================================================================
// Server Join Protocol

func (s *Server) join() {
	s.trace.RecordAction(ServerJoining{s.config.ServerId})

	serverJoinResponse, err := s.handleJoinCoordChainRequest()

	s.trace = s.strace.ReceiveToken(serverJoinResponse.Token)
	if serverJoinResponse.TailServerId == 0 {
		fmt.Println("I'm the head!")
		//This server is the head
	} else {
		s.handleJoinNewNext(serverJoinResponse)
	}

	s.trace.RecordAction(ServerJoined{s.config.ServerId})

	s.handleJoinCoordAck(err)
}

func (s *Server) handleJoinCoordAck(err error) {
	serverJoinedAck := ServerJoinedAck{
		ServerId: s.config.ServerId,
		Token:    s.trace.GenerateToken(),
	}

	var serverJoinedResponse ServerJoinedResponse

	err = s.coord.Call("CoordRPC.Joined", serverJoinedAck, &serverJoinedResponse)

	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) handleJoinCoordChainRequest() (ServerJoinResponse, error) {
	serverJoinRequest := ServerJoinRequest{
		ServerId:         s.config.ServerId,
		ServerAddr:       s.config.ServerAddr,
		ServerListenAddr: s.config.ServerListenAddr,
		ClientListenAddr: s.config.ClientListenAddr,
		FcheckAddr:       s.config.fcheckAddr,
		CoordListenAddr:  s.config.CoordListenAddr,
		Token:            s.trace.GenerateToken(),
	}

	var serverJoinResponse ServerJoinResponse

	err := s.coord.Call("CoordRPC.Join", serverJoinRequest, &serverJoinResponse)
	fmt.Println("After rpc")
	if err != nil {
		log.Fatal(err)
	}
	return serverJoinResponse, err
}

func (s *Server) handleJoinNewNext(serverJoinResponse ServerJoinResponse) {
	var prev = LinkedServer{
		ServerID: serverJoinResponse.TailServerId,
		Addr:     serverJoinResponse.TailServerAddr,
	}
	conn, err := rpc.Dial("tcp", prev.Addr)
	if err != nil {
		log.Fatal(err)
	}
	prev.conn = conn

	s.prevServer = prev

	request := NextServerJoined{
		NextServer: LinkedServer{
			ServerID: s.config.ServerId,
			Addr:     s.config.ServerListenAddr,
		},
		Token: s.trace.GenerateToken(),
	}

	var response NextServerJoinedResponse

	err = prev.conn.Call("ServerRPC.JoinNewNextRPC", request, &response)
	if err != nil {
		log.Fatal(err)
	}

	if response.Success {
		s.trace = s.strace.ReceiveToken(response.Token)
	}
	fmt.Println("Done setup prev")
}

//===================================================================================================================

//===================================================================================================================
func (s *Server) getGID() uint64 {
	s.syn.gidMutex.RLock()
	defer s.syn.gidMutex.RUnlock()
	return s.data.gID
}

func (s *Server) incrGID() uint64 {
	s.syn.gidMutex.Lock()
	defer s.syn.gidMutex.Unlock()
	s.data.gID++

	return s.data.gID
}

//===================================================================================================================

//===================================================================================================================
// Request Handling -- All Servers

//===================================================================================================================

//===================================================================================================================
// Request Handling -- Tail
