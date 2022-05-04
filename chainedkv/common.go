package chainedkv

import "github.com/DistributedClocks/tracing"

// Message structs that are sent between Coord and Server //////////////////////////////////////////

// FailedServerNotification sent from Coord to adjacent servers when failed server is detected
type FailedServerNotification struct {
	FailedServerId uint8
	NewServerId    uint8
	NewServerAddr  string
	Token          tracing.TracingToken
}

// FailedServerAck sent from Server to Coord when it has handled the FailedServerNotification
type FailedServerAck struct {
	Success        bool
	ServerId       uint8
	FailedServerId uint8
	Token          tracing.TracingToken
}

// ServerJoinRequest sent from Server to Coord when requesting to join the chain
type ServerJoinRequest struct {
	ServerId         uint8
	ServerAddr       string
	ServerListenAddr string
	ClientListenAddr string
	CoordListenAddr  string
	FcheckAddr       string
	Token            tracing.TracingToken
}

// ServerJoinResponse sent from Coord to Server when responding to ServerJoinRequest
type ServerJoinResponse struct {
	TailServerId   uint8
	TailServerAddr string
	Token          tracing.TracingToken
}

// ServerJoinAck sent from Server to Coord when it has joined the chain
type ServerJoinedAck struct {
	ServerId uint8
	Token    tracing.TracingToken
}

// ServerJoinedResponse sent from Coord to Server when responding to ServerJoinedAck
type ServerJoinedResponse struct {
	Success bool
}

type ServerMessage struct {
	ClientId            string
	GId                 uint64
	OpId                uint32
	Key                 string
	Value               string
	LatestSentPutId     uint32 // most recent put id that was issued from kvslib
	LatestSentGetId     uint32 // most recent get id that was issued from kvslib
	LatestReceivedPutId uint32
	LatestReceivedGetId uint32
	Token               tracing.TracingToken
	Trace               *tracing.Trace
}
