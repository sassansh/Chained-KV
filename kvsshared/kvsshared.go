package kvsshared

import (
	"github.com/DistributedClocks/tracing"
)

////////////////////////////////////////////////////// Custom Structs

////////////////////////////////////////////////////// Shared with coord
// Coord receives this struct in RPC calls to Chain.GetHeadServer and Chain.GetTailServer
type ServerAddrReq struct {
	ClientId string
	Token    tracing.TracingToken
}

// Coord replies by setting the values of this struct
type ServerAddrResult struct {
	ServerAddress string
	ClientId      string
	ServerId      uint8
	Token         tracing.TracingToken
}

////////////////////////////////////////////////////// Shared with server
// Server receives this struct in RPC calls to ServerRPC.Put, ServerRPC.Get, and ServerRPC.GetPutResult
type KVSMessage struct {
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
}

// Server receives this struct in RPC calls to ServerRPC.
type LatestOpID struct {
	ClientId string
	OpId     uint32
}
