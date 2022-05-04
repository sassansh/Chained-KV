package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
)

func main() {
	fmt.Println(4)
	var configName = fmt.Sprintf("config/server_config%d.json", 4)
	var config chainedkv.ServerConfig
	util.ReadJSONConfig(configName, &config)
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	server := chainedkv.NewServer()
	server.Start(config.ServerId, config.CoordAddr, config.ServerAddr, config.ServerServerAddr, config.ServerListenAddr, config.ClientListenAddr, stracer)
}

/*
err ← Start( serverId uint8, coordAddr string, serverAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer )
When successful, this call should not return.
The server node should run until the program is terminated.
This call should return an error if there are networking issues (e.g., incorrect ip:port given), or other unrecoverable issues.

- serverId is the identity of this server. It is a unique number that is between 1 and N, the total number of servers in the system.
- coordAddr is the IP:port of the coord node that this server node should connect to.
- serverAddr is the local IP:port that this server node should use to connect to the coord node.
- serverListenAddr is the local IP:port that this server node should use to receive connections from other (i.e., chain adjacent) servers.
- clientListenAddr is the local IP:port that this server node should use to receive connections from clients.
- strace is a server tracer instance; use this
(1) create a new distributed trace instance using Tracer.CreateTrace(), and
(2) record local server actions in the created trace that are not associated with any particular client request (see below)
*/

//type ResultStruct struct {
//	opId   uint32
//	gId    uint64
//	result string
//}

/*
If a server in the middle of the chain fails, then the servers that are adjacent to the failed server should
(1) learn about one another via the coord node (see diagram above), and
(2) exchange relevant KV state.
For example, given a chain S1 → S2 → S3, the failure of S2 may lose put operations that have been forwarded from
S1 to S2 but have not yet made it to S3 when S2 failed. These put operations must be resent by S1 to S3.
*/
