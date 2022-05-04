package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"time"
)

func main() {
	var config chainedkv.ClientConfig
	err := util.ReadJSONConfig("config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	notifCh, err := client.Start(tracer, config.ClientID, config.CoordIPPort, config.LocalCoordIPPort, config.LocalHeadServerIPPort, config.LocalTailServerIPPort, config.ChCapacity)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	// Put a key-value pair
	op, err := client.Put(tracer, config.ClientID, "key2", "value2")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)
	count := 1

	// Get a key's value
	for i := 1; i < 75; i++ {
		op, err := client.Put(tracer, config.ClientID, "key3", fmt.Sprintf("value%d", i))
		util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)

		op, err = client.Get(tracer, config.ClientID, "key3")
		util.CheckErr(err, "Error getting value %v, opId: %v\b", err, op)

		op, err = client.Get(tracer, config.ClientID, "key2")
		util.CheckErr(err, "Error getting value %v, opId: %v\b", err, op)
		count += 3

	}

	time.Sleep(3 * time.Second)

	for i := 0; i < count; i++ {
		result := <-notifCh
		log.Println(result)
	}

	client.Stop()
}
