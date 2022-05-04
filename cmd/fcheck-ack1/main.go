/*

A trivial application to illustrate how the fcheck library can be used
in assignment 2 for UBC CS 416 2021W2.

Usage:
	go run cmd/fcheck-ack1/main.go
or:
	make ack1
	./bin/ack1
*/

package main

// Expects fcheck.go to be in the ./fcheck/ dir, relative to
// this fcheck-client.go file
import (
	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"fmt"
	"os"
	"time"
)

func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8082"

	// Initialize the fchecker
	fchecker.Initialize()

	// Start acknowledging
	fchecker.StartAcknowledging(localIpPort)

	// Respond to heartbeats, wait for fcheck to stop.
	select {
	case <-time.After(100000 * time.Second):
		// case <-time.After(time.Second):
		fchecker.StopAll()
	}
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
