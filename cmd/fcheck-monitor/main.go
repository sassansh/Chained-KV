/*

A trivial application to illustrate how the fcheck library can be used
in assignment 2 for UBC CS 416 2021W2.

Usage:
	go run cmd/fcheck-monitor/main.go
or:
	make monitor
	./bin/monitor
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
	// localIpPort := "127.0.0.1:8081"

	var epochNonce = uint64(1337)

	// Monitor for a remote node.
	localIpPortMon := "127.0.0.1:0"
	toMonitorIpPort := "127.0.0.1:8080"
	toMonitorIpPort2 := "127.0.0.1:8082"
	var lostMsgThresh uint8 = 2

	// Initialize the fchecker
	notifyCh, err := fchecker.Initialize()
	if checkError(err) != nil {
		return
	}

	// Add first node to monitor
	err = fchecker.AddNodeToMonitor(fchecker.MonitorNodeStruct{epochNonce,
		localIpPortMon, toMonitorIpPort, lostMsgThresh})
	if checkError(err) != nil {
		return
	}

	// Add second node to monitor
	err = fchecker.AddNodeToMonitor(fchecker.MonitorNodeStruct{epochNonce,
		localIpPortMon, toMonitorIpPort2, lostMsgThresh})
	if checkError(err) != nil {
		return
	}

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	select {
	case notify := <-notifyCh:
		fmt.Println("Detected a failure of", notify)
		return
	case <-time.After(time.Duration(int(lostMsgThresh)*20) * time.Second):
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
