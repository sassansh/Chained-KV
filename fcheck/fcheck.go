/*

This package specifies the API to the failure checking library to be
used in assignment 2 of UBC CS 416 2021W2.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Stop, but you cannot
change its API.

*/

package fchecker

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
)
import "time"

////////////////////////////////////////////////////// DATA
// Define the message types fchecker has to use to communicate to other
// fchecker instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fchecker instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

////////////////////////////////////////////////////// API

type MonitorNodeStruct struct {
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
	LostMsgThresh                uint8
}

////////////////////////////////////////////////////// Helper Structs

type AckState struct {
	conn   *net.UDPConn
	stopCh chan string
}

type MonitoredNodeState struct {
	conn   *net.UDPConn
	stopCh chan string
}

type SentHBeat struct {
	message  HBeatMessage
	timeSent time.Time
	RTT      time.Duration
	acked    bool
}

////////////////////////////////////////////////////// Global Variables

var globalNotifyCh chan FailureDetected
var ackState AckState
var monitoredNodes map[string]MonitoredNodeState
var initialized = false

// Starts the fcheck library.

func Initialize() (chan FailureDetected, error) {
	if initialized {
		return nil, errors.New("fcheck already initialized")
	}

	initialized = true

	// Initialize the globalNotifyCh.
	globalNotifyCh = make(chan FailureDetected)

	// Initialize the monitoredNodes.
	monitoredNodes = make(map[string]MonitoredNodeState)

	log("fcheck initialized")

	return globalNotifyCh, nil
}

func StartAcknowledging(ackLocalIPAckLocalPort string) error {
	if !initialized {
		return errors.New("fcheck not initialized")
	}

	// Basic error checking
	if ackLocalIPAckLocalPort == "" {
		return errors.New("no AckLocalIPAckLocalPort specified")
	}
	if ackState.conn != nil {
		return errors.New("already acknowledging heartbeats on: " +
			ackState.conn.LocalAddr().String() + " call Stop() first")
	}

	// Parse local IP:port to ack from
	laddrAck, err := net.ResolveUDPAddr("udp", ackLocalIPAckLocalPort)
	if err != nil {
		return err
	}

	// Create a UDP connection to send acks from
	connAck, err := net.ListenUDP("udp", laddrAck)
	if err != nil {
		return err
	}

	// Create the stop channel.
	stopCh := make(chan string)

	// Create the ackState.
	ackState = AckState{conn: connAck, stopCh: stopCh}

	// Start the ack goroutine.
	go startAcking()
	log("Responding to heartbeats on:", connAck.LocalAddr().String())

	return nil
}

func AddNodeToMonitor(arg MonitorNodeStruct) error {
	if !initialized {
		return errors.New("fcheck not initialized")
	}

	// Basic error checking
	if arg.HBeatLocalIPHBeatLocalPort == "" {
		return errors.New("no HBeatLocalIPHBeatLocalPort specified")
	}
	if arg.HBeatRemoteIPHBeatRemotePort == "" {
		return errors.New("no HBeatRemoteIPHBeatRemotePort specified")
	}
	if arg.LostMsgThresh < 1 {
		return errors.New("LostMsgThresh must be > 0")
	}

	// Parse local IP:port to send from
	laddr, err := net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)
	if err != nil {
		return err
	}

	// Parse remote IP:port to send to
	raddr, err := net.ResolveUDPAddr("udp", arg.HBeatRemoteIPHBeatRemotePort)
	if err != nil {
		return err
	}

	// Create a UDP connection to send heartbeats to
	connHBeat, err := net.DialUDP("udp", laddr, raddr)
	if err != nil {
		return err
	}

	// Create the HBeatState.
	monitoredNodes[raddr.String()] = MonitoredNodeState{
		conn:   connHBeat,
		stopCh: make(chan string),
	}

	// Start the HBeat goroutine.
	go monitorNode(raddr.String(), arg.EpochNonce, arg.LostMsgThresh)
	log("Monitoring node " + connHBeat.RemoteAddr().String() + " from " + connHBeat.LocalAddr().String())

	return nil
}

func monitorNode(remoteAddr string, epochNonce uint64, lostMsgThresh uint8) {
	averageRTT := time.Millisecond * 3000
	lostMsgCount := uint8(0)
	HBeatsSent := make([]SentHBeat, 0)
	stopChannel := monitoredNodes[remoteAddr].stopCh
	conn := monitoredNodes[remoteAddr].conn
	seqNum := uint64(0)
	var lastSentHBeatTime time.Time

	log("Average RTT for {", conn.RemoteAddr(), "}:", averageRTT)

	for {
		select {
		case <-stopChannel:
			return
		default:
			// Check for timeout
			timeoutOccurred := false
			if len(HBeatsSent) > 0 && time.Since(lastSentHBeatTime) > averageRTT {
				timeoutOccurred = true
				lostMsgCount++

				log("Timeout occurred, lostMsgCount:", lostMsgCount)

				// If too many messages have been lost, send a failure notification and stop monitoring
				if lostMsgCount >= lostMsgThresh {
					log("LostMsgCount(", lostMsgCount, ") >= LostMsgThresh(", lostMsgThresh,
						"), sending failure notification")
					globalNotifyCh <- FailureDetected{
						UDPIpPort: conn.RemoteAddr().String(),
						Timestamp: time.Now(),
					}
					remoteNodeAddr := conn.RemoteAddr().String()
					close(stopChannel)
					conn.Close()
					delete(monitoredNodes, remoteNodeAddr)
					log("Stopped monitoring remote node: ", remoteNodeAddr)
					return
				}
			}

			// Check for heartbeat response
			matchingHBeatFound := false
			if len(HBeatsSent) > 0 {
				recvBuf := make([]byte, 1024)
				conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
				len, err := conn.Read(recvBuf)
				if err == nil {
					// Decode the response
					receivedAck, err := decodeAck(recvBuf, len)
					if err != nil {
						log("Error decoding AckMessage:", err)
						if timeoutOccurred {
							lostMsgCount--
						}
						continue
					}

					// Check EpochNonce matches
					if receivedAck.HBEatEpochNonce == epochNonce {
						// Find matching sent heartbeat and update RTT
						for _, hb := range HBeatsSent {
							if hb.message.SeqNum == receivedAck.HBEatSeqNum {
								log(time.Now().Format("15:04:05.000000"), "<-- Received Ack:",
									receivedAck, "from", conn.RemoteAddr())
								matchingHBeatFound = true
								lostMsgCount = 0

								// Update RTT if first ack received for this Heartbeat
								if !hb.acked {
									log("Updating RTT for {", conn.RemoteAddr(), "}: (", averageRTT,
										" + ", time.Since(hb.timeSent), ") / 2 = ",
										(averageRTT+time.Since(hb.timeSent))/2)
									averageRTT = (averageRTT + time.Since(hb.timeSent)) / 2
								}
								hb.acked = true

								// If response came early, sleep for the difference
								ExtraSleepTime := time.Until(lastSentHBeatTime.Add(hb.RTT)) - time.Millisecond*50
								if ExtraSleepTime > 0 {
									time.Sleep(ExtraSleepTime)
								}
							}
						}
					}
					// No matching heartbeat found
					if !matchingHBeatFound {
						log("Received unexpected AckMessage:", receivedAck)
					}
				}
			}

			// Send a heartbeat if timeout, matching heartbeat found, or heartbeat never sent,
			// meaning no heartbeat on the wire
			if timeoutOccurred || matchingHBeatFound || len(HBeatsSent) == 0 {
				// Create a new heartbeat
				HBeatMessage := HBeatMessage{
					EpochNonce: epochNonce,
					SeqNum:     seqNum,
				}
				// Send the heartbeat
				_, err := conn.Write(encodeHBeat(HBeatMessage))
				if err != nil {
					log("Error sending heartbeat:", err)
					if timeoutOccurred {
						lostMsgCount--
					}
					continue
				}
				// Record heartbeat in array
				lastSentHBeatTime = time.Now()
				hBeatSent := SentHBeat{
					message:  HBeatMessage,
					timeSent: time.Now(),
					RTT:      averageRTT,
				}
				HBeatsSent = append(HBeatsSent, hBeatSent)
				log(hBeatSent.timeSent.Format("15:04:05.000000"), "--> Sent Heartbeat", HBeatMessage,
					"to", conn.RemoteAddr())

				seqNum++
			}
		}
	}
}

func startAcking() {
	recvBuf := make([]byte, 1024)

	for {
		select {
		case <-ackState.stopCh:
			return
		default:
			// Receive a heartbeat
			ackState.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100)) // TODO: What timeout to use?
			len, raddr, err := ackState.conn.ReadFromUDP(recvBuf)
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					// Timeout, continue
					continue
				} else {
					// Some other error
					log("Error reading HeartBeat from UDP connection:", err)
					continue
				}
			}

			// Decode the heartbeat
			receivedHBeat, err := decodeHBeat(recvBuf, len)
			if err != nil {
				log("Error decoding HeartBeat:", err)
				continue
			}
			log(time.Now().Format("15:04:05.000000"), "<-- Received HeartBeat:",
				receivedHBeat, "from", raddr)

			time.Sleep(time.Millisecond * 200) // TODO: MAKE SURE TO REMOVE THIS FOR SUBMISSION

			// Send an ack
			ackMsg := AckMessage{
				HBEatEpochNonce: receivedHBeat.EpochNonce,
				HBEatSeqNum:     receivedHBeat.SeqNum,
			}
			_, err = ackState.conn.WriteTo(encodeAck(ackMsg), raddr)
			if err != nil {
				log("Error writing Ack to UDP connection:", err)
				continue
			}
			log(time.Now().Format("15:04:05.000000"), "--> Sent Ack", ackMsg,
				"to", raddr)
		}
	}
}

////////////////////////////////////////////////////// Acking Helper Functions

func decodeHBeat(buf []byte, len int) (HBeatMessage, error) {
	var decodedHBeat HBeatMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decodedHBeat)
	if err != nil {
		return HBeatMessage{}, err
	}
	return decodedHBeat, nil
}

func encodeAck(ackMessage AckMessage) []byte {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(ackMessage)
	if err != nil {
		log("Error encoding AckMessage:", err)
		return nil
	}
	return buf.Bytes()
}

func GetAckIpPort() string {
	return ackState.conn.LocalAddr().String()
}

////////////////////////////////////////////////////// HBeat Helper Functions

func encodeHBeat(HBMessage HBeatMessage) []byte {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(HBMessage)
	if err != nil {
		log("Error encoding HBeatMessage:", err)
		return nil
	}
	return buf.Bytes()
}

func decodeAck(buf []byte, len int) (AckMessage, error) {
	var decodedAck AckMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decodedAck)
	if err != nil {
		return AckMessage{}, err
	}
	return decodedAck, nil
}

// Tell the library to stop monitoring specified node
func StopMonitoringNode(NodeIpPort string) {
	// Find the node in the list
	for remoteAddr, remoteNode := range monitoredNodes {
		if remoteAddr == NodeIpPort {
			// Send a stop message to the node goroutine and close the channel
			remoteNode.stopCh <- "stop"
			close(remoteNode.stopCh)

			// Close the connection
			remoteNode.conn.Close()

			// Remove the node from the list
			delete(monitoredNodes, remoteAddr)

			log("Stopped monitoring: ", remoteAddr)
			return
		}
	}
	log("Error: Node to stop monitoring not found:", NodeIpPort)
}

// Tell the library to stop monitoring all nodes
func StopMonitoringAllNodes() {
	// Stop monitoring remote nodes
	for remoteAddr, remoteNode := range monitoredNodes {
		// Send a stop message to the node goroutine and close the channel
		remoteNode.stopCh <- "stop"
		close(remoteNode.stopCh)

		// Close the connection
		remoteNode.conn.Close()

		// Remove the node from the list
		delete(monitoredNodes, remoteAddr)
		log("Stopped monitoring: ", remoteAddr)
	}

	return
}

// Tell the library to stop responding acks.
func StopAcknowledging() {
	// Stop responding to heartbeats
	if ackState.conn != nil {
		heartBeatRespondAddr := ackState.conn.LocalAddr().String()

		// Send a stop message to the goroutine and close the channel
		ackState.stopCh <- "stop"
		close(ackState.stopCh)

		// Close the connection
		ackState.conn.Close()
		ackState.conn = nil

		log("Stopped responding to heartbeats on: ", heartBeatRespondAddr)
	}

	return
}

// Tell the library to stop monitoring/responding acks.
func StopAll() {
	// Stop responding to heartbeats
	StopAcknowledging()

	// Stop monitoring remote nodes
	StopMonitoringAllNodes()

	return
}

// Log print function
func log(v ...interface{}) {
	// Flag to print to logs
	printToStdout := false

	// If the printToStdout flag is set, print to stdout
	if printToStdout {
		fmt.Println(v...)
	}
}
