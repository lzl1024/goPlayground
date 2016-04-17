package lsp

import (
	"github.com/cmu440/lspnet"
)

// class for all endpoint
type endPoint struct {
	currentEpoch int
	Params
	conn *lspnet.UDPConn
	// buffered global channel
	globalMsgIncomingChan chan *Message // chan to get message from window
	globalMsgOutGoingChan chan *Message // chan to send out message to user
	readRequestChan       chan bool     // chan to rcv read request
	globalBuffedMsg       []*Message

	//close info
	endPointClose     bool
	endPointCloseChan chan bool
}

const (
	GLOBAL_BUFF_LEN = 150
)

func newEndPoint(params Params, conn *lspnet.UDPConn) *endPoint {
	e := &endPoint{
		currentEpoch: 0,
		Params:       params,
		conn:         conn,
		globalMsgIncomingChan: make(chan *Message),
		globalMsgOutGoingChan: make(chan *Message),
		globalBuffedMsg:       make([]*Message, 0, GLOBAL_BUFF_LEN),
		readRequestChan:       make(chan bool),
		endPointClose:         false,
		endPointCloseChan:     make(chan bool),
	}

	go e.globalBuffListener()

	return e
}

// handle buffed msg send-in and send-out, a small state machine
func (e *endPoint) globalBuffListener() {
	// read is not blocking first
	readBlocking := false
	for {
		select {
		case <-e.readRequestChan:
			// if there is someting in the buffer, give to it
			if len(e.globalBuffedMsg) > 0 {
				e.globalMsgOutGoingChan <- e.globalBuffedMsg[0]
				e.globalBuffedMsg = e.globalBuffedMsg[1:]
				readBlocking = false
			} else {
				// if nothing in the buffer, block the read
				readBlocking = true
			}
		case msg, ok := <-e.globalMsgIncomingChan:
			if !ok {
				return
			}
			// if global out if blocking, send to it
			if readBlocking {
				e.globalMsgOutGoingChan <- msg
				readBlocking = false
			} else {
				// if global out is not blocking, add to buffer
				e.globalBuffedMsg = append(e.globalBuffedMsg, msg)
			}
		case <-e.endPointCloseChan:
			return
		}
	}
}

// close end point channels
func (e *endPoint) close() {
	close(e.endPointCloseChan)
	e.conn.Close()
	close(e.globalMsgIncomingChan)
	close(e.globalMsgOutGoingChan)
	close(e.readRequestChan)
}
