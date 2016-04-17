// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

//var logger = log.New(logFile, "Server-- ", log.Lmicroseconds|log.Lshortfile)
type client struct {
	*endPoint
	*lspConn
}

const (
	RETRY_TIME_AFTER_FAIL_MS = 500
)

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	// build connection
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

    // create client
	c := &client{
		endPoint: newEndPoint(*params, conn),
	}
	c.lspConn = newLspConn(c.endPoint, addr, 0, conn, params.WindowSize, 0, false)

	// start client service
	go c.handleRequests()
	go c.startEpochTask()

	success := <-c.connBuiltChan
	if !success {
		err = errors.New("Failed to build connection with Server")
	}

	return c, err
}

// start epoch task. Epoch task is a mechanism to timout connection
// if one client does not answering for couple of epoch task, connection
// will be terminated
func (c *client) startEpochTask() {
	tick := time.Tick(time.Duration(c.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-c.endPointCloseChan:
			return
		case <-tick:
			c.logger.Println("Start epoch...")
			if c.endPointClose {
				return
			}
			if c.currentEpoch-c.lastRcvEpoch >= c.EpochLimit {
				c.closeNow()
				c.closeEndpoint()
				return
			} else {
				c.epochHandler()
			}
			c.currentEpoch++
		}
	}
}

// rcv and handle all kinds of request
func (c *client) handleRequests() {
	buf := make([]byte, READ_BUF_LEN)
	c.logger.Println("Start listening")

	// send out connection msg
	msg := NewConnect()
	c.sendWindow.elements[0] = msg
	c.sendMsgChan <- msg

	for {
		n, err := c.endPoint.conn.Read(buf)
		if err != nil {
			c.logger.Println("Failed to read from UDP ", err.Error())
			time.Sleep(time.Duration(RETRY_TIME_AFTER_FAIL_MS) * time.Millisecond)
		} else {
			if c.endPointClose {
				return
			}
			msg := new(Message)
			json.Unmarshal(buf[0:n], msg)
			c.logger.Println("Rcv msg ", msg.String(), len(c.rcvMsgChan))
			c.lastRcvEpoch = c.currentEpoch
			c.rcvMsgChan <- msg
		}
	}
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
        // there is a read request
    c.readRequestChan <- true
	select {
	case msg, ok := <-c.globalMsgOutGoingChan:
		if !ok {
			return nil, errors.New("Connection Closed")
		}
		return msg.Payload, nil
	case <-c.endPointCloseChan:
		return nil, errors.New("Connection Closed")
	}
}

func (c *client) Write(payload []byte) error {
	c.sendOutMsg(payload)
	return nil
}

func (c *client) Close() error {
	c.closeFlag = true
	if c.isGoodCloseCondition() {
		c.closeNow()
	} else {
		// waiting for close signal
		<-c.closeChan
	}
	c.closeEndpoint()
	return nil
}

// close enpoint
func (c *client) closeEndpoint() {
	defer func() { c.closeLock.Unlock() }()
	c.closeLock.Lock()
	c.endPointClose = true
	select {
	// if it is closed already, directly return
	case <-c.endPointCloseChan:
		return
	default:
		c.endPoint.close()	
	}
}
