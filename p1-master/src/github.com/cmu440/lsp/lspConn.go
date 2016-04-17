package lsp

import (
	"encoding/json"
	"github.com/cmu440/lspnet"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	MSG_CHANN_BUFF = 150
)

// class for all lsp connection
type lspConn struct {
	//basic info
	addr   *lspnet.UDPAddr
	connId int
	conn   *lspnet.UDPConn

	// sending window
	sendWindowStartId   int
	sendWindow          *window
	sendWindowAckStatus *window
	lastSentId          int64 // last sent out id
	sendLock            sync.Mutex

	// rcv window
	rcvWindowStartId int
	rcvWindow        *window
	rcvLock          sync.Mutex
	lastInOrderRcvId int // lastest id can read by server

	// epoch
	lastRcvEpoch int

	// channgels
	rcvMsgChan  chan *Message
	sendMsgChan chan *Message
	bufferedMsg []*Message
	bufferLock  sync.Mutex

	// log
	logger *log.Logger

	parent        *endPoint
	isServer      bool
	connBuiltChan chan bool

	// close status
	closeFlag bool
	closeChan chan bool
	closeLock sync.Mutex
}

// util function to rcv upper layer's write request
func (c *lspConn) sendOutMsg(payload []byte) {
	msgId := int(atomic.AddInt64(&c.lastSentId, 1))
	msg := NewData(c.connId, msgId, payload)

	c.logger.Println("Create msg ", msg.String())
	// add to buffer message and send out
	c.sendLock.Lock()
	c.bufferLock.Lock()
	c.bufferedMsg = append(c.bufferedMsg, msg)
	c.checkAndSendBufferedWrite()
	c.bufferLock.Unlock()
	c.sendLock.Unlock()

}

/// send handler only responisble to send out msg asyncly.
func (c *lspConn) sendHandler() {
	for msg := range c.sendMsgChan {
		jsonMsg, _ := json.Marshal(msg)

		var err error = nil
		if c.isServer {
			_, err = c.conn.WriteToUDP(jsonMsg, c.addr)
			c.logger.Println("send msg to client", msg.String(), len(c.sendMsgChan))
		} else {
			_, err = c.conn.Write(jsonMsg)
			c.logger.Println("send msg to server", msg.String(), len(c.sendMsgChan))
		}
		if err != nil {
			c.logger.Println("Failed to write", err.Error())
		}
	}
}

// handle data and ack msg
func (c *lspConn) rcvHandler() {
	for msg := range c.rcvMsgChan {
		// handler for msg type
		switch msg.Type {
		case MsgData:
			c.dataHandler(msg)
		case MsgAck:
			c.ackHandler(msg)
		default:
			c.logger.Println("UNKNOWN type", msg.Type)
		}
	}
}

//handle data msg
func (c *lspConn) dataHandler(msg *Message) {
	newMsgIndex := 0
	c.rcvLock.Lock()
	defer func() { c.rcvLock.Unlock() }()

	// older message resent, ignore
	if msg.SeqNum <= c.rcvWindowStartId {
		c.logger.Println("Too old msg, Ingore, current rcvwindowStartId is", c.rcvWindowStartId, msg.SeqNum)
		return
	} else if msg.SeqNum > c.rcvWindowStartId+c.rcvWindow.size {
		// ahead of the window, window need to slide and handle

		// send ordered msg to server
		c.sendToGlobalRcvChan()
		if msg.SeqNum-c.lastInOrderRcvId > c.rcvWindow.size {
			c.logger.Fatalln("Some thing wrong with server window, we just in order: ", c.lastInOrderRcvId)
		}

		// sliding window
		slidingIndex := msg.SeqNum - c.rcvWindowStartId - c.rcvWindow.size
		c.rcvWindow.slidingAndAddBack(slidingIndex, msg)
		c.rcvWindowStartId += slidingIndex
		c.logger.Println("Sliding Window and new startId", slidingIndex, c.rcvWindowStartId)
		newMsgIndex = c.rcvWindow.size - 1
	} else {
		// in the range of window
		newMsgIndex = msg.SeqNum - c.rcvWindowStartId - 1
		c.rcvWindow.replace(newMsgIndex, msg)
	}
	// check current windows orderness
	c.sendToGlobalRcvChan()

	// send back ack
	ack := NewAck(c.connId, msg.SeqNum)
	c.sendMsgChan <- ack
}

// handle ack msg
func (c *lspConn) ackHandler(msg *Message) {
	c.sendLock.Lock()
	defer func() { c.sendLock.Unlock() }()
	// if connection build success, need to change logger and return build success
	c.specialFirstAckHandler(msg)
	// older message resent, ignore
	if msg.SeqNum <= c.sendWindowStartId {
		c.logger.Println("Too old msg, Ingore, current sendwindowStartId is", c.sendWindowStartId, msg.SeqNum)
		return
	} else if msg.SeqNum > c.sendWindowStartId+c.sendWindow.size {
		// ack in the future
		c.logger.Println("Get unknown ack, current sendWindowStartId is ", c.sendWindowStartId, msg.String())
		return
	} else {
		// inside buffer
		index := msg.SeqNum - c.sendWindowStartId - 1
		c.sendWindowAckStatus.elements[index] = msg
		// ordering ack to sliding window
		slideDist := c.slideSendingWindow()

		// check blocking write
		if slideDist > 0 {
			c.bufferLock.Lock()
			c.checkAndSendBufferedWrite()
			c.bufferLock.Unlock()
		}

		// check close condition
		if c.isGoodCloseCondition() {
			c.closeNow()
		}
	}
}

// first ack is to build connection and setup connId and return back to upper layer
func (c *lspConn) specialFirstAckHandler(msg *Message) {
	// get connID
	if c.connId == 0 && msg.SeqNum == 0 {
		c.connId = msg.ConnID
		curLogFile := logFile
		prefix := "Client-C-" + strconv.Itoa(msg.ConnID)
		if debugMode {
			curLogFile, _ = os.OpenFile(prefix+"log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
		}
		c.logger = log.New(curLogFile, prefix+" ", log.Lmicroseconds|log.Lshortfile)
		c.logger.Println("client Get new connID", msg.ConnID)
		c.connBuiltChan <- true
	}
}

// slide sending window and update the ack status because ordered ack comes
func (c *lspConn) slideSendingWindow() int {
	// how many ordered acked msg
	moveDist := 0
	for i := 0; i < c.sendWindowAckStatus.size; i++ {
		// break if no ack
		if c.sendWindowAckStatus.elements[i] == nil {
			break
		}
		moveDist++
	}
	c.sendWindowStartId += moveDist

	if moveDist > 0 {
		// sliding window since ordered acked
		c.sendWindow.sliding(moveDist)
		c.sendWindowAckStatus.sliding(moveDist)
		c.logger.Println("Sliding Window and new startId", moveDist, c.sendWindowStartId)
	}
	return moveDist
}

// if buffered msg can be written, write out
func (c *lspConn) checkAndSendBufferedWrite() {
	maxId := c.sendWindowStartId + c.sendWindow.size
	c.logger.Println("Window status", c.sendWindowStartId, c.sendWindow.size)
	var moveDist = 0
	for _, msg := range c.bufferedMsg {
		if msg.SeqNum <= maxId {
			index := msg.SeqNum - c.sendWindowStartId - 1
			c.sendWindow.elements[index] = msg
			c.sendMsgChan <- msg
			moveDist++
		}
	}

	c.logger.Println("buffer move", moveDist, len(c.bufferedMsg), c.sendWindow.elements[0])
	c.bufferedMsg = c.bufferedMsg[moveDist:]
}

// send the ordered message to rcv message, so upper layer user will be able to see
func (c *lspConn) sendToGlobalRcvChan() {
	for i := c.lastInOrderRcvId - c.rcvWindowStartId; i < c.rcvWindow.size; i++ {
		msg := c.rcvWindow.elements[i]
		if msg == nil {
			break
		}
		c.lastInOrderRcvId++
		c.parent.globalMsgIncomingChan <- msg
		c.logger.Println("Send msg for global read", msg.String())
	}
}

func newLspConn(parent *endPoint, addr *lspnet.UDPAddr, connId int, conn *lspnet.UDPConn, windowSize int, currentEpoch int, isServer bool) *lspConn {
	client := &lspConn{
		addr:                addr,
		connId:              connId,
		conn:                conn,
		sendWindowStartId:   -1,
		lastSentId:          0,
		sendWindow:          NewWindow(windowSize),
		sendWindowAckStatus: NewWindow(windowSize),
		rcvWindowStartId:    -1,
		rcvWindow:           NewWindow(windowSize),
		lastInOrderRcvId:    0,
		lastRcvEpoch:        currentEpoch,

		rcvMsgChan:    make(chan *Message, MSG_CHANN_BUFF),
		sendMsgChan:   make(chan *Message, MSG_CHANN_BUFF),
		bufferedMsg:   make([]*Message, 0),
		parent:        parent,
		isServer:      isServer,
		connBuiltChan: make(chan bool),
		closeFlag:     false,
		closeChan:     make(chan bool),
	}

	// first msg client will rcv is 1, otherwise server fisrt sent msg will be 1
	if isServer {
		client.sendWindowStartId = 0
		curLogFile := logFile
		prefix := "Server-C-" + strconv.Itoa(connId)
		if debugMode {
			curLogFile, _ = os.OpenFile(prefix+"log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
		}
		client.logger = log.New(curLogFile, prefix+" ", log.Lmicroseconds|log.Lshortfile)
	} else {
		client.rcvWindowStartId = 0
		client.logger = log.New(logFile, "Client-C-NEW ", log.Lmicroseconds|log.Lshortfile)
	}

	// start client's routine tasks
	go client.sendHandler()
	go client.rcvHandler()
	return client
}

//close right now
func (c *lspConn) closeNow() {
	defer func() { c.closeLock.Unlock() }()
	c.closeLock.Lock()
	select {
	// if it is closed already, directly return
	case <-c.closeChan:
		return
	default:
		c.logger.Println("Close Now", c.connId)
		close(c.rcvMsgChan)
		close(c.sendMsgChan)
		close(c.closeChan)
	}
}

// check if it is good to close everything now
func (c *lspConn) isGoodCloseCondition() bool {
	// basic info check
	if !c.closeFlag || len(c.bufferedMsg) > 0 || len(c.sendMsgChan) > 0 || len(c.rcvMsgChan) > 0 {
		return false
	}
	// check to see if every sent msg has been acked
	var lastSentId = int(c.lastSentId)
	// sendWindow is empty, all acked
	if lastSentId <= c.sendWindowStartId {
		return true
	}
	for _, msg := range c.sendWindowAckStatus.elements {
		// no ack, cannot close
		if msg == nil {
			return false
		} else if msg.SeqNum == lastSentId {
			return true
		}
	}
	return false
}

// handle epoch task, basically resend message from buffered window
func (c *lspConn) epochHandler() {
	c.sendLock.Lock()
	// For each data message that has been sent but not yet acknowledged, resend the data message.
	for index, msg := range c.sendWindow.elements {
		if c.sendWindowAckStatus.elements[index] == nil && msg != nil {
			c.sendMsgChan <- msg
		}
	}
	c.sendLock.Unlock()
	c.rcvLock.Lock()
	// Resend an acknowledgment message for each of the last ω (or possibly fewer) distinct data
	// messages that have been received.
	noRcv := true
	for _, msg := range c.rcvWindow.elements {
		if msg != nil {
			noRcv = false
			ackMsg := NewAck(c.connId, msg.SeqNum)
			c.sendMsgChan <- ackMsg
		}
	}

	// Just for client If the connection request has been sent and acknowledged, but no data messages have been received,
	// then send an acknowledgment with sequence number 0.
	if !c.isServer && c.rcvWindowStartId == 0 && noRcv {
		ackMsg := NewAck(c.connId, 0)
		c.sendMsgChan <- ackMsg
	}
	c.rcvLock.Unlock()
}
