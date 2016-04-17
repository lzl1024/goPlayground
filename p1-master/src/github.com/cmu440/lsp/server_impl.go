// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var logFile, _ = os.OpenFile("Server-log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
var debugMode bool = true

const (
	READ_BUF_LEN = 1500
)

type server struct {
	*endPoint

	clientMap map[int]*lspConn
	addrCache map[string]int

	lastConnId     int
	logger         *log.Logger
	clientLostFlag bool
	clientMapLock  sync.Mutex

	// channel to get all close notification
	closeChan chan int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	// start server
	s := &server{
		clientMap:  make(map[int]*lspConn),
		endPoint:   newEndPoint(*params, conn),
		lastConnId: 0,
		addrCache:  make(map[string]int),
		logger:     log.New(logFile, "Server-- ", log.Lmicroseconds|log.Lshortfile),
		closeChan:  make(chan int),
	}

	// start server service
	go s.handleRequests()
	go s.startEpochTask()
	return s, nil
}

// start epoch task. Epoch task is a mechanism to timout connection
// if one client does not answering for couple of epoch task, connection
// will be terminated
func (s *server) startEpochTask() {
	tick := time.Tick(time.Duration(s.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-s.endPointCloseChan:
			return
		case <-tick:
			s.logger.Println("Start epoch...")
			if s.endPointClose {
				return
			}
			s.clientMapLock.Lock()
			for connID, client := range s.clientMap {
				// remove client, too long haven't hread from it
				if s.currentEpoch-client.lastRcvEpoch >= s.EpochLimit {
					client.closeNow()
					delete(s.clientMap, connID)
					s.clientLostFlag = true
					s.closeChan <- client.connId
				} else {
					client.epochHandler()
				}
			}

			s.clientMapLock.Unlock()
			s.currentEpoch++
		}
	}
}

// rcv and handle all kinds of request
func (s *server) handleRequests() {
	buf := make([]byte, READ_BUF_LEN)

	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			s.logger.Println("Failed to read from UDP ", err.Error())
			return
		}
		if s.endPointClose {
			return
		}

		msg := new(Message)
		json.Unmarshal(buf[0:n], msg)
		// add new client into map if addr is unique
		if msg.Type == MsgConnect {
			s.logger.Println("Rcv msg ", msg.String())
			s.addNewClient(addr, msg)
		} else {
			if client, ok := s.clientMap[msg.ConnID]; ok {
				client.logger.Println("Client Rcv msg ", msg.String(), len(client.rcvMsgChan))
				client.lastRcvEpoch = s.currentEpoch
				client.rcvMsgChan <- msg
			} else {
				s.logger.Println("Unknown connection sending message", msg)
			}
		}
	}
}

// add new client into the cache and start connection
func (s *server) addNewClient(addr *lspnet.UDPAddr, msg *Message) {
	addrString := addr.String()
	// check cache
	if id, ok := s.addrCache[addrString]; ok {
		s.logger.Println("Addr has been connected, dont add again", id)
	}

	// create client and update cache
	s.lastConnId++
	client := newLspConn(s.endPoint, addr, s.lastConnId, s.conn, s.WindowSize, s.currentEpoch, true)

	s.logger.Println("New client", s.lastConnId, addrString)
	s.clientMap[s.lastConnId] = client
	s.addrCache[addrString] = s.lastConnId

	// send connection ack back
	client.rcvWindow.elements[0] = msg
	ack := NewAck(client.connId, 0)
	client.sendMsgChan <- ack
}

func (s *server) Read() (int, []byte, error) {
    // there is a read request
    s.readRequestChan <- true
	select {
	case msg, ok := <-s.globalMsgOutGoingChan:
		if !ok {
			return 0, nil, errors.New("Connection Closed")
		}
		return msg.ConnID, msg.Payload, nil
	case <-s.endPointCloseChan:
		return 0, nil, errors.New("Connection Closed")

	case connId, ok := <-s.closeChan:
		if !ok {
			return 0, nil, errors.New("Connection Closed")
		}
		return connId, nil, errors.New("Connection Closed")
	}
}

func (s *server) Write(connID int, payload []byte) error {
	if client, ok := s.clientMap[connID]; !ok {
		s.logger.Println("Cannot find connID", connID)
		return errors.New("Cannot find connID")
	} else {
		client.sendOutMsg(payload)
	}
	return nil
}

func (s *server) CloseConn(connID int) error {
	if client, ok := s.clientMap[connID]; !ok {
		s.logger.Println("Cannot find connID", connID)
		return errors.New("Cannot find connID")
	} else {
		client.closeFlag = true
		if client.isGoodCloseCondition() {
			s.clientMapLock.Lock()
			client.closeNow()
			// remove from client map
			delete(s.clientMap, connID)
			s.clientMapLock.Unlock()
		} else {
			go s.aSyncWaitClose(client)
		}
	}
	return nil
}

// async wait client to close
func (s *server) aSyncWaitClose(client *lspConn) {
	<-client.closeChan
	// remove from clientMap
	s.clientMapLock.Lock()
	delete(s.clientMap, client.connId)
	s.clientMapLock.Unlock()
}

func (s *server) Close() error {
	chanList := make([]chan bool, 0)
	// set deleted flag for all clients
	s.clientLostFlag = false
	s.clientMapLock.Lock()
	for _, client := range s.clientMap {
		client.closeFlag = true
		if client.isGoodCloseCondition() {
			client.closeNow()
		} else {
			chanList = append(chanList, client.closeChan)
		}
	}
	s.clientMapLock.Unlock()

	// waiting for all client to be closed
	for _, closeChan := range chanList {
		<-closeChan
	}

	s.endPointClose = true

    // close endpoint
	close(s.closeChan)
	s.endPoint.close()	

	// return error if some clients' connection lost during this time
	if s.clientLostFlag {
		return errors.New("Some clients connection lost during close time.")
	}
	return nil
}
