// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	INIT_CLIENT_CAP   = 10
	SWEEP_INT_IN_SEC  = 5
	FLOW_CONTROL_SIZE = 100
	ENABLE_DEBUG_LOG  = false
	SWEEP_MEMBER_MIN  = 10
)

type multiEchoServer struct {
	memberCount int
	countChan   chan byte // countChan, used as lock, since lock is not allowed
	listener    *net.TCPListener
	isClosed    bool        // closed flag
	clients     []*client   // client list
	clientChan  chan byte   // used as lock for client management
	msgChan     chan string // channel for wirting message
	closeChan   chan byte   // close signal
}

// invisble outside
type client struct {
	isCon          bool
	flowController chan string // control the flow
	conn           net.Conn
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	echoServer := &multiEchoServer{
		memberCount: 0,
		countChan:   make(chan byte, 1),
		listener:    nil,
		isClosed:    false,
		clients:     make([]*client, 0, INIT_CLIENT_CAP),
		clientChan:  make(chan byte, 1),
		msgChan:     make(chan string),
		closeChan:   make(chan byte, 1),
	}

	return echoServer
}

func (mes *multiEchoServer) Start(port int) error {
	fmt.Println("Starting Server... ", port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	mes.listener = listener

	fmt.Println("Server started.")
	// start thread for adding connections
	go func() {
		for {
			if conn, err := mes.listener.Accept(); err != nil {
				if !mes.isClosed {
					fmt.Println("Failed to accept client %s", err.Error())
				} else {
					return
				}
			} else {
				mes.addClient(conn)
			}
		}
	}()

	// start message handler
	go func() {
		for message := range mes.msgChan {
			mes.writeToClients(message)
		}
	}()

	// start thread for sweep connections
	go func() {
		ticker := time.NewTicker(SWEEP_MEMBER_MIN * time.Minute)
		for {
			select {
			case <-ticker.C:
				fmt.Println("Start client sweeping task...")
				// remove disconnected client from list
				mes.countChan <- 1
				newClient := make([]*client, 0, len(mes.clients))
				for _, client := range mes.clients {
					if client.isCon {
						newClient = append(newClient, client)
					}
				}
				// copy over the new client list
				mes.clients = newClient
				fmt.Println("Finished client sweeping task.", len(mes.clients))
				<-mes.countChan
			case <-mes.closeChan:
				ticker.Stop()
				return
			}
		}
	}()
	return nil
}

func (mes *multiEchoServer) Close() {
	if mes.listener == nil {
		panic("Server has not been started yet.")
	}
	mes.isClosed = true
	mes.listener.Close()

	mes.countChan <- 1
	for _, client := range mes.clients {
		client.conn.Close()
	}

	close(mes.msgChan)
	for _, client := range mes.clients {
		close(client.flowController)
	}
	close(mes.countChan)
	close(mes.closeChan)
}

func (mes *multiEchoServer) Count() int {
	mes.clientChan <- 1
	defer func() { <-mes.clientChan }()
	if mes.listener == nil || mes.isClosed {
		panic("Server has been closed or not been started yet.")
	}
	debug("member count is " + strconv.Itoa(mes.memberCount))
	return mes.memberCount
}

// add one client
func (mes *multiEchoServer) addClient(conn net.Conn) {
	// init client
	cl := &client{isCon: true,
		flowController: make(chan string, FLOW_CONTROL_SIZE),
		conn:           conn,
	}

	fmt.Println("Client added " + conn.LocalAddr().String())
	// lock client list when adding client
	mes.clientChan <- 1
	mes.clients = append(mes.clients, cl)
	mes.memberCount++
	<-mes.clientChan

	// start reader
	go func() {
		reader := bufio.NewReader(conn)
		for {
			if message, err := reader.ReadString('\n'); err != nil {
				if !mes.isClosed {
					// real error
					cl.isCon = false
					mes.clientChan <- 1
					mes.memberCount--
					<-mes.clientChan
					conn.Close()
					fmt.Println("Client quit: ", err.Error())
				} else {
				    fmt.Println("Client quit without error.")
				}
				return
			} else {
				mes.msgChan <- message
			}
		}
	}()

	// write to remote
	go func() {
		for msg := range cl.flowController {
			if _, err := cl.conn.Write([]byte(msg)); err != nil {
				fmt.Println("Writing error:", err.Error())
			}
		}
	}()
}

// write message to each clients
func (mes *multiEchoServer) writeToClients(msg string) {
	mes.clientChan <- 1
	cls := mes.clients
	<-mes.clientChan
	for _, cl := range cls {
		if chanLen := len(cl.flowController); cl.isCon && chanLen < FLOW_CONTROL_SIZE {
			cl.flowController <- msg
		} else if chanLen >= FLOW_CONTROL_SIZE {
			debug("Drop message " + msg)
		}
	}
	//time.Sleep(1 * time.Microsecond)
}

func debug(msg string) {
	if ENABLE_DEBUG_LOG {
		fmt.Println(msg)
	}
}
