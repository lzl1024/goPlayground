package storageserver

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	RE_CONN_INT_SEC = 1
)

type leaseReco struct {
	expirationTimeInSec time.Time
	hostport            string
}

type storageServer struct {
	// basic info
	numNodes      int
	nodeID        uint32
	rangeStart    uint32
	servers       map[uint32]*storagerpc.Node
	isMaster      bool
	readyChan     chan struct{}
	bootstrapLock sync.Mutex

	// persist info
	persist     map[string]interface{}
	persistLock sync.RWMutex
	// lease info
	leaseInfo   map[string]*list.List
	leaseLock   sync.RWMutex
	revokingMap map[string]chan struct{} // key and revoking waiting chann

	logger *log.Logger
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	curLogFile, _ := os.OpenFile("Storage-"+strconv.Itoa(int(nodeID))+"-log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
	ss := &storageServer{
		numNodes:    numNodes,
		nodeID:      nodeID,
		servers:     make(map[uint32]*storagerpc.Node),
		isMaster:    masterServerHostPort == "",
		readyChan:   make(chan struct{}),
		persist:     make(map[string]interface{}),
		leaseInfo:   make(map[string]*list.List),
		revokingMap: make(map[string]chan struct{}),
		logger:      log.New(curLogFile, " ", log.Lmicroseconds|log.Lshortfile),
	}

	// register rpc
	address := net.JoinHostPort("localhost", strconv.Itoa(port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	// rpc server start
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	node := &storagerpc.Node{
		HostPort: address,
		NodeID:   nodeID,
	}

	// master node starting
	if ss.isMaster {
		// add himself into the nodeID list
		ss.servers[nodeID] = node
		if numNodes == 1 {
			close(ss.readyChan)
		} else {
			ss.logger.Println("Master waiting for ready.")
			<-ss.readyChan
		}
		ss.readyChan = nil
		ss.setRangeStart()
		return ss, nil
	} else {
		// other nodes waiting for rpc message
		for {
			ss.logger.Println("Node try bootstraping.. ", nodeID)
			if err := ss.bootstrapNonMaster(masterServerHostPort, *node); err == nil {
				return ss, nil
			} else {
				time.Sleep(time.Second * time.Duration(RE_CONN_INT_SEC))
			}
		}
	}
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.isMaster {
		reply.Status = storagerpc.WrongServer
		return errors.New("Error server")
	}

	ss.bootstrapLock.Lock()
	defer ss.bootstrapLock.Unlock()
	// already finished
	if ss.readyChan == nil {
		reply.Status = storagerpc.OK
		ss.logger.Println("Register finish already.")
		return nil
	}

	// register
	ss.servers[args.ServerInfo.NodeID] = &args.ServerInfo
	if len(ss.servers) < ss.numNodes {
		reply.Status = storagerpc.NotReady
		ss.logger.Println("Register server not ready ", args.ServerInfo.NodeID)
	} else {
		reply.Status = storagerpc.OK
		close(ss.readyChan)
		for _, server := range ss.servers {
			reply.Servers = append(reply.Servers, *server)
		}
		ss.logger.Println("Register server ", args.ServerInfo.NodeID)
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if !ss.isMaster {
		reply.Status = storagerpc.WrongServer
		return errors.New("Error server")
	}
	ss.bootstrapLock.Lock()
	defer ss.bootstrapLock.Unlock()
	// already finished
	if ss.readyChan == nil {
		reply.Status = storagerpc.OK
		for _, server := range ss.servers {
			reply.Servers = append(reply.Servers, *server)
		}
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	status, lease, val := ss.get(args)
	// check status
	reply.Status = status
	if reply.Status == storagerpc.OK {
		reply.Lease = lease
		reply.Value = val.(string)
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	status, lease, val := ss.get(args)
	// check status
	reply.Status = status
	if reply.Status == storagerpc.OK {
		reply.Lease = lease
		reply.Value = val.([]string)
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.logger.Println("Put new key ", args.Key, args.Value)
	// key is not in server
	if st := ss.getServer(args.Key); st != storagerpc.OK {
		reply.Status = st
		return nil
	}

	ss.inRevoking(args.Key)
	ss.doRevoke(args.Key)

	ss.persistLock.Lock()
	defer ss.persistLock.Unlock()

	ss.persist[args.Key] = args.Value
	reply.Status = storagerpc.OK
	ss.logger.Println("Put new key complete ", args.Key, args.Value)
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.logger.Println("AppendTo List ", args.Key, args.Value)
	// key is not in server
	if st := ss.getServer(args.Key); st != storagerpc.OK {
		reply.Status = st
		return nil
	}
	ss.inRevoking(args.Key)

	ss.persistLock.Lock()
	defer ss.persistLock.Unlock()
	vals, ok := ss.persist[args.Key]
	// sanity checks
	if !ok {
		v := make([]string, 0)
		v = append(v, args.Value)
		ss.persist[args.Key] = v
		ss.logger.Println("Create new k,v in list ", args.Key, args.Value)
		reply.Status = storagerpc.OK
		return nil
	}

	v := vals.([]string)
	for _, e := range v {
		if e == args.Value {
			reply.Status = storagerpc.ItemExists
			ss.logger.Println("item exist ", args.Key, args.Value, v)
			return nil
		}
	}
	ss.persistLock.Unlock()

	// reove keys after update
	ss.doRevoke(args.Key)
	ss.persistLock.Lock()
	ss.persist[args.Key] = append(v, args.Value)
	ss.logger.Println("append new k,v in list ", args.Key, args.Value)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.logger.Println("RemoveFromList List ", args.Key, args.Value)
	// key is not in server
	if st := ss.getServer(args.Key); st != storagerpc.OK {
		reply.Status = st
		return nil
	}

	ss.inRevoking(args.Key)

	ss.persistLock.Lock()
	defer ss.persistLock.Unlock()
	// sanity checks
	vals, ok := ss.persist[args.Key]
	if !ok {
		ss.logger.Println("Key not found k,v in list ", args.Key, args.Value)
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	// remove key in the list if found
	v := vals.([]string)
	for i, e := range v {
		if e == args.Value {
		    ss.persistLock.Unlock()
			ss.doRevoke(args.Key)
			ss.persistLock.Lock()
			ss.persist[args.Key] = append(v[:i], v[i+1:]...)
			ss.logger.Println("Remove k,v in list ", args.Key, args.Value, ss.persist[args.Key])
			reply.Status = storagerpc.OK
			return nil
		}
	}

	ss.logger.Println("Item not found k,v in list ", args.Key, args.Value)
	reply.Status = storagerpc.ItemNotFound
	return nil
}

func (ss *storageServer) bootstrapNonMaster(mastPort string, node storagerpc.Node) error {
	master, err := rpc.DialHTTP("tcp", mastPort)
	if err != nil {
		return err
	}

	//send join rpc
	args := &storagerpc.RegisterArgs{
		ServerInfo: node,
	}
	var reply storagerpc.RegisterReply
	if err := master.Call("StorageServer.RegisterServer", args, &reply); err != nil {
		return err
	}

	// update information from server
	if reply.Status == storagerpc.OK {
		for _, server := range reply.Servers {
			ss.servers[server.NodeID] = &server
		}
		ss.setRangeStart()
		ss.readyChan = nil
		ss.logger.Println("Register success.")
		return nil
	} else {
		return errors.New("Failed to register server")
	}
}

func (ss *storageServer) get(args *storagerpc.GetArgs) (storagerpc.Status, storagerpc.Lease, interface{}) {
	lease := storagerpc.Lease{}
	if st := ss.getServer(args.Key); st != storagerpc.OK {
		return st, lease, nil
	}
	// check cache
	ss.persistLock.RLock()
	val, ok := ss.persist[args.Key]
	if !ok {
		ss.logger.Println("Cannot find the key ", args.Key)
		ss.persistLock.RUnlock()
		return storagerpc.KeyNotFound, lease, nil
	}
	ss.persistLock.RUnlock()

	// check lease
	if args.WantLease {
		lease = ss.updateLeaseInfo(args.Key, args.HostPort)
	}

	ss.logger.Println("Get the key ", args.Key, lease, val)
	return storagerpc.OK, lease, val
}

func (ss *storageServer) updateLeaseInfo(key, hostport string) storagerpc.Lease {
	ss.logger.Println("Update lease info ", key, hostport)
	ss.leaseLock.Lock()
	defer ss.leaseLock.Unlock()

	// don't grant if it is being revoking
	if _, ok := ss.revokingMap[key]; ok {
		ss.logger.Println("In revoking ", key, hostport)
		return storagerpc.Lease{Granted: false}
	}

	// add record into the leases info cache
	leases := ss.leaseInfo[key]
	if leases == nil {
		leases = list.New()
		ss.leaseInfo[key] = leases
	}

	leases.PushBack(&leaseReco{
		expirationTimeInSec: time.Now().Add(time.Second * time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)),
		hostport:            hostport,
	})
	ss.logger.Println("Grant lease ", key, hostport)

	return storagerpc.Lease{
		Granted:      true,
		ValidSeconds: storagerpc.LeaseSeconds,
	}
}

// check if key belongs to this server
func (ss *storageServer) getServer(key string) storagerpc.Status {
	if ss.readyChan != nil {
		return storagerpc.NotReady
	}

	user := strings.Split(key, ":")[0]
	hash := libstore.StoreHash(user)

	if ss.rightServer(hash) {
		return storagerpc.OK
	} else {
		ss.logger.Println("Wrong Server, hash is ", hash)
		return storagerpc.WrongServer
	}
}

// check if this hash should be managed by this server
func (ss *storageServer) rightServer(hash uint32) bool {
	// only node
	if ss.rangeStart == ss.nodeID {
		return true
	} else if ss.rangeStart > ss.nodeID {
		// first node
		return hash <= ss.nodeID || hash > ss.rangeStart
	} else {
		// other nodes should have hash in the range
		return hash <= ss.nodeID && hash > ss.rangeStart
	}
}

// check if key belongs to this server
func (ss *storageServer) setRangeStart() {
	if len(ss.servers) == 1 {
		ss.rangeStart = ss.nodeID
		ss.logger.Println("range start one server : ", ss.nodeID)
		return
	}

    var chosenServer uint32
	chosenMin := ^uint32(0)
	// find nearest successor -- might have overflow
	for serverId, _ := range ss.servers {
		tmp := uint32(ss.nodeID - serverId)
		if tmp != 0 && tmp < chosenMin {
			chosenMin = tmp
			chosenServer = serverId
		}
	}
	ss.logger.Println("range start is : ", chosenServer)
	ss.rangeStart = chosenServer
}

func (ss *storageServer) inRevoking(key string) {
	// waiting for channel signal
	for {
		ss.leaseLock.RLock()
		if wchan, ok := ss.revokingMap[key]; !ok {
			ss.leaseLock.RUnlock()
			return
		} else {
			ss.logger.Println("Waiting channel revoking...")
			ss.leaseLock.RUnlock()
			<-wchan
		}
	}
}

func (ss *storageServer) doRevoke(key string) {
    ss.leaseLock.Lock()
	leases, ok := ss.leaseInfo[key]
	if !ok {
		ss.logger.Println("Not in leases, no revoke ", key)
		ss.leaseLock.Unlock()
		return
	}

	// make channel for revoking
	revokeChan := make(chan struct{})
	ss.revokingMap[key] = revokeChan

	// release lock whens sending rpcs, revoke chan has protected
	ss.leaseLock.Unlock()

	for e := leases.Front(); e != nil; e = e.Next() {
		rec := e.Value.(*leaseReco)

		// not expired revoke
		if rec.expirationTimeInSec.After(time.Now()) {
			expir := rec.expirationTimeInSec.Sub(time.Now())
			finished := make(chan *rpc.Call, 1)

			// send out rpc
			ss.logger.Println("Send out revoke lease to ", rec.hostport, key)
			client, err := rpc.DialHTTP("tcp", rec.hostport)
			if err == nil {
				args := &storagerpc.RevokeLeaseArgs{Key: key}
				var reply storagerpc.RevokeLeaseReply
				client.Go("LeaseCallbacks.RevokeLease", args, &reply, finished)
			} else {
				ss.logger.Println("Cannot build connection for revoking lease rpc ", rec.hostport, key)
				continue
			}

			// expired or get callback
			select {
			case <-time.After(expir):
				ss.logger.Println("Send out revoke lease expired ", rec.hostport, key)
			case <-finished:
				ss.logger.Println("Send out revoke lease get callback ", rec.hostport, key)
			}
		}
	}

	// lock when remove records
	ss.leaseLock.Lock()
	delete(ss.revokingMap, key)
	delete(ss.leaseInfo, key)
	close(revokeChan)
	ss.logger.Println("revoke complete ", key)
	ss.leaseLock.Unlock()
}
