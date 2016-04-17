package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	CLEAN_CACHE_PREIOD_SEC = 3
	MAX_CONN_RETRY_TIME    = 5
	RPC_LEASE_CALL_BACK    = "LeaseCallbacks"
	RPC_GET_SERVER         = "StorageServer.GetServers"
	RPC_GET                = "StorageServer.Get"
	RPC_PUT                = "StorageServer.Put"
	RPC_GET_LIST           = "StorageServer.GetList"
	RPC_APPEND_LIST        = "StorageServer.AppendToList"
	RPC_REMOVE_LIST        = "StorageServer.RemoveFromList"
)

var (
	FailedToFindKeyError  = errors.New("Failed to find key")
	WrongServerError      = errors.New("Wrong Server")
	ItemAlreadyExistError = errors.New("Item already exist")
	FaileToFindItemError  = errors.New("Cannot find the item")
	UnknownError          = errors.New("UNKNOWN error")
)

// request record
// stats of the cache
type reqValStats struct {
	t     int64 // time this solt is used
	count int
}

// value in the cache
type reqValue struct {
	lastUpdateTimeInSec int64
	stats               [storagerpc.QueryCacheSeconds]*reqValStats // each second takes a slot
}

// cached record
type cachedValue struct {
	value       interface{}
	expiredTime time.Time
}

type libstore struct {
	// basic info
	mode       LeaseMode
	myHostPort string
	sNodes     map[uint32]*rpc.Client

	// request record
	reqInfo map[string]*reqValue
	reqLock sync.RWMutex

	// cache
	valueCache map[string]*cachedValue
	cLock      sync.RWMutex

	logger *log.Logger
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	curLogFile, _ := os.OpenFile("Lib-"+myHostPort+"-log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
	ls := &libstore{
		mode:       mode,
		myHostPort: myHostPort,
		sNodes:     make(map[uint32]*rpc.Client),
		reqInfo:    make(map[string]*reqValue),
		valueCache: make(map[string]*cachedValue),
		logger:     log.New(curLogFile, " ", log.Lmicroseconds|log.Lshortfile),
	}

	// connect to storage master
	sMaster, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		ls.logger.Println("Failed to dail master", masterServerHostPort, err)
		return nil, err
	}
	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply
	success := false
	for i := 0; i < MAX_CONN_RETRY_TIME; i++ {
		ls.logger.Println("Send out RPC ", RPC_GET_SERVER)
		err := sMaster.Call(RPC_GET_SERVER, &args, &reply)
		if err == nil && reply.Status == storagerpc.OK {
			success = true
			break
		}
		time.Sleep(time.Second)
	}
	if !success {
		return nil, errors.New("Failed to connected with master storage server")
	}

	// connect with storage nodes
	for _, node := range reply.Servers {
		ls.sNodes[node.NodeID], err = rpc.DialHTTP("tcp", node.HostPort)
		if err != nil {
			return nil, err
		}
	}
	// register lease call back and start routines
	if mode != Never {
		rpc.RegisterName(RPC_LEASE_CALL_BACK, librpc.Wrap(ls))
		go ls.cleanCache()
	}
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// try cache first
	if v, ok := ls.tryCache(key); ok {
		ls.logger.Println("Get key from cache ", key, v)
		return v.(string), nil
	}

	// send rpc call
	var reply storagerpc.GetReply
	args := &storagerpc.GetArgs{Key: key, HostPort: ls.myHostPort}
	if ls.wantLease(key) {
		args.WantLease = true
	}
	client := ls.getRPCClient(key)
	ls.logger.Println("Send out RPC ", RPC_GET)
	if err := client.Call(RPC_GET, args, &reply); err != nil {
		return "", err
	}

	// return
	switch reply.Status {
	case storagerpc.OK:
		result := reply.Value
		// update cache if lease granted
		if reply.Lease.Granted {
			ls.cLock.Lock()
			ls.valueCache[key] = &cachedValue{
				value:       result,
				expiredTime: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
			}
			ls.cLock.Unlock()
		}
		return result, nil
	case storagerpc.WrongServer:
		ls.logger.Println("Wrong Server ", key)
		return "", WrongServerError
	case storagerpc.KeyNotFound:
		ls.logger.Println("Failed to find key ", key)
		return "", FailedToFindKeyError
	default:
		return "", UnknownError
	}
}

func (ls *libstore) Put(key, value string) error {
	return ls.updateInternal(key, value, RPC_PUT)
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// try cache first
	if v, ok := ls.tryCache(key); ok {
		ls.logger.Println("Get key from cache ", key, v)
		return v.([]string), nil
	}

	// send rpc call
	var reply storagerpc.GetListReply
	args := &storagerpc.GetArgs{Key: key, HostPort: ls.myHostPort}
	if ls.wantLease(key) {
		args.WantLease = true
	}
	client := ls.getRPCClient(key)
	ls.logger.Println("Send out RPC ", RPC_GET_LIST)
	if err := client.Call(RPC_GET_LIST, args, &reply); err != nil {
	    ls.logger.Println("RPC failed ", err)
		return nil, err
	}

	// return
	switch reply.Status {
	case storagerpc.OK:
		result := reply.Value
		// update cache if lease granted
		if reply.Lease.Granted {
			ls.cLock.Lock()
			ls.valueCache[key] = &cachedValue{
				value:       result,
				expiredTime: time.Now().Add(time.Second * time.Duration(reply.Lease.ValidSeconds)),
			}
			ls.cLock.Unlock()
		}
		ls.logger.Println("Get key use rpc ", key, result)
		return result, nil
	case storagerpc.WrongServer:
		ls.logger.Println("Wrong Server ", key)
		return nil, WrongServerError
	case storagerpc.KeyNotFound:
		ls.logger.Println("Failed to find key ", key)
		return nil, FailedToFindKeyError
	default:
		return nil, UnknownError
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	return ls.updateInternal(key, removeItem, RPC_REMOVE_LIST)
}

func (ls *libstore) AppendToList(key, newItem string) error {
	return ls.updateInternal(key, newItem, RPC_APPEND_LIST)
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.cLock.Lock()
	defer ls.cLock.Unlock()
	// return ok if found the key else, return key not found
	if _, ok := ls.valueCache[args.Key]; ok {
		delete(ls.valueCache, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

// update call code can be reused
func (ls *libstore) updateInternal(key, value, rpcName string) error {
	// get client and send rpc call
	client := ls.getRPCClient(key)
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	ls.logger.Println("Send out RPC ", rpcName)
	if err := client.Call(rpcName, args, &reply); err != nil {
		return err
	}
	// return
	switch reply.Status {
	case storagerpc.OK:
		ls.logger.Println("Successfully put ", key, value)
		return nil
	case storagerpc.WrongServer:
		ls.logger.Println("Wrong Server ", key, rpcName)
		return WrongServerError
	case storagerpc.ItemExists:
		ls.logger.Println("Item already exist ", key, rpcName)
		return ItemAlreadyExistError
	case storagerpc.ItemNotFound:
		ls.logger.Println("Cannot find the item ", key, rpcName)
		return FaileToFindItemError
	default:
		return UnknownError
	}
}

// try to get value from cache
func (ls *libstore) tryCache(key string) (interface{}, bool) {
	ls.cLock.Lock()
	defer ls.cLock.Unlock()
	// it should be in the cache and less than expiredTime
	v, ok := ls.valueCache[key]
	if ok {
		if time.Now().After(v.expiredTime) {
			delete(ls.valueCache, key)
		} else {
			return v.value, true
		}
	}
	return nil, false
}

func (ls *libstore) wantLease(key string) bool {
	if ls.mode == Always {
		return true
	} else if ls.mode == Never {
		return false
	}

	now := time.Now().Unix()
	// update request bitmap
	ls.updateRequestInfo(key, now)

	// count requestInfo
	ls.reqLock.RLock()
	defer ls.reqLock.RUnlock()
	vReqInfo, ok := ls.reqInfo[key]
	if !ok {
		return false
	}

	count := 0
	minTime := now - storagerpc.QueryCacheSeconds
	for _, valStats := range vReqInfo.stats {
		// add up available request count
		if valStats != nil && valStats.t > minTime {
			count += valStats.count
			if count >= storagerpc.QueryCacheThresh {
				ls.logger.Println("Recent count, lease :", key, count)
				return true
			}
		}
	}
	ls.logger.Println("Recent count, don't lease :", key, count)
	return false
}

func (ls *libstore) updateRequestInfo(key string, now int64) {
	ls.reqLock.Lock()
	defer ls.reqLock.Unlock()
	// locate req info
	vReqInfo, ok := ls.reqInfo[key]
	if !ok {
		ls.logger.Println("Create new request info :", key)
		vReqInfo = new(reqValue)
		ls.reqInfo[key] = vReqInfo
	}
	// do update
	index := int(now) % storagerpc.QueryCacheSeconds

	valStats := vReqInfo.stats[index]
	// if not exist or sliding over, create a new one
	if valStats == nil || valStats.t != now {
		vReqInfo.stats[index] = &reqValStats{
			count: 1,
			t:     now,
		}
		ls.logger.Println("Create new request status :", key, now)
	} else {
		valStats.count++
		ls.logger.Println("Add count :", valStats.count)
	}
	// update last update time
	vReqInfo.lastUpdateTimeInSec = now
}

func (ls *libstore) getRPCClient(key string) *rpc.Client {
	user := strings.Split(key, ":")[0]
	hash := StoreHash(user)

	var chosenClient *rpc.Client
	chosenServer := ^uint32(0)
	// find nearest successor -- might have overflow
	for serverId, rpcClient := range ls.sNodes {
		tmp := uint32(serverId - hash)
		if tmp < chosenServer {
			chosenServer = tmp
			chosenClient = rpcClient
		}
	}
	ls.logger.Println("Get client from:", key, chosenServer)
	return chosenClient
}

func GetStorageServer(key string, sNodes map[uint32]interface{}) (uint32, interface{}) {
	user := strings.Split(key, ":")[0]
	hash := StoreHash(user)

	var chosenClient interface{}
	chosenServer := ^uint32(0)
	// find nearest successor -- might have overflow
	for serverId, rpcClient := range sNodes {
		tmp := uint32(serverId - hash)
		if tmp < chosenServer {
			chosenServer = tmp
			chosenClient = rpcClient
		}
	}
	return chosenServer, chosenClient
}

// clean cache if values are too old, to protect server from OOM
func (ls *libstore) cleanCache() {
	for {
		time.Sleep(time.Second * time.Duration(CLEAN_CACHE_PREIOD_SEC))
		curTime := time.Now()

		// cache
		ls.cLock.Lock()
		for k, v := range ls.valueCache {
			if v.expiredTime.Before(curTime) {
				ls.logger.Println("Clean cache routine:", k)
				delete(ls.valueCache, k)
			}
		}
		ls.cLock.Unlock()

		// request info
		ls.reqLock.Lock()
		stallTime := curTime.Unix() - storagerpc.QueryCacheSeconds
		for k, v := range ls.reqInfo {
			if v.lastUpdateTimeInSec <= stallTime {
				ls.logger.Println("Clean request info routine:", k)
				delete(ls.reqInfo, k)
			}
		}
		ls.reqLock.Unlock()
	}
}
