package tribserver

import (
	"container/heap"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
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

type tribServer struct {
	libstore.Libstore
	logger *log.Logger
}

const (
	sub uint8 = iota + 1
	unsub
)

// Helper functions
const (
	getTribNum    = 100
	userSuffix    = "user"
	tribbleSuffix = "trib"
)

// min max heap
type heapElement struct {
	t      int64
	value  string
	userId string
	index  int
}

type maxHeap []*heapElement

func (h maxHeap) Len() int { return len(h) }

func (h maxHeap) Less(i, j int) bool {
	return h[i].t < h[j].t
}

func (h *maxHeap) Swap(i, j int) {
	slice := *h
	slice[i], slice[j] = slice[j], slice[i]
}

func (h *maxHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapElement))
}

func (h *maxHeap) Pop() interface{} {
	slice := *h
	l := len(slice)
	e := slice[l-1]
	*h = slice[0 : l-1]
	return e
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	store, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}

	curLogFile, _ := os.OpenFile("Server-"+myHostPort+"-log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
	// init tribServer
	ts := &tribServer{
		Libstore: store,
		logger:   log.New(curLogFile, " ", log.Lmicroseconds|log.Lshortfile),
	}

	conn, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	go http.Serve(conn, nil)

	// register tribServer to rpc framwork
	if err := rpc.RegisterName("TribServer", tribrpc.Wrap(ts)); err != nil {
		return nil, err
	}

	// open rpc listener
	rpc.HandleHTTP()
	fmt.Println("Server", myHostPort, "has been started")
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	ts.logger.Println("Create user ", args.UserID)
	userKey := genUserKey(args.UserID)
	// try update key
	if err := ts.Libstore.AppendToList(userKey, ""); err != nil {
		if err != libstore.ItemAlreadyExistError {
			return err
		} else {
			reply.Status = tribrpc.Exists
			return nil
		}
	}
	ts.logger.Println("Create user complete ", args.UserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) subAction(uId, targetUid string, reply *tribrpc.SubscriptionReply, action uint8) error {
	// try to find target user
	if _, err := ts.Libstore.GetList(genUserKey(targetUid)); err != nil || targetUid == "" {
		reply.Status = tribrpc.NoSuchTargetUser
		ts.logger.Println("Cannot find target user ", targetUid, action)
		return nil
	}

	// do action, ignore error, might be no subscription
	subKey := genUserKey(uId)
	subList, err := ts.Libstore.GetList(genUserKey(uId))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		ts.logger.Println("Cannot find user ", uId, action)
		return nil
	}

	switch action {
	case sub:
		for _, subUser := range subList {
			if subUser == targetUid {
				ts.logger.Println("User subscribe exist ", uId, targetUid)
				reply.Status = tribrpc.Exists
				return nil
			}
		}
		// add sub
		err := ts.Libstore.AppendToList(subKey, targetUid)
		// allow to find exist to find item here
		if err == nil || err == libstore.ItemAlreadyExistError {
			reply.Status = tribrpc.OK
			ts.logger.Println("User subscribe to target user ", uId, targetUid)
		} else {
			reply.Status = tribrpc.NoSuchUser
			ts.logger.Println("Failed subscribe to target user ", uId, targetUid, err)
		}
		return nil
	case unsub:
		for _, subUser := range subList {
			if subUser == targetUid {
				err := ts.Libstore.RemoveFromList(subKey, targetUid)
				// allow to failed to find item here
				if err == nil || err == libstore.FaileToFindItemError {
					reply.Status = tribrpc.OK
					ts.logger.Println("User unsubscribe to target user ", uId, targetUid)
				} else {
					reply.Status = tribrpc.NoSuchTargetUser
					ts.logger.Println("Failed unsubscribe to target user ", uId, targetUid)
				}
				return nil
			}
		}
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	ts.logger.Println("UNKNOWN action ", action)
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.logger.Println("Add sub ", args.UserID, args.TargetUserID)
	return ts.subAction(args.UserID, args.TargetUserID, reply, sub)
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return ts.subAction(args.UserID, args.TargetUserID, reply, unsub)
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	// try to find user
	subKey := genUserKey(args.UserID)
	if subList, err := ts.Libstore.GetList(subKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	} else {
		ts.logger.Println("Get subs ", subList)
		// first is resverved empty
		if len(subList) > 0 {
			reply.UserIDs = subList[1:]
		}
		reply.Status = tribrpc.OK
		return nil
	}
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	// try to find user
	if _, err := ts.Libstore.GetList(genUserKey(args.UserID)); err != nil {
		reply.Status = tribrpc.NoSuchUser
		ts.logger.Println("Cannot find user ", args.UserID)
		return nil
	}

	tribListKey := genTribHashListKey(args.UserID)
	// tribHash is time|hash
	tribHash := genTribHash(args.Contents)

	// append hash to user hashList
	if err := ts.Libstore.AppendToList(tribListKey, tribHash); err != nil {
		reply.Status = tribrpc.NoSuchUser
		ts.logger.Println("Failed to appendToList post: ", err)
		return err
	}

	// generate key-value pair to store
	tribKey := genTribKey(args.UserID, tribHash)
	if err := ts.Libstore.Put(tribKey, args.Contents); err != nil {
		reply.Status = tribrpc.NoSuchUser
		ts.logger.Println("Failed to put post: ", err)
		return err
	}
	ts.logger.Println("Post tribble: ", args.UserID, tribListKey, tribHash, args.Contents)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// try to find user
	if _, err := ts.Libstore.GetList(genUserKey(args.UserID)); err != nil {
		reply.Status = tribrpc.NoSuchUser
		ts.logger.Println("Cannot find user ", args.UserID)
		return nil
	}

	if tribbles, err := ts.getTribblesHelper(args.UserID, 0); err != nil {
		reply.Status = tribrpc.OK
		if err != libstore.FailedToFindKeyError {
			return err
		}
		return nil
	} else {
		reply.Tribbles = ts.getMaxNTribbles(tribbles)
		reply.Status = tribrpc.OK
		ts.logger.Println("Get tribbles: ", args.UserID, reply.Tribbles)
		return nil
	}
}

func (ts *tribServer) getTribblesHelper(userId string, index int) (*maxHeap, error) {
	// get hash list from user
	hashList, err := ts.Libstore.GetList(genTribHashListKey(userId))
	if err != nil {
		ts.logger.Println("Failed to get list ", err)
		return nil, err
	}

	// get max 100 use heap
	return ts.getMax(userId, hashList, index), nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// get all subscribees
	subsList, err := ts.Libstore.GetList(genUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		ts.logger.Println("Cannot find user ", args.UserID)
		return nil
	}

	// wait to get max N from each subscirbee
	l := len(subsList)
	if l == 0 {
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	}

	var wg sync.WaitGroup
	maxList := make([]*maxHeap, l)
	wg.Add(l)
	for i, sub := range subsList {
		go func(i int, sub string) {
			defer wg.Done()
			maxList[i], err = ts.getTribblesHelper(sub, i)
		}(i, sub)
	}
	wg.Wait()

	reply.Tribbles = ts.getMaxNTribbles(ts.mergeHeaps(maxList))
	reply.Status = tribrpc.OK
	ts.logger.Println("Get tribbles sub: ", args.UserID, reply.Tribbles)
	return nil
}

// get max N heap from a hashList
func (ts *tribServer) getMax(userId string, hashList []string, index int) *maxHeap {
	h := new(maxHeap)

	// minmax heap for getTribNum
	for i, value := range hashList {
		e, err := ts.makeElement(userId, value, index)
		if err != nil {
			ts.logger.Println("Failed to convert value", value)
			continue
		}
		heap.Push(h, e)
		if i >= getTribNum {
			heap.Pop(h)
		}
	}
	return h
}

// deserialize hash value and make heap element
func (ts *tribServer) makeElement(userId string, value string, id int) (*heapElement, error) {
	fields := strings.Split(value, "|")
	t, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		ts.logger.Println("Failed to split ", fields)
		return nil, err
	}
	e := &heapElement{
		userId: userId,
		index:  id,
		value:  value,
		t:      t,
	}
	return e, nil
}

// get max tribbles using rpc calls
func (ts *tribServer) getMaxNTribbles(maxList *maxHeap) []tribrpc.Tribble {
	if maxList == nil {
		return nil
	}
	l := len(*maxList)
	ts.logger.Println("Number of tribbles ", l)
	tribble := make([]tribrpc.Tribble, l)
	failed := make([]int, 0)
	var fLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(l)
	for i := 0; i < l; i++ {
		e := heap.Pop(maxList).(*heapElement)
		go func(index int, e *heapElement) {
			defer wg.Done()
			v, err := ts.Libstore.Get(genTribKey(e.userId, e.value))
			if err != nil {
				ts.logger.Println("Failed to find value by", e.userId, e.value)
				fLock.Lock()
				failed = append(failed, i)
				fLock.Unlock()
				return
			}
			ts.logger.Println("Get value", e.userId, v, e.t)
			tribble[index] = tribrpc.Tribble{
				UserID:   e.userId,
				Posted:   time.Unix(0, e.t),
				Contents: v,
			}
		}(l-i-1, e)
	}
	// waiting for get hash from rpc
	wg.Wait()
	// remove failed ones -- concurrent put event might happend
	for i := range failed {
		tribble = append(tribble[:i], tribble[i+1:]...)
	}
	return tribble
}

func (ts *tribServer) mergeHeaps(maxList []*maxHeap) *maxHeap {
	l := len(maxList)
	if l == 1 {
		return maxList[0]
	}
	// becuase these are min MaxHeap, it has to go through
	// all the elments in the heap
	maxH := maxList[0]
	size := 0
	if maxH == nil {
		maxH = new(maxHeap)
	} else {
		size = len(*maxList[0])
	}

	for i := 1; i < l; i++ {
		if maxList[i] == nil {
			continue
		}
		for _, e := range *maxList[i] {
			heap.Push(maxH, e)
			size++
			if size > getTribNum {
				heap.Pop(maxH)
				size--
			}
		}
	}
	return maxH
}

// formatter helper functions
func genUserKey(user string) string {
	return fmt.Sprintf("%s:%s", user, userSuffix)
}

func genTribHashListKey(user string) string {
	return fmt.Sprintf("%s:%s", user, tribbleSuffix)
}

func genTribKey(user string, hash string) string {
	return fmt.Sprintf("%s:%s", user, hash)
}

func genTribHash(contents string) string {
	return fmt.Sprintf("%d|%d", time.Now().UnixNano(), libstore.StoreHash(contents))
}
