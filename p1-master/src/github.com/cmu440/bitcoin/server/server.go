package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
)

const (
	// split work into work item
	WORK_ITEM_LENGTH = 2000
	// preferred load for worker, big task can be assigned under this load
	// otherwise can only signle item can be assiegned
	PERFERRED_WORKER_LOAD = 5 * WORK_ITEM_LENGTH
	// max load for worker
	MAX_WORK_LOAD = 2 * PERFERRED_WORKER_LOAD
)

type worker struct {
	id            int
	workingQueue  *list.List
	workLoadTotal uint64
}

func newWorker(id int) *worker {
	return &worker{
		id: id,
		// work list list of *miningRequest
		workingQueue:  list.New(),
		workLoadTotal: 0,
	}
}

type workers []*worker

// sort worker by load
func (w workers) Len() int { return len(w) }
func (w workers) Less(i, j int) bool {
	return w[i].workLoadTotal < w[j].workLoadTotal
}
func (w workers) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

// job status
type job struct {
	finishNonce  uint64 // how much has finished
	connId       int
	minHash      uint64
	nonce        uint64
	message      string
	upper        uint64
}

func newJob(connId int, msg string, upper uint64) *job {
	return &job{
		finishNonce: 0,
		connId:       connId,
		minHash:      math.MaxUint64,
		nonce:        0,
		message:      msg,
		upper:        upper,
	}
}

type miningRequest struct {
	jobId int
	lower uint64
	upper uint64
}

func newMiningRequest(jobId int, lower uint64, upper uint64) *miningRequest {
	return &miningRequest{
		jobId: jobId,
		lower: lower,
		upper: upper,
	}
}

var (
	// basic
	logFile, _ = os.OpenFile("Bitcoin-Server.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND, 777)
	logger     *log.Logger
	lspServer  lsp.Server
	// resource manager
	workerList = make(workers, 0, 5)
	// job manager
	jobStatusMap = make(map[int]*job)
	lastJobId    = 0
	// client manager clientId -> jobList
	clientSet = make(map[int]bool)
	// pending request queue due to high load, list of *miningRequest
	pendingRequestQueue = list.New()
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be an integer")
		return
	}
	logger = log.New(logFile, "BitCoinServer-- ", log.Lmicroseconds|log.Lshortfile)
	defer logFile.Close()

	params := lsp.NewParams()
	lspServer, err = lsp.NewServer(port, params)
	if err != nil {
		fmt.Println("Failed to create server")
		return
	}
	// block here, because no close interface
	requestListener()
}

// listen all kinds of request
func requestListener() {
	for {
		connId, payLoad, err := lspServer.Read()
		if err != nil {
			logger.Println("Failure handler with clientId", connId)
			failureHandler(connId)
		} else {
			incomingMsgHanler(connId, payLoad)
		}
	}
}

// handle normal message
func incomingMsgHanler(connId int, payLoad []byte) {
	// unmarshall the message
	msg := &bitcoin.Message{}
	err := json.Unmarshal(payLoad, msg)
	if err != nil {
		fmt.Println("cannot unmarshall the message data")
		return
	}
	logger.Println("Rcv msg", msg)
	switch msg.Type {
	case bitcoin.Join:
		joinHandler(connId)
	case bitcoin.Request:
		requestHandler(connId, msg)
	case bitcoin.Result:
		resultHandler(connId, msg)
	}
}

// handle join message
func joinHandler(connId int) {
	logger.Println("Miner join", connId)
	// add worker
	workerList = append(workerList, newWorker(connId))
	distributedPendingJobs()
}

// handle request message
func requestHandler(connId int, msg *bitcoin.Message) {
	logger.Println("Client join", connId)
	lastJobId++
	jobReq := newJob(connId, msg.Data, msg.Upper)
	jobStatusMap[lastJobId] = jobReq
	clientSet[connId] = true
	// put request to queue
	pendingRequestQueue.PushBack(newMiningRequest(lastJobId, msg.Lower, msg.Upper))
	distributedPendingJobs()
}

// handle result message
func resultHandler(connId int, msg *bitcoin.Message) {
	// find the request
	var wk *worker = nil
	for _, w := range workerList {
		if w.id == connId {
			wk = w
			break
		}
	}
	if wk == nil {
		logger.Println("Failed to find worker ", connId)
	}
	// find request
	req := wk.workingQueue.Remove(wk.workingQueue.Front()).(*miningRequest)

	// accumulate result
	if status, ok := jobStatusMap[req.jobId]; !ok {
		logger.Println("Cannot find job", req.jobId)
	} else {
		if _, ok := clientSet[status.connId]; ok {
			if status.minHash > msg.Hash {
				status.minHash = msg.Hash
				status.nonce = msg.Nonce
				logger.Println("update minHash and nonce", req.jobId, status.minHash, status.nonce)
			}
			// check if the whole job finish
			status.finishNonce += req.upper - req.lower + 1
			logger.Println("Job status: ", status.finishNonce, status.upper)
			if status.finishNonce >= status.upper {
				result := bitcoin.NewResult(status.minHash, status.nonce)
				logger.Println("Job finished!", result)
				sendMsg(status.connId, result)
				delete(jobStatusMap, req.jobId)
			}
		} else {
			logger.Println("client has left")
		}
	}

	// recover the load and sort worker
	wk.workLoadTotal -= req.upper - req.lower + 1
	// work on next job
	if wk.workingQueue.Len() > 0 {
		req := wk.workingQueue.Front().Value.(*miningRequest)
		msg := bitcoin.NewRequest(jobStatusMap[req.jobId].message, req.lower, req.upper)
		logger.Println("Work on next job", msg)
		sendMsg(wk.id, msg)
	}
	sort.Sort(workerList)
	distributedPendingJobs()
}

// send out message
func sendMsg(connId int, msg *bitcoin.Message) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		logger.Println("Failed to marshal the message")
	}
	err = lspServer.Write(connId, buf)
	if err != nil {
		logger.Println("Failed to send msg", msg)
	}
	return nil
}

// handle failure and re-distribute the work
func failureHandler(connId int) {
	if _, ok := clientSet[connId]; ok {
		logger.Println("Clear client and related jobs")
		// client failure, remove client
		delete(clientSet, connId)
		jobTobeDeleted := make([]int, 0)
		for jobId, jobSt := range jobStatusMap {
			if jobSt.connId == connId {
				logger.Println("Clear job:", jobSt.message, jobSt.upper, jobId)
				jobTobeDeleted = append(jobTobeDeleted, jobId)
			}
		}
		// delete job
		for _, jobId := range jobTobeDeleted {
			delete(jobStatusMap, jobId)
		}
	} else {
		// miner failure
		logger.Println("reschedule miner jobs")
		deletedId := -1
		for idx, w := range workerList {
			if w.id == connId {
				deletedId = idx
				runningWork := w.workingQueue.Front()
				for {
					// remove all running work
					if runningWork == nil {
						break
					}
					req := runningWork.Value.(*miningRequest)
					logger.Println("Reschedule:", req.jobId, req.lower, req.upper)
					// update job status, only update job that can be found
					if jobSt, ok := jobStatusMap[req.jobId]; ok {
						pendingRequestQueue.PushBack(req)
					} else {
						logger.Println("Client left so no reschedule", req.jobId, jobSt.connId)
					}
					runningWork = runningWork.Next()
				}
				break
			}
		}
		if deletedId != -1 {
			logger.Println("Remove worker")
			// remove from worker list
			workerList = append(workerList[:deletedId], workerList[deletedId+1:]...)
			distributedPendingJobs()
		} else {
			logger.Println("Unknown connId to be deleted", connId)
		}
	}
}

// distribute pending jobs until no pending jobs or all full
func distributedPendingJobs() {
	for {
		// no pending jobs
		if pendingRequestQueue.Len() == 0 {
			return
		}
		// get next pending job
		pendingJobRequest := pendingRequestQueue.Remove(pendingRequestQueue.Front())
		// return if all full
		if !distributeRequest(pendingJobRequest.(*miningRequest)) {
			return
		}
	}
}

// distribute a request, return distibuted complete or not
func distributeRequest(request *miningRequest) bool {
	logger.Println("Distributing request", request.jobId, request.lower, request.upper)
	curLower := request.lower
	for _, w := range workerList {
		// unable to continue distributed
		if w.workLoadTotal >= MAX_WORK_LOAD {
			break
		} else {
			var allocLower, allocUpper uint64
			// worker has light weight can give a big one
			if PERFERRED_WORKER_LOAD > w.workLoadTotal {
				availableRange := PERFERRED_WORKER_LOAD + WORK_ITEM_LENGTH - w.workLoadTotal
				// leftRange is too large to be in one worker
				if request.upper-curLower+1 > availableRange {
					allocLower = curLower
					allocUpper = curLower + availableRange - 1
				} else {
					// left range can be allocated to one worker totally
					allocLower = curLower
					allocUpper = request.upper
				}
			} else {
				// leftRange is too large to be in one high load worker
				if request.upper-curLower+1 > WORK_ITEM_LENGTH {
					allocLower = curLower
					allocUpper = curLower + WORK_ITEM_LENGTH - 1
				} else {
					allocLower = curLower
					allocUpper = request.upper
				}
			}
			curLower = allocUpper + 1

			// complete allocation for one work
			// update job status
			jobSt := jobStatusMap[request.jobId]
			// sen allocate message to worker
			w.workLoadTotal += allocUpper - allocLower + 1
			w.workingQueue.PushBack(newMiningRequest(request.jobId, allocLower, allocUpper))
			logger.Println("Alloc: ", request.jobId, allocLower, allocUpper, w.id, w.workLoadTotal)
			// if only this job is working, work on it
			if w.workingQueue.Len() == 1 {
				msg := bitcoin.NewRequest(jobSt.message, allocLower, allocUpper)
				logger.Println("Work on next job", msg)
				sendMsg(w.id, msg)
			}
			// finish all
			if curLower > request.upper {
				sort.Sort(workerList)
				return true
			}
		}
	}

	// cannot finish job distributed, no worker or all worker too full
	// create a mining request for left job and push back in the queue
	pendingRequestQueue.PushBack(newMiningRequest(request.jobId, curLower, request.upper))
	logger.Println("push unfinished request", request.jobId, curLower, request.upper)
	sort.Sort(workerList)
	return false
}
