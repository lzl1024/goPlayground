package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"math"
	"os"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}
	hostport := os.Args[1]
	lspClient, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to create client")
		return
	}
	// send join message
	if err := sendMsg(lspClient, bitcoin.NewJoin()); err != nil {
		return
	}

	for {
		buf, err := lspClient.Read()
		if err != nil {
			fmt.Println("Disconnected")
			return
		}
		// listen to requests
		requestMsg := &bitcoin.Message{}
		if err := json.Unmarshal(buf, requestMsg); err != nil {
			fmt.Println("Failed to unmarshal")
			return
		}
		minHash, nonce := runJob(requestMsg.Data, requestMsg.Lower, requestMsg.Upper)
		// send back result
		sendMsg(lspClient, bitcoin.NewResult(minHash, nonce))
	}
}

func runJob(data string, lower, upper uint64) (uint64, uint64) {
	var minHash uint64 = math.MaxUint64
	var nonce uint64 = 0
	for i := lower; i <= upper; i++ {
		hash := bitcoin.Hash(data, i)
		if hash < minHash {
			minHash = hash
			nonce = i
		}
	}
	fmt.Println("Finished Job", minHash, nonce, data, lower, upper)
	return minHash, nonce
}

//send out message
func sendMsg(lspClient lsp.Client, msg *bitcoin.Message) error {
	fmt.Println("Send msg:", msg)
	buf, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("Failed to parse message")
		return err
	}
	if err := lspClient.Write(buf); err != nil {
		fmt.Println("Failed to write to server")
		return err
	}
	return nil
}
