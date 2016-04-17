package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
	"strconv"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}
	hostport := os.Args[1]
	msg := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Println("Failed to parse maxNoce")
	}
	lspClient, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to create client")
		return
	}
	// send request
	if err := sendMsg(lspClient, bitcoin.NewRequest(msg, 0, maxNonce)); err != nil {
		return
	}
	// read result
	buf, err := lspClient.Read()
	if err != nil {
		printDisconnected()
		return
	}

	resultMsg := &bitcoin.Message{}
	if err := json.Unmarshal(buf, resultMsg); err != nil {
		fmt.Println("Failed to unmarshal")
		return
	}
	
	fmt.Println("Rcv: ", resultMsg)
	printResult(strconv.FormatUint(resultMsg.Hash, 10), strconv.FormatUint(resultMsg.Nonce, 10))
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

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
