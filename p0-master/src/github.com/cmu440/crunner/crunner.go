package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's echoed response to standard output. Whether or
// not you add any code to this file will not affect your grade.
func main() {
	// initialize client
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(defaultPort))
	if err != nil {
		panic(err.Error())
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	// start reading
	go func() {
		for {
			reader := bufio.NewReader(os.Stdin)
			if input, err := reader.ReadBytes('\n'); err != nil {
				panic(err.Error())
				return
			} else {
				// send to server with an extra line
				if _, err := conn.Write(input); err != nil {
					panic(err.Error())
					return
				}
			}
		}
	}()

	// start writing
	go func() {
		for {
			reader := bufio.NewReader(conn)
			// read from server
			if msg, err := reader.ReadString('\n'); err != nil {
				panic(err.Error())
				return
			} else {
				// print message from server
				fmt.Print(msg)
			}
		}
	}()

	// Block forever.
	select {}
}
