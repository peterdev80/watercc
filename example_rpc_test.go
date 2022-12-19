package watercc_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"watercc"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var rpcserv = "rpc_command"

func createServer() *watercc.RPCServer {
	rpc, err := watercc.NewRPCServer(amqpURI, rpcserv)
	if err != nil {
		panic(err)
	}

	hf := func(m *message.Message) (*message.Message, error) {
		st := strings.ToUpper(string(m.Payload))

		return message.NewMessage(watermill.NewUUID(), []byte(st)), nil
	}

	rpc.AddHandler("UpperCase", hf)

	return rpc
}

func ExampleRpc() {
	server := createServer()
	defer server.Close()

	client, err := watercc.NewRPCClient(amqpURI, rpcserv)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	m, err := client.Send(ctx, "UpperCase", []byte("Hello world"))

	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(string(m.Payload))
	}

	// Output:
	// HELLO WORLD
}
