package main

import (
	"docker-visualizer/aggregator/graph"
	"docker-visualizer/aggregator/sse"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"docker-visualizer/aggregator/operations"
	"docker-visualizer/aggregator/rest"
)

const (
	dgraph = "127.0.0.1:9080"
)

func main() {

	streamChannel := make(chan []byte)

	go sse.Start(&streamChannel)

	conn, err := grpc.Dial(dgraph, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	clientDir, err := ioutil.TempDir("", "client_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(clientDir)

	g := graph.NewGraphClient(conn, clientDir)

	restServer := rest.NewRestServer(g)

	go restServer.Listen()

	err = g.InitializedSchema()

	if err != nil {
		log.Fatal(err)
	}

	defer g.Close()

	listener, err := net.Listen("tcp", ":10000")
	if err != nil {
		log.Fatal(err)
	}

	log.Info("Starting grpc server")

	operations.NewGrpcOperations(&streamChannel, g).Serve(listener)

}
