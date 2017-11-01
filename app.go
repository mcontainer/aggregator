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
	DGRAPH_ENDPOINT = "127.0.0.1:9080"
)

func setupGrpcConnection() *grpc.ClientConn {
	c, e := grpc.Dial(DGRAPH_ENDPOINT, grpc.WithInsecure())
	if e != nil {
		log.WithField("Error", e.Error()).Fatal("Cannot open grpc connection to the database")
	}
	return c
}

func setupDatabaseDir() string {
	d, e := ioutil.TempDir("", "client_")
	if e != nil {
		log.WithField("Error", e.Error()).Fatal("Cannot create temporary database directory")
	}
	return d
}

func setupGrpcListener() net.Listener {
	l, e := net.Listen("tcp", ":10000")
	if e != nil {
		log.WithField("Error", e).Fatal("Cannot create grpc listener")
	}
	return l
}

func main() {

	streamChannel := make(chan []byte)

	go sse.Start(&streamChannel)

	conn := setupGrpcConnection()
	defer conn.Close()

	clientDir := setupDatabaseDir()
	defer os.RemoveAll(clientDir)

	g := graph.NewGraphClient(conn, clientDir)
	restServer := rest.NewRestServer(g)

	go restServer.Listen()

	defer g.Close()

	listener := setupGrpcListener()

	log.Info("Starting grpc server")

	operations.NewGrpcOperations(&streamChannel, g).Serve(listener)

}
