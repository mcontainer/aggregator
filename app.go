package main

import (
	"docker-visualizer/aggregator/graph"
	"docker-visualizer/aggregator/sse"
	log "github.com/sirupsen/logrus"
	"os"
	"docker-visualizer/aggregator/operations"
	"docker-visualizer/aggregator/rest"
	"docker-visualizer/aggregator/version"
	"docker-visualizer/aggregator/utils"
)

var (
	VERSION string
	COMMIT  string
	BRANCH  string
)

func init() {
	version.Info(VERSION, COMMIT, BRANCH)
}

func main() {

	streamChannel := make(chan []byte)

	go sse.Start(&streamChannel)

	conn := utils.SetupGrpcConnection()
	defer conn.Close()

	clientDir := utils.SetupDatabaseDir()
	defer os.RemoveAll(clientDir)

	g := graph.NewGraphClient(conn, clientDir)
	restServer := rest.NewRestServer(g)

	go restServer.Listen()

	defer g.Close()

	listener := utils.SetupGrpcListener()

	log.Info("Starting grpc server")

	operations.NewGrpcOperations(&streamChannel, g).Serve(listener)

}
