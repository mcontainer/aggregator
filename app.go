package main

import (
	"docker-visualizer/aggregator/graph"
	"docker-visualizer/aggregator/log"
	"docker-visualizer/aggregator/operations"
	"docker-visualizer/aggregator/rest"
	"docker-visualizer/aggregator/sse"
	"docker-visualizer/aggregator/utils"
	"docker-visualizer/aggregator/version"
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

	g := graph.NewGraphClient(conn)
	restServer := rest.NewRestServer(g)

	go restServer.Listen()

	listener := utils.SetupGrpcListener()

	log.Info("Starting grpc server")

	operations.NewGrpcOperations(&streamChannel, g).Serve(listener)

}
