package main

import (
	pb "docker-visualizer/docker-graph-aggregator/events"
	"docker-visualizer/docker-graph-aggregator/graph"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net"
	"os"
)

const (
	dgraph = "127.0.0.1:9080"
)

type server struct{}

func (s *server) PushEvent(stream pb.EventService_PushEventServer) error {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"ipSrc":       event.IpSrc,
			"ipDst":       event.IpDst,
			"packet size": event.Size,
			"stack":       event.Stack,
		}).Info("Received")

	}
}

func main() {

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

	graph := graph.NewGraphClient(conn, clientDir)

	defer graph.Close()

	resp, err := graph.Connect(pb.Event{
		IpSrc: "10.0.0.5",
		IpDst: "10.0.0.8",
		Stack: "microservice",
		Size:  10,
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp)

	listener, err := net.Listen("tcp", ":10000")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcServer, &server{})
	grpcServer.Serve(listener)

}
