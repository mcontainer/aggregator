package main

import (
	pb "docker-visualizer/docker-graph-aggregator/events"
	"docker-visualizer/docker-graph-aggregator/graph"
	"docker-visualizer/docker-graph-aggregator/sse"
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

type server struct {
	graph    *graph.GraphClient
	streamer *chan string
}

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

		resp, err := s.graph.Connect(pb.Event{
			IpSrc: event.IpSrc,
			IpDst: event.IpDst,
			Stack: "microservice",
			Size:  event.Size,
		})

		if err != nil {
			log.Fatal(err)
		}

		log.Info("Add data to graph")

		*s.streamer <- event.IpSrc + " - " + event.IpDst + " - " + event.Stack

		fmt.Println(resp)

	}
}

func main() {

	streamPipe := make(chan string)

	go sse.Start(&streamPipe)

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

	listener, err := net.Listen("tcp", ":10000")
	if err != nil {
		log.Fatal(err)
	}

	log.Info("Starting grpc server")
	grpcServer := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcServer, &server{graph: graph, streamer: &streamPipe})
	grpcServer.Serve(listener)

}
