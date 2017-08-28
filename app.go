package main

import (
	pb "docker-visualizer/docker-graph-aggregator/events"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"net"
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

	listener, err := net.Listen("tcp", ":10000")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcServer, &server{})
	grpcServer.Serve(listener)

}
