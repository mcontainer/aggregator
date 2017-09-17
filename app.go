package main

import (
	"docker-visualizer/docker-graph-aggregator/graph"
	"docker-visualizer/docker-graph-aggregator/sse"
	pb "docker-visualizer/proto/containers"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net"
	"os"
	"time"
)

const (
	dgraph = "127.0.0.1:9080"
)

type clientEvent struct {
	Action  string        `json:"action"`
	Payload *[]graph.Node `json:"payload"`
}

type server struct {
	graph    *graph.GraphClient
	streamer *chan []byte
}

func (s *server) AddNode(ctx context.Context, containers *pb.ContainerInfo) (*pb.Response, error) {
	log.WithField("Node", containers).Info("Inserting node")
	exist, e := s.graph.ExistID(containers.Id)
	if e != nil {
		log.Fatal(e)
	}
	if !exist {
		e := s.graph.InsertNode(containers)
		if e != nil {
			return nil, e
		}
	} else {
		log.Info("Node " + containers.Id + " already exists")
	}
	return &pb.Response{Success: true}, nil
}

func (s *server) RemoveNode(ctx context.Context, containers *pb.ContainerID) (*pb.Response, error) {
	e := s.graph.DeleteNode(containers.Id)
	if e != nil {
		return nil, e
	}
	n := graph.Node{
		Id: containers.Id,
	}
	event := clientEvent{
		Action:  "DELETE",
		Payload: &[]graph.Node{n},
	}
	b, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
	}
	*s.streamer <- b
	return &pb.Response{Success: true}, nil
}

func (s *server) StreamContainerEvents(stream pb.ContainerService_StreamContainerEventsServer) error {
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

		if _, err := s.graph.Connect(pb.ContainerEvent{
			IpSrc: event.IpSrc,
			IpDst: event.IpDst,
			Stack: event.Stack,
			Size:  event.Size,
			Host:  event.Host,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {

	streamPipe := make(chan []byte)

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

	g := graph.NewGraphClient(conn, clientDir)
	err = g.InitializedSchema()

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		tick := time.Tick(2 * time.Second)
		for {
			select {
			case <-tick:
				resp, err := g.FindByStack("microservice")
				if err != nil {
					log.Fatal(err)
				}
				event := clientEvent{
					Action:  "NONE",
					Payload: resp,
				}
				b, err := json.Marshal(event)
				if err != nil {
					log.Fatal(err)
				}
				streamPipe <- b
			}
		}
	}()

	defer g.Close()

	listener, err := net.Listen("tcp", ":10000")
	if err != nil {
		log.Fatal(err)
	}

	log.Info("Starting grpc server")
	grpcServer := grpc.NewServer()
	pb.RegisterContainerServiceServer(grpcServer, &server{graph: g, streamer: &streamPipe})
	grpcServer.Serve(listener)

}
