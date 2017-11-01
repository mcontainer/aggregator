package operations

import (
	log "github.com/sirupsen/logrus"
	pb "docker-visualizer/proto/containers"
	"encoding/json"
	"golang.org/x/net/context"
	"docker-visualizer/aggregator/graph"
	"io"
	"google.golang.org/grpc"
)

type clientEvent struct {
	Action  string      `json:"action"`
	Payload interface{} `json:"payload"`
}

type server struct {
	graph    *graph.GraphClient
	streamer *chan []byte
}

func NewGrpcOperations(stream *chan []byte, graph *graph.GraphClient) *grpc.Server {
	grpcServer := grpc.NewServer()
	pb.RegisterContainerServiceServer(grpcServer, &server{graph: graph, streamer: stream})
	return grpcServer
}

func (s *server) AddNode(ctx context.Context, containers *pb.ContainerInfo) (*pb.Response, error) {
	log.WithField("Node", containers).Info("Inserting node")
	exist, e := s.graph.ExistID(containers.Id)
	if e != nil {
		log.WithField("error", e).Warn("Add node")
		return nil, e
	}
	if !exist {
		e := s.graph.InsertNode(containers)
		event := clientEvent{
			Action:  "ADD",
			Payload: containers,
		}
		b, err := json.Marshal(event)
		if err != nil {
			log.Warn(err)
			return nil, err
		}
		*s.streamer <- b
		if e != nil {
			return nil, e
		}
	} else {
		log.Info("Node " + containers.Id + " already exists")
	}
	return &pb.Response{Success: true}, nil
}


func (s *server) RemoveNode(ctx context.Context, containers *pb.ContainerID) (*pb.Response, error) {
	exist, _ := s.graph.ExistID(containers.Id)
	if exist {
		e := s.graph.DeleteNode(containers.Id)
		if e != nil {
			log.Warn(e)
		} else {
			event := clientEvent{
				Action:  "DELETE",
				Payload: containers,
			}
			b, err := json.Marshal(event)
			if err != nil {
				log.Warn(err)
			} else {
				*s.streamer <- b
			}
		}
	}
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

		connection, err := s.graph.Connect(event)
		if err != nil {
			log.Warn(err)
		} else {
			data := clientEvent{
				Action:  "CONNECT",
				Payload: connection,
			}
			b, err := json.Marshal(data)
			if err != nil {
				log.Warn(err)
			} else {
				*s.streamer <- b
			}
		}

	}
}