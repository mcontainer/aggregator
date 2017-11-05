package operations

import (
	"docker-visualizer/aggregator/graph"
	"docker-visualizer/aggregator/log"
	pb "docker-visualizer/proto/containers"
	"encoding/json"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
)

type clientEvent struct {
	Action  string      `json:"action"`
	Payload interface{} `json:"payload"`
}

type server struct {
	graph    graph.IGraph
	streamer *chan []byte
}

const (
	EVENT_ADD     = "ADD"
	EVENT_DELETE  = "DELETE"
	EVENT_CONNECT = "CONNECT"
)

func NewGrpcOperations(stream *chan []byte, graph graph.IGraph) *grpc.Server {
	grpcServer := grpc.NewServer()
	pb.RegisterContainerServiceServer(grpcServer, &server{graph: graph, streamer: stream})
	return grpcServer
}

func setClientEvent(action string, data interface{}) clientEvent {
	return clientEvent{
		Action:  action,
		Payload: data,
	}
}

func (s *server) AddNode(ctx context.Context, containers *pb.ContainerInfo) (*pb.Response, error) {
	log.WithField("Node", containers).Info("Inserting node")
	exist, e := s.graph.ExistID(containers.Id)
	if e != nil {
		log.WithField("error", e).Error("Error while checking if node exist")
		return nil, e
	}
	if !exist {
		e := s.graph.InsertNode(containers)
		if e != nil {
			log.WithField("error", e).Error("Error while inserting node")
			return nil, e
		}
		event := setClientEvent(EVENT_ADD, containers)
		b, err := json.Marshal(event)
		if err != nil {
			log.WithField("error", err).Error("Error while marshalling event client")
			return nil, err
		}
		*s.streamer <- b
	} else {
		log.Info("Node " + containers.Id + " already exists")
	}
	return &pb.Response{Success: true}, nil
}

func (s *server) RemoveNode(ctx context.Context, containers *pb.ContainerID) (*pb.Response, error) {
	exist, e := s.graph.ExistID(containers.Id)
	if e != nil {
		log.WithField("error", e).Error("Error while checking if node exist")
		return nil, e
	}
	if exist {
		e := s.graph.DeleteNode(containers.Id)
		if e != nil {
			log.WithField("error", e).Error("Error while removing node")
			return nil, e
		} else {
			event := setClientEvent(EVENT_DELETE, containers)
			b, err := json.Marshal(event)
			if err != nil {
				log.WithField("error", err).Error("Error while marshalling event client")
				return nil, err
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

		log.
			WithField("ipSrc", event.IpSrc).
			WithField("ipDst", event.IpDst).
			WithField("packet size", event.Size).
			WithField("stack", event.Stack).Info("Received")

		connection, err := s.graph.Connect(event)
		if err != nil {
			log.WithField("error", err).Error("Error while connecting node")
		} else {
			data := setClientEvent(EVENT_CONNECT, connection)
			b, err := json.Marshal(data)
			if err != nil {
				log.WithField("error", err).Error("Error while marshalling event client")
			} else {
				*s.streamer <- b
			}
		}
	}
}
