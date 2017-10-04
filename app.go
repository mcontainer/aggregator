package main

import (
	"docker-visualizer/docker-graph-aggregator/graph"
	"docker-visualizer/docker-graph-aggregator/sse"
	pb "docker-visualizer/proto/containers"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"
)

const (
	dgraph = "127.0.0.1:9080"
)

type clientEvent struct {
	Action  string      `json:"action"`
	Payload interface{} `json:"payload"`
}

type server struct {
	graph    *graph.GraphClient
	streamer *chan []byte
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

func startServer(g *graph.GraphClient) {
	router := httprouter.New()
	router.GET("/topology/:stack", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		resp, err := g.FindByStack(params.ByName("stack"))
		if err != nil {
			log.Fatal(err)
		}
		b, err := json.Marshal(resp)
		if err != nil {
			log.Warn(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write(b)
	})
	log.Fatal(http.ListenAndServe(":8081", router))
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

	go startServer(g)

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
