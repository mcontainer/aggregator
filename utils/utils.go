package utils

import (
	"io/ioutil"
	"net"
	"google.golang.org/grpc"
	"docker-visualizer/aggregator/log"
)

const (
	DGRAPH_ENDPOINT = "127.0.0.1:9080"
)

func SetupGrpcConnection() *grpc.ClientConn {
	c, e := grpc.Dial(DGRAPH_ENDPOINT, grpc.WithInsecure())
	if e != nil {
		log.WithField("Error", e.Error()).Fatal("Cannot open grpc connection to the database")
	}
	return c
}

func SetupDatabaseDir() string {
	d, e := ioutil.TempDir("", "client_")
	if e != nil {
		log.WithField("Error", e.Error()).Fatal("Cannot create temporary database directory")
	}
	return d
}

func SetupGrpcListener() net.Listener {
	l, e := net.Listen("tcp", ":10000")
	if e != nil {
		log.WithField("Error", e).Fatal("Cannot create grpc listener")
	}
	return l
}
