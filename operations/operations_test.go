package operations

import (
	"context"
	"docker-visualizer/aggregator/graph"
	pb "docker-visualizer/proto/containers"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type graphMock struct {
	mock.Mock
}

func (m *graphMock) InitializedSchema() error {
	args := m.Called()
	return args.Error(0)
}

func (m *graphMock) ExistID(id string) (bool, error) {
	args := m.Called(id)
	return args.Bool(0), args.Error(1)
}

func (m *graphMock) Exist(stack, ip, host string) (bool, error) {
	args := m.Called(stack, ip, host)
	return args.Bool(0), args.Error(1)
}

func (m *graphMock) DeleteNode(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

func (m *graphMock) InsertNode(info *pb.ContainerInfo) error {
	args := m.Called(info)
	return args.Error(0)
}

func (m *graphMock) Connect(event *pb.ContainerEvent) (*graph.Connection, error) {
	args := m.Called(event)
	return args.Get(0).(*graph.Connection), args.Error(1)
}

func (m *graphMock) FindNodeById(id string) (n *graph.Node, err error) {
	args := m.Called(id)
	return args.Get(0).(*graph.Node), args.Error(1)
}

func (m *graphMock) FindNodeByIp(ip string) (n *graph.Node, err error) {
	args := m.Called(ip)
	return args.Get(0).(*graph.Node), args.Error(1)
}

func (m *graphMock) FindByStack(stack string) (nodes *[]graph.Node, err error) {
	args := m.Called(stack)
	return args.Get(0).(*[]graph.Node), args.Error(1)
}

func (m *graphMock) Close() {
	m.Called()
}

func TestNewGrpcOperations(t *testing.T) {
	server := NewGrpcOperations(nil, nil)
	assert.NotNil(t, server)
}

func TestServer_AddNode(t *testing.T) {
	stream := make(chan []byte, 1)
	m := &graphMock{}
	s := &server{
		graph:    m,
		streamer: &stream,
	}
	m.On("ExistID", "123").Return(false, nil)
	m.On("InsertNode", mock.AnythingOfType("*containers.ContainerInfo")).Return(nil)

	container := pb.ContainerInfo{
		Id: "123",
	}
	r, e := s.AddNode(context.Background(), &container)

	m.AssertNumberOfCalls(t, "ExistID", 1)
	m.AssertNumberOfCalls(t, "InsertNode", 1)

	a := <-*s.streamer
	assert.Nil(t, e)
	assert.NotNil(t, r)
	assert.NotNil(t, a)
}

func TestServer_AddNodeFailure(t *testing.T) {
	stream := make(chan []byte, 1)
	m := &graphMock{}
	s := &server{
		graph:    m,
		streamer: &stream,
	}
	m.On("ExistID", "123").Return(false, errors.New("error"))

	container := pb.ContainerInfo{
		Id: "123",
	}
	r, e := s.AddNode(context.Background(), &container)

	m.AssertNumberOfCalls(t, "ExistID", 1)

	assert.Nil(t, r)
	assert.NotNil(t, e)
	assert.Equal(t, "error", e.Error())
}
