package rest

import (
	"docker-visualizer/aggregator/graph"
	pb "docker-visualizer/proto/containers"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
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

func TestNewRestServer(t *testing.T) {
	server := NewRestServer(&graphMock{})
	assert.NotNil(t, server)
}

func TestFetchTopologyOK(t *testing.T) {
	m := graphMock{}
	n := []graph.Node{
		{
			Id: "123",
		},
	}
	m.On("FindByStack", "toto").Return(&n, nil)

	server := NewRestServer(&m)
	w := httptest.NewRecorder()

	req, _ := http.NewRequest("GET", "/topology/toto", nil)

	server.GetRouter().ServeHTTP(w, req)

	m.AssertNumberOfCalls(t, "FindByStack", 1)

	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), "123")

}

func TestFetchTopology500(t *testing.T) {
	m := graphMock{}
	m.On("FindByStack", "toto").Return(&[]graph.Node{}, errors.New("custom error"))

	server := NewRestServer(&m)

	req, _ := http.NewRequest("GET", "/topology/toto", nil)
	w := httptest.NewRecorder()
	server.GetRouter().ServeHTTP(w, req)

	assert.Equal(t, 500, w.Code)
	assert.Contains(t, w.Body.String(), "custom error")
}
