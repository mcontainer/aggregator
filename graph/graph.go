package graph

import (
	"context"
	pb "docker-visualizer/proto/containers"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type GraphClient struct {
	cli *client.Dgraph
}

type Connection struct {
	Src  string `json:"source"`
	Dst  string `json:"destination"`
	Size uint32 `json:"size"`
}

type IGraph interface {
	InitializedSchema() error
	ExistID(id string) (bool, error)
	Exist(stack, ip, host string) (bool, error)
	FindByStack(stack string) (node []byte, err error)
	FindNodeById(id string) (node []byte, err error)
	FindNodeByIp(ip string) (node []byte, err error)
	DeleteNode(id string) error
	InsertNode(info *pb.ContainerInfo) error
	Connect(event *pb.ContainerEvent) (*Connection, error)
}

func NewGraphClient(connection *grpc.ClientConn) IGraph {
	log.Info("Creating a graph client")
	graph := &GraphClient{cli: client.NewDgraphClient(api.NewDgraphClient(connection))}
	if graph.InitializedSchema() != nil {
		log.Fatal("Error while initializing schema")
	}
	return graph
}

func (g *GraphClient) InitializedSchema() error {
	if err := g.cli.Alter(context.Background(), &api.Operation{
		Schema: `
			name: string @index(exact, term) .
			ip: string @index(exact, term) .
			stack: string @index(exact, term) .
			id: string @index(exact, term) .
			network: string @index(exact, term) .
			service: string @index(exact, term) .
			host: string @index(exact, term) .
			connected: uid @count .
			parent: uid @count .
		`,
	}); err != nil {
		return err
	}
	log.Info("Schema has been added")
	return nil
}

func (g *GraphClient) ExistID(id string) (bool, error) {
	q := `{
	  exist(func: eq(id, $id)) {
		uid
	  }
	}`
	p := make(map[string]string)
	p["$id"] = id
	resp, err := g.cli.NewTxn().QueryWithVars(context.Background(), q, p)
	if err != nil {
		log.Error(err)
		return false, err
	}

	type Root struct {
		Exist []struct {
			Uid string `json:"uid"`
		} `json:"exist"`
	}

	var r Root
	err = json.Unmarshal(resp.GetJson(), &r)
	if err != nil {
		log.Error(err)
	}

	return len(r.Exist) > 0, nil
}

func (g *GraphClient) Exist(stack, ip, host string) (bool, error) {

	q := `{
		  exist(func: eq(stack, $stack)) @filter(eq(ip, $ip) and eq(host, $host)) {
			uid
		  }
		}`
	p := make(map[string]string)
	p["$stack"] = stack
	p["$ip"] = ip
	p["$host"] = host
	resp, err := g.cli.NewTxn().QueryWithVars(context.Background(), q, p)
	if err != nil {
		return false, err
	}

	type Root struct {
		Exist []struct {
			Uid string `json:"uid"`
		} `json:"exist"`
	}
	var r Root
	err = json.Unmarshal(resp.GetJson(), &r)
	if err != nil {
		log.Error(err)
	}
	if len(r.Exist) == 0 {
		return false, errors.New("response array is empty")
	}
	return len(r.Exist) > 0, nil
}

func (g *GraphClient) DeleteNode(id string) error {
	q := `{
		  find(func: eq(id, $id)) {
			uid
			connected {
			  uid
			}
			parent {
			  uid
			}
		  }
		}
	`
	type info struct {
		Uid       string `json:"uid,omitempty"`
		Connected []info `json:"connected,omitempty"`
		Parent    []info `json:"parent,omitempty"`
	}

	type rootNode struct {
		Find []info
	}
	var root rootNode

	param := make(map[string]string)
	param["$id"] = id

	r, e := g.cli.NewTxn().QueryWithVars(context.Background(), q, param)
	if e != nil {
		return e
	}

	e = json.Unmarshal(r.Json, &root)
	if e != nil {
		return e
	}

	if len(root.Find) == 0 {
		log.Info("no result")
		return nil
	}

	txn := g.cli.NewTxn()
	defer txn.Discard(context.Background())
	mu := &api.Mutation{}
	a := make([]info, 0)

	if len(root.Find[0].Connected) > 0 {
		client.DeleteEdges(mu, root.Find[0].Uid, "connected")
		for _, v := range root.Find[0].Connected {
			i := info{
				Uid:    v.Uid,
				Parent: []info{{Uid: root.Find[0].Uid}},
			}
			a = append(a, i)
		}
		b, _ := json.Marshal(a)
		mu.DeleteJson = b
		txn.Mutate(context.Background(), mu)
		txn.Commit(context.Background())
	}

	mu = &api.Mutation{}
	a = make([]info, 0)
	txn2 := g.cli.NewTxn()
	defer txn2.Discard(context.Background())
	if len(root.Find[0].Parent) > 0 {
		client.DeleteEdges(mu, root.Find[0].Uid, "parent")
		for _, v := range root.Find[0].Parent {
			i := info{
				Uid:       v.Uid,
				Connected: []info{{Uid: root.Find[0].Uid}},
			}
			a = append(a, i)
		}
		b, _ := json.Marshal(a)
		mu.DeleteJson = b
		txn2.Mutate(context.Background(), mu)
		txn2.Commit(context.Background())
	}

	txn3 := g.cli.NewTxn()
	defer txn3.Discard(context.Background())
	i := info{Uid: root.Find[0].Uid}
	mu = &api.Mutation{}
	b, _ := json.Marshal(i)
	mu.DeleteJson = b
	txn3.Mutate(context.Background(), mu)
	txn3.Commit(context.Background())

	return nil
}

func (g *GraphClient) InsertNode(info *pb.ContainerInfo) error {
	mu := &api.Mutation{
		CommitNow: true,
	}
	bytes, err := json.Marshal(info)
	if err != nil {
		log.Error(err)
		return err
	}
	mu.SetJson = bytes
	_, err = g.cli.NewTxn().Mutate(context.Background(), mu)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (g *GraphClient) Connect(event *pb.ContainerEvent) (*Connection, error) {

	q := `{
	  dest(func: eq(ip, $dest)) {
		uid
		name
		id
		ip
		stack
		network
		host
		service
		parent {
			uid
			name
			id
			ip
		}
	  }

	  src(func: eq(ip, $src)) {
		uid
		name
		id
		ip
		stack
		network
		host
		service
		connected {
		  uid
		  name
		  id
		  ip
		}
	  }
	}`

	param := make(map[string]string)
	param["$dest"] = event.IpDst
	param["$src"] = event.IpSrc
	r, e := g.cli.NewTxn().QueryWithVars(context.Background(), q, param)
	if e != nil {
		return nil, e
	}
	type node struct {
		Uid       string `json:"uid,omitempty"`
		Id        string `json:"id,omitempty"`
		Connected []node `json:"connected,omitempty"`
		Parent    []node `json:"parent,omitempty"`
	}

	type root struct {
		Dest []node
		Src  []node
	}
	var rootNode root
	e = json.Unmarshal(r.Json, &rootNode)
	if e != nil {
		return nil, e
	}

	rootNode.Src[0].Connected = append(rootNode.Src[0].Connected, rootNode.Dest[0])
	rootNode.Dest[0].Parent = append(rootNode.Dest[0].Parent, rootNode.Src[0])

	fmt.Printf("data upadted: %+v\n", rootNode)

	mu := &api.Mutation{
		CommitNow: true,
	}

	b, e := json.Marshal(rootNode)
	if e != nil {
		return nil, e
	}

	mu.SetJson = b

	_, e = g.cli.NewTxn().Mutate(context.Background(), mu)

	if e != nil {
		return nil, e
	}

	return &Connection{Src: rootNode.Src[0].Id, Dst: rootNode.Dest[0].Id, Size: event.Size}, nil
}

func (g *GraphClient) FindNodeById(id string) (n []byte, err error) {
	q := `{
	  find(func: eq(id, $id)) @recurse(loop: false) {
		uid
		name
		id
		ip
		stack
		network
		host
		service
		connected
		parent
	  }
	}`
	m := make(map[string]string)
	m["$id"] = id
	resp, err := g.cli.NewTxn().QueryWithVars(context.Background(), q, m)
	if err != nil {
		return []byte{}, err
	}

	type info struct {
		Uid       string `json:"uid"`
		Name      string `json:"name"`
		Ip        string `json:"ip"`
		Stack     string `json:"stack"`
		Connected []info `json:"connected"`
		Parent    []info `json:"parent"`
	}

	type rootNode struct {
		Find []info
	}

	var node rootNode
	bytes := resp.GetJson()
	err = json.Unmarshal(bytes, &node)
	if err != nil {
		return []byte{}, err
	}
	if len(node.Find) == 0 {
		return []byte{}, errors.New("Not found node with id " + id)
	}
	return bytes, nil
}

func (g *GraphClient) FindNodeByIp(ip string) (n []byte, err error) {
	q := `{
	  find(func: eq(ip, $ip)) @recurse(loop: false) {
		uid
		name
		id
		ip
		stack
		network
		host
		service
		connected
		parent
	  }
	}`
	m := make(map[string]string)
	m["$ip"] = ip
	resp, err := g.cli.NewTxn().QueryWithVars(context.Background(), q, m)
	if err != nil {
		return nil, err
	}

	type info struct {
		Uid       string `json:"uid"`
		Name      string `json:"name"`
		Ip        string `json:"ip"`
		Stack     string `json:"stack"`
		Connected []info `json:"connected"`
		Parent    []info `json:"parent"`
	}

	type rootNode struct {
		Find []info
	}

	var node rootNode
	bytes := resp.GetJson()
	err = json.Unmarshal(bytes, &node)
	if err != nil {
		return []byte{}, err
	}
	if len(node.Find) == 0 {
		return []byte{}, errors.New("Not found node with ip " + ip)
	}
	return bytes, nil
}

func (g *GraphClient) FindByStack(stack string) (node []byte, err error) {
	q := `{
		  find(func: eq(stack, $stack)) @recurse {
			uid
			name
			id
			ip
			stack
			network
			host
			service
			connected
			parent
		  }
		}`
	m := make(map[string]string)
	m["$stack"] = stack
	resp, err := g.cli.NewTxn().QueryWithVars(context.Background(), q, m)
	if err != nil {
		return nil, err
	}

	type info struct {
		Uid       string `json:"uid"`
		Name      string `json:"name"`
		Ip        string `json:"ip"`
		Stack     string `json:"stack"`
		Connected []info `json:"connected"`
		Parent    []info `json:"parent"`
	}

	type rootNode struct {
		Find []info
	}

	var root rootNode
	bytes := resp.GetJson()
	err = json.Unmarshal(bytes, &root)
	if err != nil {
		log.Error(err)
	}
	if len(root.Find) == 0 {
		return []byte{}, errors.New("response array is empty")
	}

	return bytes, nil
}
