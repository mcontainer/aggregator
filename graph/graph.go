package graph

import (
	"bytes"
	"context"
	"docker-visualizer/aggregator/log"
	pb "docker-visualizer/proto/containers"
	"errors"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"google.golang.org/grpc"
	"strconv"
)

type GraphClient struct {
	cli *client.Dgraph
}

type Node struct {
	UID       uint64 `dgraph:"_uid_"`
	Id        string `dgraph:"id"`
	Ip        string `dgraph:"ip"`
	Name      string `dgraph:"name"`
	Stack     string `dgraph:"stack"`
	Connected []Node `dgraph:"connected"`
	Parents   []Node `dgraph:"~connected"`
	Network   string `dgraph:"network"`
	Service   string `dgraph:"service"`
	Host      string `dgraph:"host"`
}

type rootNode struct {
	Root []Node `dgraph:"recurse"`
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
	DeleteNode(id string) error
	InsertNode(info *pb.ContainerInfo) error
	Connect(event *pb.ContainerEvent) (*Connection, error)
	FindNodeById(id string) (n *Node, err error)
	FindNodeByIp(ip string) (n *Node, err error)
	FindByStack(stack string) (nodes *[]Node, err error)
	Close()
}

func NewGraphClient(connection *grpc.ClientConn, dir string) IGraph {
	log.Info("Creating a graph client")
	graph := &GraphClient{cli: client.NewDgraphClient([]*grpc.ClientConn{connection}, client.DefaultOptions, dir)}
	if graph.InitializedSchema() != nil {
		log.Fatal("Error while initializing schema")
	}
	return graph
}

func (g *GraphClient) InitializedSchema() error {
	req := client.Req{}
	schema := `
		name: string @index(exact, term) .
		ip: string @index(exact, term) .
		stack: string @index(exact, term) .
		id: string @index(exact, term) .
		network: string @index(exact, term) .
		service: string @index(exact, term) .
		host: string @index(exact, term) .
		connected: uid @count @reverse .
	`
	req.SetSchema(schema)
	_, e := g.run(req)
	if e != nil {
		return e
	}
	log.Info("Schema has been added")
	return nil
}

func (g *GraphClient) createRequest(q string) client.Req {
	req := client.Req{}
	req.SetQuery(q)
	return req
}

func (g *GraphClient) createRequestWithVariable(q string, v map[string]string) client.Req {
	req := client.Req{}
	req.SetQueryWithVariables(q, v)
	return req
}

func (g *GraphClient) addFacet(e *client.Edge, key string, value string) {
	e.AddFacet(key, value)
}

func (g *GraphClient) addEdge(n client.Node, pred string, v interface{}) (*client.Edge, error) {
	e := n.Edge(pred)
	switch v.(type) {
	case string:
		err := e.SetValueString(v.(string))
		if err != nil {
			return nil, err
		}
		return &e, nil
	case int64:
		err := e.SetValueInt(v.(int64))
		if err != nil {
			return nil, err
		}
		return &e, nil
	default:
		return nil, errors.New("unknown type")
	}
}

func (g *GraphClient) addEdges(n client.Node, p map[string]interface{}) (array []*client.Edge, err error) {
	for k, v := range p {
		e, err := g.addEdge(n, k, v)
		if err != nil {
			return nil, err
		}
		array = append(array, e)
	}
	return array, nil
}

func (g *GraphClient) addToRequest(req *client.Req, array []*client.Edge) (err error) {
	for _, e := range array {
		err = req.Set(*e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GraphClient) ExistID(id string) (bool, error) {
	q := `{
	  exist(func: eq(id, $id)) {
		_uid_
	  }
	}`
	p := make(map[string]string)
	p["$id"] = id
	req := g.createRequestWithVariable(q, p)
	resp, err := g.run(req)
	if err != nil {
		return false, err
	}
	if len(resp.N) == 0 {
		return false, errors.New("response array is empty")
	}
	return len(resp.N[0].Children) > 0, nil
}

func (g *GraphClient) Exist(stack, ip, host string) (bool, error) {
	q := `{
	  exist(func: eq(stack, $stack)) @filter(eq(ip, $ip) and eq(host, $host)) {
		_uid_
		name
	  }
	}`
	p := make(map[string]string)
	p["$stack"] = stack
	p["$ip"] = ip
	p["$host"] = host
	req := g.createRequestWithVariable(q, p)
	resp, err := g.run(req)
	if err != nil {
		return false, err
	}
	if len(resp.N) == 0 {
		return false, errors.New("response array is empty")
	}
	return len(resp.N[0].Children) > 0, nil
}

func (g *GraphClient) DeleteNode(id string) error {
	log.WithField("id", id).Info("Graph:: DeleteNode")
	var b bytes.Buffer
	n, e := g.FindNodeById(id)
	if e != nil {
		return e
	}

	log.Info("Start deleting node " + id)
	for _, v := range n.Parents {
		b.WriteString("<0x" + strconv.FormatUint(v.UID, 16) + "> <connected> <0x" + strconv.FormatUint(n.UID, 16) + "> .\n")
	}
	b.WriteString("<0x" + strconv.FormatUint(n.UID, 16) + "> * * .\n")
	q := `mutation {
	delete {
		` + b.String() + `
	}
	}
	`
	req := g.createRequest(q)
	if _, err := g.run(req); err != nil {
		return err
	}

	return nil
}

func (g *GraphClient) InsertNode(info *pb.ContainerInfo) error {
	req := client.Req{}
	n, e := g.cli.NodeBlank("")
	params := make(map[string]interface{})
	params["name"] = info.Name
	params["id"] = info.Id
	params["service"] = info.Service
	params["ip"] = info.Ip
	params["network"] = info.Network
	params["stack"] = info.Stack
	params["host"] = info.Host
	if e != nil {
		return e
	}
	array, e := g.addEdges(n, params)
	if e != nil {
		return e
	}
	e = g.addToRequest(&req, array)
	if e != nil {
		return e
	}
	if _, e := g.run(req); e != nil {
		return e
	}
	return nil
}

func (g *GraphClient) Connect(event *pb.ContainerEvent) (*Connection, error) {
	req := client.Req{}
	var newNode client.Node
	var targetNode client.Node
	var err error
	r, err := g.FindNodeByIp(event.IpDst)
	if err != nil {
		return nil, err
	}
	newNode = g.cli.NodeUid(r.UID)

	target, err := g.FindNodeByIp(event.IpSrc)
	if err != nil {
		return nil, err
	}
	targetNode = g.cli.NodeUid(target.UID)
	e := targetNode.ConnectTo("connected", newNode)
	err = req.Set(e)
	if err != nil {
		return nil, err
	}
	if _, err := g.run(req); err != nil {
		return nil, err
	}

	return &Connection{Src: r.Id, Dst: target.Id, Size: event.Size}, nil
}

func (g *GraphClient) FindNodeById(id string) (n *Node, err error) {
	q := `{
	  recurse(func: eq(id, $id)) {
		_uid_
		name
		id
		ip
		stack
		network
		host
		service
		connected
		~connected
	  }
	}`
	m := make(map[string]string)
	m["$id"] = id
	req := g.createRequestWithVariable(q, m)
	resp, err := g.run(req)
	if err != nil {
		return nil, err
	}
	var node rootNode
	err = client.Unmarshal(resp.N, &node)
	if err != nil {
		return nil, err
	}
	if len(node.Root) > 0 {
		return &node.Root[0], nil
	}
	return nil, errors.New("Not found node with id " + id)
}

func (g *GraphClient) FindNodeByIp(ip string) (n *Node, err error) {
	q := `{
	  recurse(func: eq(ip, $ip)) {
		_uid_
		name
		id
		ip
		stack
		network
		host
		service
		connected
		~connected
	  }
	}`
	m := make(map[string]string)
	m["$ip"] = ip
	req := g.createRequestWithVariable(q, m)
	resp, err := g.run(req)
	if err != nil {
		return nil, err
	}
	var node rootNode
	err = client.Unmarshal(resp.N, &node)
	if err != nil {
		return nil, err
	}
	if len(node.Root) > 0 {
		return &node.Root[0], nil
	}
	return nil, errors.New("Not found node with ip " + ip)
}

func (g *GraphClient) FindByStack(stack string) (nodes *[]Node, err error) {
	q := `{
	  recurse(func: eq(stack, $stack)) {
		_uid_
		name
		id
		ip
		stack
		network
		host
		service
		connected
		~connected
	  }
	}`
	m := make(map[string]string)
	m["$stack"] = stack
	req := g.createRequestWithVariable(q, m)
	resp, err := g.run(req)
	if err != nil {
		return nil, err
	}
	var root rootNode
	err = client.Unmarshal(resp.N, &root)
	if err != nil {
		log.Fatal(err)
	}
	return &root.Root, nil
}

func (g *GraphClient) run(request client.Req) (*protos.Response, error) {
	return g.cli.Run(context.Background(), &request)
}

func (g *GraphClient) Close() {
	g.cli.Close()
}
