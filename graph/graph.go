package graph

import (
	"context"
	pb "docker-visualizer/proto/containers"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type GraphClient struct {
	cli *client.Dgraph
}

type node struct {
	UID       uint64 `dgraph:"_uid_"`
	Ip        string `dgraph:"ip"`
	Name      string `dgraph:"name"`
	Stack     string `dgraph:"stack"`
	Connected []node `dgraph:"connected"`
}

type rootNode struct {
	Root []node `dgraph:"recurse"`
}

func NewGraphClient(connection *grpc.ClientConn, dir string) *GraphClient {
	log.Info("Creating a graph client")
	return &GraphClient{cli: client.NewDgraphClient([]*grpc.ClientConn{connection}, client.DefaultOptions, dir)}
}

func (g *GraphClient) CreateRequest(q string) client.Req {
	req := client.Req{}
	req.SetQuery(q)
	return req
}

func (g *GraphClient) CreateRequestWithVariable(q string, v map[string]string) client.Req {
	req := client.Req{}
	req.SetQueryWithVariables(q, v)
	return req
}

func (g *GraphClient) AddFacet(e *client.Edge, key string, value string) {
	e.AddFacet(key, value)
}

func (g *GraphClient) AddEdge(n client.Node, pred string, v interface{}) (*client.Edge, error) {
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
		return nil, errors.New("Unknow type")
	}
}

func (g *GraphClient) AddEdges(n client.Node, p map[string]interface{}) (array []*client.Edge, err error) {
	for k, v := range p {
		e, err := g.AddEdge(n, k, v)
		if err != nil {
			return nil, err
		}
		array = append(array, e)
	}
	return array, nil
}

func (g *GraphClient) AddToRequest(req *client.Req, array []*client.Edge) (err error) {
	for _, e := range array {
		err = req.Set(*e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GraphClient) Exist(stack, ip, name string) (bool, error) {
	q := `{
	  exist(func: eq(stack, $stack)) @filter(eq(ip, $ip) and eq(name, $name)) {
		_uid_
		name
	  }
	}`
	p := make(map[string]string)
	p["$stack"] = stack
	p["$ip"] = ip
	p["$name"] = name
	req := g.CreateRequestWithVariable(q, p)
	resp, err := g.run(req)
	if err != nil {
		return false, err
	}
	return len(resp.N[0].Children) > 0, nil
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
	if e != nil {
		return e
	}
	array, e := g.AddEdges(n, params)
	if e != nil {
		return e
	}
	e = g.AddToRequest(&req, array)
	if e != nil {
		return e
	}
	if _, e := g.run(req); e != nil {
		return e
	}
	return nil
}

func (g *GraphClient) Connect(event pb.ContainerEvent) (*protos.Response, error) {
	req := client.Req{}
	var newNode client.Node
	var targetNode client.Node
	var err error
	exist, _ := g.Exist(event.Stack, event.IpDst, "core")
	if !exist {
		log.Info("Creating new node")
		newNode, err = g.cli.NodeBlank("")
		params := make(map[string]interface{})
		params["name"] = "core"
		params["stack"] = event.Stack
		params["ip"] = event.IpDst
		if err != nil {
			return nil, err
		}
		array, err := g.AddEdges(newNode, params)
		if err != nil {
			return nil, err
		}
		err = g.AddToRequest(&req, array)
		if err != nil {
			return nil, err
		}
		if _, err := g.run(req); err != nil {
			return nil, err
		}
		r, _ := g.FindNodeByIp(event.IpDst)
		newNode = g.cli.NodeUid(r.UID)
		log.Info("OK")
	} else {
		r, err := g.FindNodeByIp(event.IpDst)
		if err != nil {
			return nil, err
		}
		newNode = g.cli.NodeUid(r.UID)

	}

	req = client.Req{}
	exist, _ = g.Exist(event.Stack, event.IpSrc, "Client")

	if !exist {
		log.Info("Creating target node")
		targetNode, err = g.cli.NodeBlank("")
		params := make(map[string]interface{})
		params["name"] = "Client"
		params["stack"] = event.Stack
		params["ip"] = event.IpSrc
		if err != nil {
			return nil, err
		}
		array, err := g.AddEdges(targetNode, params)
		if err != nil {
			return nil, err
		}
		err = g.AddToRequest(&req, array)
		if err != nil {
			return nil, err
		}
		if _, err := g.run(req); err != nil {
			return nil, err
		}
		r, _ := g.FindNodeByIp(event.IpSrc)
		targetNode = g.cli.NodeUid(r.UID)
		log.Info("OK")
	} else {
		target, err := g.FindNodeByIp(event.IpSrc)
		if err != nil {
			return nil, err
		}
		fmt.Println(*target)
		targetNode = g.cli.NodeUid(target.UID)
	}

	req = client.Req{}

	e := targetNode.ConnectTo("connected", newNode)
	//e.AddFacet("weight", strconv.Itoa(36))
	err = req.Set(e)
	if err != nil {
		return nil, err
	}
	resp, err := g.run(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (g *GraphClient) FindNodeByIp(ip string) (n *node, err error) {
	q := `{
	  recurse(func: eq(ip, $ip)) {
		_uid_
		name
		ip
		stack
		connected
	  }
	}`
	m := make(map[string]string)
	m["$ip"] = ip
	req := g.CreateRequestWithVariable(q, m)
	resp, err := g.run(req)
	if err != nil {
		return nil, err
	}
	var node rootNode
	err = client.Unmarshal(resp.N, &node)
	if err != nil {
		return nil, err
	}
	return &node.Root[0], nil
}

func (g *GraphClient) FindByStack(stack string) (nodes *[]node, err error) {
	q := `{
	  recurse(func: eq(stack, $stack)) {
		_uid_
		name
		stack
		connected
		ip
	  }
	}`
	m := make(map[string]string)
	m["$stack"] = stack
	req := g.CreateRequestWithVariable(q, m)
	resp, err := g.run(req)
	if err != nil {
		return nil, err
	}
	fmt.Println(resp)
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
