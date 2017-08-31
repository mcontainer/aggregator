package graph

import (
	"context"
	pb "docker-visualizer/docker-graph-aggregator/events"
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
	Root []node `dgraph:"node"`
}

func NewGraphClient(connection *grpc.ClientConn, dir string) *GraphClient {
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

func (g *GraphClient) AddEdge(r *client.Req, n client.Node, pred string, v interface{}) (err error) {
	e := n.Edge(pred)
	switch v.(type) {
	case string:
		err = e.SetValueString(v.(string))
		if err != nil {
			return err
		}
		err = r.Set(e)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("Unknow type")
	}
}

func (g *GraphClient) AddEdges(r *client.Req, n client.Node, p map[string]interface{}) (err error) {
	for k, v := range p {
		if err = g.AddEdge(r, n, k, v); err != nil {
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

func (g *GraphClient) Connect(event pb.Event) (*protos.Response, error) {
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
		err = g.AddEdges(&req, newNode, params)
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
		err = g.AddEdges(&req, targetNode, params)
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
		targetNode = g.cli.NodeUid(target.UID)
	}

	req = client.Req{}
	fmt.Println(targetNode)
	fmt.Println(newNode)

	e := targetNode.ConnectTo("connected", newNode)
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
	  node(func: eq(ip, $ip)) {
		_uid_
		name
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
	  node(func: anyofterms(stack, $stack)) {
	  	_uid_
		expand(_all_) {
		  ip
		  name
		  connected
		}
	  }
	}`
	m := make(map[string]string)
	m["$stack"] = stack
	req := g.CreateRequestWithVariable(q, m)
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
