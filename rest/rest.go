package rest

import (
	"docker-visualizer/aggregator/graph"
	"github.com/julienschmidt/httprouter"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type Handler struct {
	graph graph.IGraph
}

type RestServer struct {
	router *httprouter.Router
}

type IRestServer interface {
	Listen()
	GetRouter() *httprouter.Router
}

func (h *Handler) fetchTopologyByStack(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resp, err := h.graph.FindByStack(params.ByName("stack"))
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(resp)
}

func NewRestServer(graph graph.IGraph) IRestServer {
	router := httprouter.New()
	h := &Handler{graph: graph}
	router.GET("/topology/:stack", h.fetchTopologyByStack)
	return &RestServer{router: router}
}

func (s *RestServer) Listen() {
	log.Fatal(http.ListenAndServe(":8081", s.router))
}

func (s *RestServer) GetRouter() *httprouter.Router {
	return s.router
}
