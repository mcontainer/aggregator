package sse

import (
	"fmt"
	"net/http"
	"docker-visualizer/aggregator/log"
)

type Broker struct {
	notifier         chan []byte
	incomingClients  chan chan []byte
	outcomingClients chan chan []byte
	clients          map[chan []byte]bool
}

type IBroker interface {
	ServeHTTP(w http.ResponseWriter, req *http.Request)
	listen()
	close(message chan []byte)
	getNotifier() chan []byte
}

func newSSE() IBroker {
	broker := &Broker{
		notifier:         make(chan []byte, 1),
		incomingClients:  make(chan chan []byte),
		outcomingClients: make(chan chan []byte),
		clients:          make(map[chan []byte]bool),
	}
	go broker.listen()
	return broker
}

func Start(streamer *chan []byte) {
	log.Info("Starting Server sent event")
	b := newSSE()
	go func() {
		for {
			b.getNotifier() <- <-*streamer
		}
	}()
	http.Handle("/streaming", b)
	log.Fatal("HTTP server error: ", http.ListenAndServe(":1234", nil))
}

func (b *Broker) getNotifier() chan []byte {
	return b.notifier
}

func (b *Broker) listen() {
	for {
		select {
		case x := <-b.incomingClients:
			b.clients[x] = true
			log.WithField("clients size", len(b.clients)).Info("New client")
		case x := <-b.outcomingClients:
			delete(b.clients, x)
			log.WithField("clients size", len(b.clients)).Info("Delete client")
		case x := <-b.notifier:
			for channel := range b.clients {
				channel <- x
			}
		}
	}
}

func (b *Broker) close(message chan []byte) {
	b.outcomingClients <- message
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	messageChan := make(chan []byte)

	b.incomingClients <- messageChan

	defer b.close(messageChan)

	notify := w.(http.CloseNotifier).CloseNotify()

	go func() {
		<-notify
		b.outcomingClients <- messageChan
	}()

	for {
		message, opened := <-messageChan
		if !opened {
			break
		}
		fmt.Fprintf(w, "data: %s\n\n", message)
		flusher.Flush()
	}

}
