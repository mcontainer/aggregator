package sse

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type Broker struct {
	Notifier         chan []byte
	incomingClients  chan chan []byte
	outcomingClients chan chan []byte
	clients          map[chan []byte]bool
}

func newSSE() *Broker {
	broker := &Broker{
		Notifier:         make(chan []byte, 1),
		incomingClients:  make(chan chan []byte),
		outcomingClients: make(chan chan []byte),
		clients:          make(map[chan []byte]bool),
	}
	go broker.listen()
	return broker
}

func Start(streamer *chan string) {
	log.Info("Starting Server sent event")
	b := newSSE()
	go func() {
		for {
			b.Notifier <- []byte(<-*streamer)
		}
	}()
	http.Handle("/streaming", b)
	log.Fatal("HTTP server error: ", http.ListenAndServe("localhost:1234", nil))
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
		case x := <-b.Notifier:
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
		fmt.Fprintf(w, "data: %s\n\n", <-messageChan)
		flusher.Flush()
	}

}
