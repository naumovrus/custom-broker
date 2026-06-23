package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

/*
broker - структура нашего брокера сообщений

queue – сама очередь;
mu – мьютекс
notify – это мапа, где ключ - название очереди, а значение это небуф канал.

notify нужен для обработки timeout – если он 0, то мы ждем ответ до тех пор пока сообщение не появится,
если нет то ждем заданное кол-во времени до тех пор пока сообщение не появится, иначе выходим с ошибкой (404)
*/
type broker struct {
	queue   map[string][]string // - key: name of q; - val: q values
	mu      sync.Mutex
	notifiy map[string]chan struct{} // key: name of q; val - notify chanel
}

type iBroker interface {
	SetMessage(q, mes string)
	GetMessage(q string, timeout time.Duration) (string, error)
}

type handler struct {
	mbroker iBroker
}

func main() {
	port := flag.String("port", "8080", "listenning port")

	flag.Parse()

	//init broker
	mbroker := NewBroker()

	//init handler
	hand := NewHandler(mbroker)

	//set up http-service
	mux := http.NewServeMux()
	registerRoutes(mux, hand)

	log.Printf("Сервер запущен на http://localhost:%s", *port)
	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", *port), // надо парсить с флага
		Handler:      mux,
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  0,
	}

	log.Fatal(server.ListenAndServe())

}

// BROKER LOGIC
func NewBroker() *broker {
	queue := make(map[string][]string)
	notify := make(map[string]chan struct{})
	return &broker{
		mu:      sync.Mutex{},
		queue:   queue,
		notifiy: notify,
	}
}

// SetMessage - сетим в брокер сообщение,
// и закрываем канал с очередью, на случай если SetMessage ждет сообщение
func (b *broker) SetMessage(q, mes string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queue[q] == nil {
		b.queue[q] = make([]string, 0)
		b.notifiy[q] = make(chan struct{})
	}

	b.queue[q] = append(b.queue[q], mes)
	close(b.notifiy[q])

	b.notifiy[q] = make(chan struct{})
}

func (b *broker) GetMessage(q string, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)

	for {
		b.mu.Lock()
		resp := ""

		if len(b.queue[q]) > 0 {
			// pop
			resp, b.queue[q] = b.queue[q][0], b.queue[q][1:]
			b.mu.Unlock()
			return resp, nil
		}

		// на случай если мы запросили сообщение из ПОКА не существующей очереди
		if b.queue[q] == nil {
			b.queue[q] = make([]string, 0)
		}
		if b.notifiy[q] == nil {
			b.notifiy[q] = make(chan struct{})
		}
		ch := b.notifiy[q]
		b.mu.Unlock()

		if timeout == 0 {
			<-ch
			continue
		}

		select {
		case <-ch:
			continue

		case <-deadline:
			return "", errors.New("timeout")
		}
	}

}

// HTTP
func NewHandler(mbroker iBroker) *handler {
	return &handler{
		mbroker: mbroker,
	}
}
func registerRoutes(mux *http.ServeMux, h *handler) {
	mux.HandleFunc("PUT /{q}", h.SetMessage)
	mux.HandleFunc("GET /{q}", h.GetMessage)
}

func (h *handler) SetMessage(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("q")
	if queue == "" {
		http.Error(w, "", http.StatusBadRequest) // empty queue
		return
	}
	query := r.URL.Query()
	message := query.Get("v")
	if message == "" {
		http.Error(w, "", http.StatusBadRequest) // empty value
		return
	}

	h.mbroker.SetMessage(queue, message)
	w.WriteHeader(http.StatusOK)
}

func (h *handler) GetMessage(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("q")
	if queue == "" {
		http.Error(w, "", http.StatusBadRequest) // empty queue
		return
	}

	query := r.URL.Query()
	timeoutStr := query.Get("timeout")
	timeout, err := strconv.Atoi(timeoutStr)
	if err != nil {
		timeout = 0
	}

	dur := time.Duration(timeout) * time.Second

	res, err := h.mbroker.GetMessage(queue, dur)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}
