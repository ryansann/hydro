package tcp

import (
	"log"
	"net"
	"os"

	"github.com/ryansann/hydro/storage"
	"github.com/ryansann/hydro/storage/hash"
)

// OptionFunc overrides a default option
type OptionFunc func(*options)

type options struct {
	port  string
	log   *log.Logger
	store storage.Storer
}

// Port overrides the default port ":8080"
func Port(p string) OptionFunc {
	return func(opts *options) {
		opts.port = p
	}
}

// Store overrides the server's default storage engine to the passed in value
func Store(s storage.Storer) OptionFunc {
	return func(opts *options) {
		opts.store = s
	}
}

// Logger overrides default logger to the one passed in
func Logger(l *log.Logger) OptionFunc {
	return func(opts *options) {
		opts.log = l
	}
}

// Server is a tcp server that receives commands and interacts
// with a storage layer
type Server struct {
	port  string
	log   *log.Logger
	store storage.Storer
	close chan struct{}
	conns chan net.Conn
}

// NewServer returns a configured Server instance ready to start serving
func NewServer(opts ...OptionFunc) (*Server, error) {
	store, err := hash.NewIndex()
	if err != nil {
		return nil, err
	}

	cfg := &options{
		port:  ":8080",
		log:   log.New(os.Stdout, "server ", log.LstdFlags|log.Lshortfile),
		store: store,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &Server{
		port:  cfg.port,
		log:   cfg.log,
		store: cfg.store,
		close: make(chan struct{}),
		conns: make(chan net.Conn),
	}, nil
}

// Serve starts the server
func (s *Server) Serve() {
	s.log.Println("accepting connections")

	ln, err := net.Listen("tcp", s.port)
	if err != nil {
		s.log.Fatalf("fatal: %v\n", err)
	}

	go s.acceptConns(ln)

	for {
		select {
		case <-s.close:
			s.log.Println("closing")

			err := ln.Close()
			if err != nil {
				s.log.Printf("error closing listener: %v\n", err)
			}

			err = s.store.Close()
			if err != nil {
				s.log.Printf("error closing store: %v\n", err)
			}

			return
		case c := <-s.conns:
			go s.handleConn(c)
		}
	}
}

// Close triggers the server to stop accepting connections and close its store
func (s *Server) Close() error {
	defer close(s.close)
	s.close <- struct{}{}
	return nil
}

func (s *Server) acceptConns(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			s.log.Printf("error accepting connection: %v\n", err)
			continue
		}

		s.conns <- c
	}
}

func (s *Server) handleConn(c net.Conn) {
	s.log.Println("handling connection")
}
