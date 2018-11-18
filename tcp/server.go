package tcp

import (
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ryansann/hydro/storage"
	"github.com/ryansann/hydro/storage/hash"
)

// OptionFunc overrides a default option
type OptionFunc func(*options)

type options struct {
	port            string
	log             *log.Logger
	store           storage.Storer
	readtimeout     time.Duration
	shutdowntimeout time.Duration
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

// ReadTimeout overrides the default read timeout to the duration passed in
// If overriding ShutdownTimeout as well, ReadTimeout should be greater than ShutdownTimeout
func ReadTimeout(t time.Duration) OptionFunc {
	return func(opts *options) {
		opts.readtimeout = t
	}
}

// ShutdownTimeout overrides the default value for how long the server will wait for connections
// to finish being handled before forcefully shutting down (Server.Serve exits).
// If ReadTimeout > ShutdownTimeout there may be cases where connections don't get enough time to close
// before Server.Serve exits.
func ShutdownTimeout(t time.Duration) OptionFunc {
	return func(opts *options) {
		opts.shutdowntimeout = t
	}
}

// Server is a tcp server that receives commands and interacts
// with a storage layer
type Server struct {
	port            string
	log             *log.Logger
	store           storage.Storer
	ln              net.Listener
	handlers        sync.WaitGroup
	readtimeout     time.Duration
	shutdowntimeout time.Duration
	close           chan struct{}
	exited          chan struct{}
}

// NewServer returns a configured Server instance ready to start serving
func NewServer(opts ...OptionFunc) (*Server, error) {
	store, err := hash.NewIndex()
	if err != nil {
		return nil, err
	}

	cfg := &options{
		port:            ":8080",
		log:             log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile),
		store:           store,
		readtimeout:     time.Second * 15,
		shutdowntimeout: time.Second * 20,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &Server{
		port:            cfg.port,
		log:             cfg.log,
		store:           cfg.store,
		handlers:        sync.WaitGroup{},
		readtimeout:     cfg.readtimeout,
		shutdowntimeout: cfg.shutdowntimeout,
		close:           make(chan struct{}),
		exited:          make(chan struct{}),
	}, nil
}

// Serve starts the server
func (s *Server) Serve() {
	defer func() {
		if ok := s.wait(); ok {
			s.log.Println("connection handlers exited normally")
		} else {
			s.log.Println("timed out waiting for connection handlers to exit")
		}
		close(s.exited)
	}()

	s.log.Println("accepting connections")

	ln, err := net.Listen("tcp", s.port)
	if err != nil {
		s.log.Fatalf("fatal: %v\n", err)
	}

	s.ln = ln

	for {
		// ln.Accept returns an error when we close the listener, this is strange behavior
		c, err := ln.Accept()
		if err != nil {
			// only log the error if it isn't due to a closed network connection
			// not sure how else to detect this since something like net.errClosed isn't exported from net package
			if !strings.Contains(err.Error(), "closed network connection") {
				s.log.Printf("error accepting connection: %v\n", err)
			}

			select {
			case <-s.close:
				s.log.Println("closing")

				err = s.store.Close()
				if err != nil {
					s.log.Printf("error closing store: %v\n", err)
				}

				return
			default:
			}
		} else {
			s.handlers.Add(1)
			go s.handle(c)
		}
	}
}

// Close triggers the server to stop accepting connections and close its store
func (s *Server) Close() error {
	err := s.ln.Close()
	if err != nil {
		s.log.Printf("error closing listener: %v\n", err)
	}

	// close the close channel to tell server to shutdown
	close(s.close)
	// wait for server to exit (wait for handlers to finish)
	<-s.exited

	return nil
}

func (s *Server) handle(c net.Conn) {
	defer func() {
		s.log.Printf("closing connection from: %v\n", c.RemoteAddr())

		err := c.Close()
		if err != nil {
			s.log.Printf("error closing connection: %v\n", err)
		}

		s.handlers.Done()
	}()
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)

	c.SetReadDeadline(time.Now().Add(s.readtimeout))

	// Read the incoming connection into the buffer.
	nbytes, err := c.Read(buf)
	if err != nil {
		s.log.Printf("error reading: %v\n", err)
	}

	s.log.Printf("read %v bytes: %s", nbytes, string(buf))

	// Send a response back to person contacting us.
	c.Write([]byte("received"))
}

// wait waits for wg waiting finishes normally it returns true, otherwise it returns false.
func (s *Server) wait() bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		s.handlers.Wait()
	}()
	select {
	case <-c:
		return true // completed normally
	case <-time.After(s.shutdowntimeout):
		return false // timed out
	}
}
