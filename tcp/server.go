package tcp

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ryansann/hydro/index"
)

// ServerOption overrides a default option
type ServerOption func(*options)

type options struct {
	port            string
	log             *log.Logger
	readTimeout     time.Duration
	shutdownTimeout time.Duration
}

// Port overrides the default port ":8080"
func Port(p string) ServerOption {
	return func(opts *options) {
		opts.port = p
	}
}

// Logger overrides default logger to the one passed in
func Logger(l *log.Logger) ServerOption {
	return func(opts *options) {
		opts.log = l
	}
}

// ReadTimeout overrides the default read timeout to the duration passed in
// If overriding ShutdownTimeout as well, ReadTimeout should be greater than ShutdownTimeout
func ReadTimeout(t time.Duration) ServerOption {
	return func(opts *options) {
		opts.readTimeout = t
	}
}

// ShutdownTimeout overrides the default value for how long the server will wait for connections
// to finish being handled before forcefully shutting down (Server.Serve exits).
// If ReadTimeout > ShutdownTimeout there may be cases where connections don't get enough time to close
// before Server.Serve exits.
func ShutdownTimeout(t time.Duration) ServerOption {
	return func(opts *options) {
		opts.shutdownTimeout = t
	}
}

// Server is a tcp server that receives commands and interacts
// with a storage layer
type Server struct {
	mtx             sync.Mutex
	port            string
	log             *log.Logger
	index           index.Indexer
	ln              net.Listener
	handlers        sync.WaitGroup
	readTimeout     time.Duration
	shutdownTimeout time.Duration
	close           chan struct{}
	exited          chan struct{}
}

const (
	defaultReadTimeout     = 60 * time.Second
	defaultShutdownTimeout = 5 * time.Second
)

// NewServer returns a configured Server instance ready to start serving
func NewServer(index index.Indexer, opts ...ServerOption) (*Server, error) {
	cfg := &options{
		port:            ":8080",
		log:             log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile),
		readTimeout:     defaultReadTimeout,
		shutdownTimeout: defaultShutdownTimeout,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &Server{
		mtx:             sync.Mutex{},
		port:            cfg.port,
		log:             cfg.log,
		index:           index,
		handlers:        sync.WaitGroup{},
		readTimeout:     cfg.readTimeout,
		shutdownTimeout: cfg.shutdownTimeout,
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

	s.mtx.Lock()
	s.ln = ln
	s.mtx.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

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

				// close open connections
				cancel()

				return
			default:
			}
		} else {
			s.log.Printf("accepted new connection from: %s\n", c.RemoteAddr().String())
			s.handlers.Add(1)
			go s.handle(ctx, newConn(c))
		}
	}
}

// Close triggers the server to stop accepting connections and close its store
func (s *Server) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

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

type conn struct {
	net.Conn
	close chan struct{}
}

func newConn(c net.Conn) *conn {
	return &conn{
		c,
		make(chan struct{}),
	}
}

func (s *Server) handle(ctx context.Context, c *conn) {
	defer func() {
		s.log.Printf("closing connection from: %v\n", c.RemoteAddr())

		err := c.Close()
		if err != nil {
			s.log.Printf("error closing connection: %v\n", err)
		}

		s.handlers.Done()
	}()

	reader := bufio.NewReaderSize(c, 4<<8)
	for {
		select {
		case <-ctx.Done():
			c.Write([]byte("CLOSING\n"))
			return
		case <-c.close:
			c.Write([]byte("QUITTING\n"))
			return
		default:
			c.SetReadDeadline(time.Now().Add(s.readTimeout))

			l, _, err := reader.ReadLine()
			if err != nil {
				if strings.Contains(err.Error(), "i/o timeout") {
					s.log.Println("read timeout")
				}
				return
			}

			cmd, err := parseCommand(string(l))
			if err != nil {
				s.log.Println("failed to parse command")
				c.Write([]byte(fmt.Sprintf("PARSE FAILED: %v\n", err)))
				continue
			}

			res, err := cmd.execute(c.close, s.index)
			if err != nil {
				s.log.Printf("failed to execute command: %s %s %s\n", commands[cmd.op], cmd.key, cmd.val)
				c.Write([]byte(fmt.Sprintf("EXECUTE FAILED: %v\n", err)))
				continue
			}

			if res != "" {
				c.Write([]byte(fmt.Sprintf("%s\n", res)))
			} else {
				c.Write([]byte("SUCCESS\n"))
			}
		}
	}
}

// wait waits for wg. If waiting finishes before timeout, it returns true, otherwise it returns false.
func (s *Server) wait() bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		s.handlers.Wait()
	}()
	select {
	case <-c:
		return true // completed normally
	case <-time.After(s.shutdownTimeout):
		return false // timed out
	}
}
