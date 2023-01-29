package tcp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ryansann/hydro/index"
	"github.com/sirupsen/logrus"
)

// ServerOption overrides a default option
type ServerOption func(*options)

type options struct {
	port            string
	readTimeout     time.Duration
	shutdownTimeout time.Duration
}

// Port overrides the default port ":8080"
func Port(p string) ServerOption {
	return func(opts *options) {
		opts.port = p
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
	log             *logrus.Logger
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
func NewServer(log *logrus.Logger, index index.Indexer, opts ...ServerOption) (*Server, error) {
	cfg := &options{
		port:            ":8080",
		readTimeout:     defaultReadTimeout,
		shutdownTimeout: defaultShutdownTimeout,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &Server{
		mtx:             sync.Mutex{},
		port:            cfg.port,
		log:             log,
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
			s.log.Info("connection handlers exited normally")
		} else {
			s.log.Error("timed out waiting for connection handlers to exit")
		}
		close(s.exited)
	}()

	s.log.Infof("accepting connections on port %s", s.port)

	ln, err := net.Listen("tcp", s.port)
	if err != nil {
		s.log.Fatal(err)
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
				s.log.Infof("error accepting connection: %v", err)
			}

			select {
			case <-s.close:
				s.log.Info("closing")

				// close open connections
				cancel()

				return
			default:
			}
		} else {
			s.log.Infof("accepted new connection from: %s", c.RemoteAddr().String())
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
		s.log.Errorf("error closing listener: %v", err)
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
		s.log.Infof("closing connection from: %v", c.RemoteAddr())

		err := c.Close()
		if err != nil {
			s.log.Errorf("error closing connection: %v", err)
		}

		s.handlers.Done()
	}()

	reader := bufio.NewReaderSize(c, 4<<8)
	for {
		select {
		case <-ctx.Done():
			s.log.Infof("closing connection for %s", c.RemoteAddr().String())
			c.Write([]byte("CLOSING\n"))
			return
		case <-c.close:
			s.log.Infof("client %s closed connection", c.RemoteAddr().String())
			c.Write([]byte("QUITTING\n"))
			return
		default:
			c.SetReadDeadline(time.Now().Add(s.readTimeout))

			l, _, err := reader.ReadLine()
			if err != nil {
				if strings.Contains(err.Error(), "i/o timeout") {
					s.log.Error("read timeout")
				}
				return
			}

			cmd, err := parseCommand(string(l))
			if err != nil {
				s.log.Error("failed to parse command")
				c.Write([]byte(fmt.Sprintf("PARSE FAILED: %v\n", err)))
				continue
			}

			res, err := cmd.execute(c.close, s.index)
			if err != nil {
				s.log.Errorf("failed to execute command: %s %s %s", commands[cmd.op], cmd.key, cmd.val)
				c.Write([]byte(fmt.Sprintf("EXECUTION FAILED: %v\n", err)))
				continue
			}

			s.log.Debugf("client %s executed command: %s %s %s", c.RemoteAddr().String(), commands[cmd.op], cmd.key, cmd.val)

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
