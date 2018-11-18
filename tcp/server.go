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
	mtx             sync.Mutex
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
		mtx:             sync.Mutex{},
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

				err = s.store.Close()
				if err != nil {
					s.log.Printf("error closing store: %v\n", err)
				}

				// close open connections
				cancel()

				return
			default:
			}
		} else {
			s.handlers.Add(1)
			go s.handle(ctx, newconn(c))
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

func newconn(c net.Conn) *conn {
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
			c.SetReadDeadline(time.Now().Add(s.readtimeout))

			l, _, err := reader.ReadLine()
			if err != nil {
				if strings.Contains(err.Error(), "i/o timeout") {
					s.log.Println("read timeout")
				}
				return
			}

			cmd, err := parsecommand(string(l))
			if err != nil {
				s.log.Println("failed to parse command")
				c.Write([]byte(fmt.Sprintf("PARSE FAILED: %v\n", err)))
				continue
			}

			res, err := cmd.execute(c.close, s.store)
			if err != nil {
				s.log.Printf("failed to execute command: %s %s %s\n", commandtypes[cmd.op], cmd.key, cmd.val)
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

type commandtype int

const (
	get = iota
	set
	del
	quit
)

var commandtypes = map[commandtype]string{
	get:  "get",
	set:  "set",
	del:  "del",
	quit: "quit",
}

type command struct {
	op  commandtype
	key string
	val string
}

func parsecommand(l string) (*command, error) {
	cmps := strings.Split(l, " ")
	if len(cmps) < 1 {
		return nil, fmt.Errorf("command must have an operation")
	}

	cmd := &command{}

	switch strings.ToLower(cmps[0]) {
	case "get":
		cmd.op = commandtype(get)
		if len(cmps) != 2 {
			return nil, fmt.Errorf("get command requires an argument")
		}
		cmd.key = cmps[1]
	case "set":
		cmd.op = commandtype(set)
		if len(cmps) != 3 {
			return nil, fmt.Errorf("set command requires 2 arguments")
		}
		cmd.key = cmps[1]
		cmd.val = cmps[2]
	case "del":
		cmd.op = commandtype(del)
		if len(cmps) != 2 {
			return nil, fmt.Errorf("del command requires an argument")
		}
		cmd.key = cmps[1]
	case "quit":
		cmd.op = commandtype(quit)
		if len(cmps) != 1 {
			return nil, fmt.Errorf("quit command should not have any arguments")
		}
	default:
		return nil, fmt.Errorf("unrecognized operation")
	}

	return cmd, nil
}

func (cmd *command) execute(c chan struct{}, s storage.Storer) (string, error) {
	switch cmd.op {
	case get:
		res, err := s.Read([]byte(cmd.key))
		if err != nil {
			return "", err
		}
		return string(res), nil
	case set:
		err := s.Write([]byte(cmd.key), []byte(cmd.val))
		if err != nil {
			return "", err
		}
		return "", nil
	case del:
		err := s.Delete([]byte(cmd.key))
		if err != nil {
			return "", err
		}
		return "", nil
	case quit:
		close(c)
		return "", fmt.Errorf("closing")
	default:
		return "", fmt.Errorf("did not execute")
	}
}
