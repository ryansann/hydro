package web

import (
	"log"
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
	}, nil
}

// Serve starts the server
func (s *Server) Serve() {

}
