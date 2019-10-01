package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryansann/hydro/index/hash"
	"github.com/ryansann/hydro/storage/file"
	"github.com/ryansann/hydro/tcp"
)

func main() {
	storage, err := file.NewStore("./data")
	if err != nil {
		log.Fatalf("could not create storage: %v", err)
	}

	idx, err := hash.NewIndex(storage)
	if err != nil {
		log.Fatalf("could not create index: %v", err)
	}

	// server uses default hash.Index as a store
	s, err := tcp.NewServer(idx, tcp.Port(":8888"))
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// this blocks until we receive a signal
		sig := <-sigs

		log.Printf("received %v signal, shutting down...\n", sig)

		err := s.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	// Serve blocks until the server is Closed
	s.Serve()
}

// serverOpts constructs the list of options to provide to the hydro tcp server.
func serverOpts() []tcp.ServerOption {
	var opts []tcp.ServerOption
	return opts
}
