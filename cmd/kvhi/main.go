package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryansann/hydro/tcp"
)

func main() {
	// server uses default hash.Index as a store
	s, err := tcp.NewServer(tcp.Port(":8888"))
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
