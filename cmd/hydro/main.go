package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ryansann/hydro/index/hash"
	"github.com/ryansann/hydro/storage/file"
	"github.com/ryansann/hydro/tcp"
)

// env vars for overriding defaults
const (
	// index env vars
	restoreEnv      = "HYDRO_INDEX_RESTORE"
	syncIntervalEnv = "HYDRO_INDEX_SYNC_INTERVAL"
	// server env var names
	portEnv            = "HYDRO_SERVER_PORT"
	readTimeoutEnv     = "HYDRO_SERVER_READ_TIMEOUT"
	shutdownTimeoutEnv = "HYDRO_SERVER_SHUTDOWN_TIMEOUT"
)

func main() {
	l := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	storage, err := file.NewStore("./data")
	if err != nil {
		l.Fatalf("could not create storage: %v", err)
	}

	iopts, err := indexOpts()
	if err != nil {
		l.Fatal(err)
	}

	idx, err := hash.NewIndex(storage, iopts...)
	if err != nil {
		l.Fatalf("could not create index: %v", err)
	}

	sopts, err := serverOpts(l)
	if err != nil {
		l.Fatal(err)
	}

	// server uses default hash.Index as a store
	s, err := tcp.NewServer(idx, sopts...)
	if err != nil {
		l.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// this blocks until we receive a signal
		sig := <-sigs

		l.Printf("received %v signal, shutting down...\n", sig)

		err := s.Close()
		if err != nil {
			l.Println(err)
		}
	}()

	// Serve blocks until the server is Closed
	s.Serve()
}

// serverOpts constructs the list of options to provide to the hydro tcp server.
func serverOpts(l *log.Logger) ([]tcp.ServerOption, error) {
	var opts []tcp.ServerOption

	opts = append(opts, tcp.Logger(l))

	if port := os.Getenv(portEnv); port != "" {
		opts = append(opts, tcp.Port(port))
	}

	if rt := os.Getenv(readTimeoutEnv); rt != "" {
		dur, err := time.ParseDuration(rt)
		if err != nil {
			return nil, err
		}

		opts = append(opts, tcp.ReadTimeout(dur))
	}

	if st := os.Getenv(shutdownTimeoutEnv); st != "" {
		dur, err := time.ParseDuration(st)
		if err != nil {
			return nil, err
		}

		opts = append(opts, tcp.ShutdownTimeout(dur))
	}

	return opts, nil
}

// indexOpts constructs the list of options to provide to the hydro hash index.
func indexOpts() ([]hash.IndexOption, error) {
	var opts []hash.IndexOption

	if nr := os.Getenv(restoreEnv); nr != "" {
		v, err := strconv.ParseBool(nr)
		if err != nil {
			return nil, err
		}

		opts = append(opts, hash.Restore(v))
	}

	if si := os.Getenv(syncIntervalEnv); si != "" {
		dur, err := time.ParseDuration(si)
		if err != nil {
			return nil, err
		}

		opts = append(opts, hash.SyncInterval(dur))
	}

	return opts, nil
}
