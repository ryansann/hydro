package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/ryansann/hydro/index/hash"
	"github.com/ryansann/hydro/storage"
	"github.com/ryansann/hydro/storage/file"
	"github.com/ryansann/hydro/storage/fileseg"
	"github.com/ryansann/hydro/tcp"
)

// env vars for overriding defaults
const (
	// storage env vars
	storageModeVar     = "HYDRO_STORAGE_MODE"
	fsFilepathVar      = "HYDRO_FILE_STORAGE_FILEPATH"
	fsSyncIntervalVar  = "HYDRO_FILE_STORAGE_SYNC_INTERVAL"
	fsegDirVar         = "HYDRO_FILESEG_STORAGE_DIR"
	fsegSegmentSizeVar = "HYDRO_FILESEG_STORAGE_SEGMENT_SIZE"

	// index env vars
	indexModeVar = "HYDRO_INDEX_MODE"
	hiRestoreVar = "HYDRO_HASH_INDEX_RESTORE"

	// server env var names
	portVar            = "HYDRO_SERVER_PORT"
	readTimeoutVar     = "HYDRO_SERVER_READ_TIMEOUT"
	shutdownTimeoutVar = "HYDRO_SERVER_SHUTDOWN_TIMEOUT"
)

func main() {
	l := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	s, err := createStorer(l)
	if err != nil {
		l.Fatal(err)
	}
	defer s.Close()

	iopts, err := indexOpts()
	if err != nil {
		l.Fatal(err)
	}

	idx, err := hash.NewIndex(s, iopts...)
	if err != nil {
		l.Fatalf("could not create index: %v", err)
	}

	sopts, err := serverOpts(l)
	if err != nil {
		l.Fatal(err)
	}

	// server uses default hash.Index as a store
	srv, err := tcp.NewServer(idx, sopts...)
	if err != nil {
		l.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// this blocks until we receive a signal
		sig := <-sigs

		l.Printf("received %v signal, shutting down...\n", sig)

		err := srv.Close()
		if err != nil {
			l.Println(err)
		}
	}()

	// Serve blocks until the server is Closed
	srv.Serve()
}

// storage mode constants
const (
	fileStorageMode    = "file"
	filesegStorageMode = "fileseg"
)

const (
	defaultFile = "./data"
	defaultDir  = "./fseg"
)

// createStorer uses the environment to instantiate the storage layer, which is returned if there was no error.
func createStorer(l *log.Logger) (storage.Storer, error) {
	mode := os.Getenv(storageModeVar)
	if mode == "" {
		l.Printf("defaulting storage mode to %+q\n", fileStorageMode)
		mode = fileStorageMode
	}

	var storer storage.Storer

	switch mode {
	case fileStorageMode:
		fpath := os.Getenv(fsFilepathVar)
		if fpath == "" {
			l.Printf("defaulting %s storage filepath to %+q\n", fileStorageMode, defaultFile)
			fpath = defaultFile
		}

		var opts []file.StoreOption
		if si := os.Getenv(fsSyncIntervalVar); si != "" {
			dur, err := time.ParseDuration(si)
			if err != nil {
				return nil, err
			}

			opts = append(opts, file.SyncInterval(dur))
		}

		s, err := file.NewStore(fpath)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create %s storage", fileStorageMode)
		}

		storer = s
	case filesegStorageMode:
		dir := os.Getenv(fsegDirVar)
		if dir == "" {
			l.Printf("defaulting %s storage dir to %+q\n", filesegStorageMode, defaultDir)
			dir = defaultDir
		}

		var opts []fileseg.StoreOption

		segSize := os.Getenv(fsegSegmentSizeVar)
		if segSize != "" {
			sz, err := strconv.ParseInt(segSize, 10, 64)
			if err != nil {
				return nil, err
			}

			opts = append(opts, fileseg.SegmentSize(int(sz)))
		}

		s, err := fileseg.NewStore(dir, opts...)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create %s storage", filesegStorageMode)
		}

		storer = s
	default:
		return nil, fmt.Errorf("unrecognized storage mode %+q", mode)
	}

	return storer, nil
}

// serverOpts constructs the list of options to provide to the hydro tcp server.
func serverOpts(l *log.Logger) ([]tcp.ServerOption, error) {
	var opts []tcp.ServerOption

	opts = append(opts, tcp.Logger(l))

	if port := os.Getenv(portVar); port != "" {
		opts = append(opts, tcp.Port(port))
	}

	if rt := os.Getenv(readTimeoutVar); rt != "" {
		dur, err := time.ParseDuration(rt)
		if err != nil {
			return nil, err
		}

		opts = append(opts, tcp.ReadTimeout(dur))
	}

	if st := os.Getenv(shutdownTimeoutVar); st != "" {
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

	if nr := os.Getenv(hiRestoreVar); nr != "" {
		v, err := strconv.ParseBool(nr)
		if err != nil {
			return nil, err
		}

		opts = append(opts, hash.Restore(v))
	}

	return opts, nil
}
