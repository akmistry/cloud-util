package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/akmistry/go-util/badgerkv"
	"google.golang.org/grpc"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/rpc"
	"github.com/akmistry/cloud-util/rpc/pb"
)

var (
	rootDir = flag.String("root-dir", "", "Root directory for key-value store files")
	addr    = flag.String("addr", "localhost:18100", "Address:port to listen on")
)

func main() {
	flag.Parse()

	if *rootDir == "" {
		panic("-root-dir MUST be specified")
	}

	err := os.MkdirAll(*rootDir, 0755)
	if err != nil {
		panic(err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()

	server := rpc.NewServer(func(name string) (cloud.UnorderedStore, error) {
		name = filepath.Join(*rootDir, "badger-"+name)
		return badgerkv.NewStore(name)
	})
	defer server.Shutdown()
	pb.RegisterStoreServer(grpcServer, server)

	signalCh := make(chan os.Signal, 1)
	go func() {
		<-signalCh
		log.Println("Shutting down...")
		grpcServer.GracefulStop()
	}()
	signal.Notify(signalCh, os.Interrupt, os.Kill, syscall.SIGTERM)

	log.Println("Serving KV server on:", *addr)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Printf("Stopping server with error: %v", err)
	} else {
		log.Println("Stopping server")
	}
}
