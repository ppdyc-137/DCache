package main

import (
	dcache "DCache"
	"context"
	"flag"
	"fmt"
	"log"
	// "os"
	// "os/signal"
)

var serviceName = "DCache"

func main() {
	port := flag.Int("p", 8000, "Port")
	nodeID := flag.String("n", "A", "Node ID")
	flag.Parse()

	addr := fmt.Sprintf("127.0.0.1:%d", *port)

	log.Printf("Node %s starting at: %s", *nodeID, addr)
	node, err := dcache.NewServer(addr, serviceName)
	if err != nil {
		log.Fatal("failed to create node: ", err)
	}

	picker, err := dcache.NewPeerPicker(addr, serviceName)
	if err != nil {
		log.Fatal("failed to create client picker: ", err)
	}

	source := dcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		log.Printf("Node %s get from source: key: %s", *nodeID, key)
		return fmt.Appendf(nil, "source value of %s from %s", key, *nodeID), nil
	})

	group := dcache.NewGroup("test", source, picker)

	go func() {
		if err := node.Start(); err != nil {
			log.Fatalf("Node start failed: %v", err)
		}
	}()

	for {
		var cmd, key, val string
		fmt.Scan(&cmd)

		switch cmd {
		case "set":
			fmt.Scan(&key)
			fmt.Scan(&val)
			if err := group.Set(context.Background(), key, []byte(val)); err != nil {
				log.Printf("set %s %s error: %v", key, val, err)
			}
		case "get":
			fmt.Scan(&key)
			val, err := group.Get(context.Background(), key)
			if err != nil {
				log.Printf("get %s error: %v", key, err)
			}
			fmt.Printf("%s %s", key, val)
		case "del":
			fmt.Scan(&key)
			err := group.Delete(context.Background(), key)
			if err != nil {
				log.Printf("del %s error: %v", key, err)
			}
		case "list":
			picker.PrintPeers()
		case "stats":
			fmt.Println(group.Stats())
		case "hash":
			fmt.Scan(&key)
			peer, _, _ := picker.PickPeer(key)
			fmt.Println(peer.Addr())
		}
	}

	// quit := make(chan os.Signal, 1)
	// signal.Notify(quit, os.Interrupt)
	// <-quit
	//
	// log.Println("Service shutdown...")
	// node.Stop()
	// picker.Close()
	// dcache.DestroyAllGroups()
}
