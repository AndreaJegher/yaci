package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"../../pkg/chord"
)

var (
	chordServicePort = flag.Int("port", 6367, "Port of the chord service.")
	nodes            = make(map[string]chord.Node)
)

type (
	//Service holds available chord related services
	Service struct {
	}
)

//Join handles joinig a ring
func (s Service) Join(name string, port int) error {
	var i chord.NodeInfo
	i.Port = port
	ips, err := net.LookupIP(name)
	if err != nil {
		return err
	}

	for pos, ip := range ips {
		i.Address = ip
		node, err := chord.Join(i)
		if err != nil {
			log.Println(err)
			if pos == len(ips)-1 {
				return err
			}
		} else {
			nodes[name] = node
			break
		}
	}

	return nil
}

//Create handles createing a ring
func (s Service) Create(name string, port, base int, exponent uint) error {
	node, err := chord.Create(name, port, base, exponent)
	if err != nil {
		return err
	}
	nodes[name] = node
	return nil
}

//Leave handles leaving a ring
func (s Service) Leave(name string) error {
	node, ok := nodes[name]
	if ok {
		node.Running = false
		return nil
	}
	return errors.New("you are not in this ring")
}

//Lookup handles lookup a key in a ring
func (s Service) Lookup(name string, key uint64) (chord.NodeInfo, error) {
	var i chord.NodeInfo
	n, ok := nodes[name]
	if ok {
		return n.Lookup(key)
	}
	return i, errors.New("you are not in this ring")
}

//SimpleLookup handles lookup a key in a ring using a simple alghoritm
func (s Service) SimpleLookup(name string, key uint64) (chord.NodeInfo, error) {
	var i chord.NodeInfo
	n, ok := nodes[name]
	if ok {
		return n.SimpleLookup(key)
	}
	return i, errors.New("you are not in this ring")
}

func main() {
	var s Service
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", *chordServicePort))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
