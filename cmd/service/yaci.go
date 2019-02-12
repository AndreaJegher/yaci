package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"

	"../../pkg/chord"
	"../../pkg/rpchelper"
)

var (
	chordServicePort = flag.Int("port", 6367, "Port of the chord service.")
	nodes            = make(map[string]*chord.Node)
)

type (
	// Service holds available chord related services
	Service struct {
	}
)

// CreateRing handles createing a ring
func (s Service) CreateRing(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	var i chord.NodeInfo
	var r chord.RingInfo

	i.Port = args.Port
	r.Name = args.Name
	r.ModuloBase = args.Base
	r.ModuloExponent = args.Exponent
	r.Modulo = uint64(math.Pow(float64(r.ModuloBase), float64(r.ModuloExponent)) - 1)
	node, err := chord.Create(i, r)
	if err != nil {
		return err
	}
	nodes[args.Name] = node
	return nil
}

// JoinRing handles joinig a ring
func (s Service) JoinRing(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	var i chord.NodeInfo

	i.Port = args.Port
	ips, err := net.LookupIP(args.Name)
	if err != nil {
		return err
	}

	for pos, ip := range ips {
		i.Address = ip
		node, err := chord.Join(i, args.LocalPort)
		if err != nil {
			log.Println(err)
			if pos == len(ips)-1 {
				return err
			}
		} else {
			nodes[args.Name] = node
			break
		}
	}

	return nil
}

// Leave handles leaving a ring
func (s Service) Leave(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	name := args.Name
	node, ok := nodes[name]
	if ok {
		node.Running = false
		return nil
	}
	return errors.New("you are not in this ring")
}

// List handles leaving a ring
func (s Service) List(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	for _, k := range nodes {
		reply.List = append(reply.List, *k)
	}
	return nil
}

// Lookup handles lookup a key in a ring
func (s Service) Lookup(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	var i chord.NodeInfo
	name := args.Name
	key := args.Key

	n, ok := nodes[name]
	if ok {
		err := n.Lookup(chord.GenID(key, n.Ring.Modulo), &i)
		if err != nil {
			(*reply).Message = "Lookup: not found"
			return err
		}
		(*reply).Message = "Lookup: found"
		(*reply).Node = i
	}
	return errors.New("you are not in this ring")
}

// SimpleLookup handles lookup a key in a ring using a simple alghoritm
func (s Service) SimpleLookup(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	var i chord.NodeInfo
	name := args.Name
	key := args.Key

	n, ok := nodes[name]
	if ok {
		err := n.SimpleLookup(chord.GenID(key, n.Ring.Modulo), &i)
		if err != nil {
			(*reply).Message = "Simple lookup: not found"
			return err
		}
		(*reply).Message = "Simple lookup: found"
		(*reply).Node = i
	}
	return errors.New("you are not in this ring")
}

func main() {
	flag.Parse()

	s := new(Service)
	rpc.Register(s)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *chordServicePort))
	if err != nil {
		log.Fatal("listen error:", err)
	}
	// TODO use ServeTLS
	log.Fatalf("Error serving: %s", http.Serve(l, nil))
}
