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
	"../../pkg/rpchelper"
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
func (s Service) Join(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	var i chord.NodeInfo
	name := args.Name
	port := args.Port

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
func (s Service) Create(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	name := args.Name
	port := args.Port
	base := args.Base
	exponent := args.Exponent

	node, err := chord.Create(name, port, base, exponent)
	if err != nil {
		return err
	}
	nodes[name] = node
	return nil
}

//Leave handles leaving a ring
func (s Service) Leave(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	name := args.Name
	node, ok := nodes[name]
	if ok {
		node.Running = false
		return nil
	}
	return errors.New("you are not in this ring")
}



//Lookup handles lookup a key in a ring
func (s Service) Lookup(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	name := args.Name
	key := args.Key

	n, ok := nodes[name]
	if ok {
		i, err := n.Lookup(chord.GenID(key, n.Ring.Modulo))
		if err != nil {
			(*reply).Message = "Lookup: not found"
			return err
		}
		(*reply).Message = "Lookup: found"
		(*reply).Node = i
	}
	return errors.New("you are not in this ring")
}

//SimpleLookup handles lookup a key in a ring using a simple alghoritm
func (s Service) SimpleLookup(args rpchelper.ServiceArgs, reply *rpchelper.ServiceReply) error {
	name := args.Name
	key := args.Key

	n, ok := nodes[name]
	if ok {
		i, err := n.SimpleLookup(chord.GenID(key, n.Ring.Modulo))
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
