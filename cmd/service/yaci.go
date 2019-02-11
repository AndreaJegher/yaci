package main

import (
  "../../yaci/chord"
  "flag"
  "errors"
  "fmt"
  "log"
  "net"
  "net/http"
  "net/rpc"
)

var (
  chordServicePort = flag.Int("port", 6367, "Port of the chord service.")
  rings = make(map[string]chord.Ring)
)

type (
  //Service holds available chord related services
  Service struct {
  }
)

//Join handles joinig a ring
func (s Service) Join(name string, port int) error {
  ring, err := chord.JoinRing(name, port)
  if err != nil {
    return err
  }
  rings[name] = ring
  return nil
}

//Create handles createing a ring
func (s Service) Create(name string, port int) error {
  ring, err := chord.CreateRing(name, port)
  if err != nil {
    return err
  }
  rings[name] = ring
  return nil
}

//Leave handles leaving a ring
func (s Service) Leave(name string) error {
  ring, ok := rings[name]
  if ok {
    return ring.Leave()
  }
  return errors.New("you are not in this ring")
}

//Lookup handles lookup a key in a ring
func (s Service) Lookup(name, key string) (net.IP, error) {
  ring, ok := rings[name]
  if ok {
    return ring.LocalNode.Lookup(key)
  }
  return nil, errors.New("you are not in this ring")
}

//SimpleLookup handles lookup a key in a ring using a simple alghoritm
func (s Service) SimpleLookup(name, key string) (net.IP, error) {
  ring, ok := rings[name]
  if ok {
    return ring.LocalNode.SimpleLookup(key)
  }
  return nil, errors.New("you are not in this ring")
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
