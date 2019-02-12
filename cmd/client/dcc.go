package main

import (
  "fmt"
  "flag"
  "log"
  "net/rpc"
  "../../pkg/rpchelper"
)

var (
  join = flag.Bool("join", false, "Connect to a ring. example: -join -name <name>")
  leave = flag.Bool("leave", false, "Leave a ring. example: -leave -name <name>")
  new = flag.Bool("new", false, "Create a ring. example: -new -name <name>")
  base = flag.Int("base", 2, "Base for ring modulo. Modulo = Base^Exponent - 1")
  exponent = flag.Int("exponent", 64, "Exponent for ring modulo.")
  port = flag.Int("port", 6368, "Port for ring's p2p communications. Default 6368. If 0 will be random.")
  lookup = flag.Bool("lookup", false, "Lookup key in a ring. example: -lookup -name <name> -key <key>")
  list = flag.Bool("list", false, "List local nodes and rings.")
  simple = flag.Bool("simple", false, "Use a simpler and less efficient lookup alghoritm. Included only for completeness.")
  name = flag.String("name", "homering.ga", "Hostname of a ring.")
  remoteport = flag.Int("remoteport", 6368, "Port of the host when joining.")
  key = flag.String("key", "00000", "Key of an item.")
  chordService = flag.String("csname", "localhost", "Address of the chord service.")
  chordServicePort = flag.Int("csport", 6367, "Port of the chord service.")
)

// Dummy Chord Client
func main() {
  flag.Parse()

  client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *chordService, *chordServicePort))
  if err != nil {
  	log.Fatal("dialing:", err)
  }

  var method string
  var args rpchelper.ServiceArgs
  var reply rpchelper.ServiceReply

  if *join {
    method = "Service.JoinRing"
    args.Name = *name
    args.Port = *remoteport
    args.LocalPort = *port
  } else if *new {
    method = "Service.CreateRing"
    args.Name = *name
    args.Port = *port
    args.Base = *base
    args.Exponent = *exponent
  } else if *leave {
    method = "Service.Leave"
    args.Name = *name
  } else if *lookup {
    if *simple {
      method = "Service.SimpleLookup"
    } else {
      method = "Service.Lookup"
    }
    args.Name = *name
    args.Key = *key
  } else if *list {
    method = "Service.List"
  }

  err = client.Call(method, args, &reply)
  if err != nil {
    log.Fatal("error:", err)
  }
  fmt.Println(reply)
}
