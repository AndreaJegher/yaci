package chord

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type (
	//Node rapresents a node on a chord node of peers
	Node struct {
		ID             string
		ModuloExponent uint
		ModuloBase     int
		Modulo         uint64
		Location       NodeLocation
		NextNode  NodeLocation
		PrevNode  NodeLocation
		FingerTable    []NodeLocation
	}

	//NodeLocation holds the location of a node
	NodeLocation struct {
		RingName string
		Address  net.IP
		Port     int
	}
)

func externalIP() (net.IP, error) {
	var ip net.IP
	ifaces, err := net.Interfaces()

	if err != nil {
		return ip, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return ip, err
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip, nil
		}
	}
	return ip, errors.New("there are no available network")
}

//GenID generate a valid entity identifier
func GenID(item string) string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(item)))
}

//JoinRing connects to a chord node and returns an object rapresenting the node
func JoinRing(name string, destport, localport int) (Node, error) {
	var node Node

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", name, destport))
	if err != nil {
		return node, err
	}

	method := "Node.Join"
	arg := localport

	err = client.Call(method, arg, &node)
	if err != nil {
		return node, err
	}

	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", localport))
	if err != nil {
		return node, err
	}
	node.Location.Port = localport
	ip, err := externalIP()
	if err != nil {
		return node, nil
	}
	node.Location.Address = ip
	node.ID = GenID(ip.String())

	go http.Serve(l, nil)
	return node, nil
}

//CreateRing creates a new chord node and returns an object rapresenting the node
func CreateRing(name string, port int) (Node, error) {
	var node Node

	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return node, err
	}

	node.Location.RingName = name
	node.ModuloExponent = 64
	node.ModuloBase = 1
	node.Modulo = uint64(node.ModuloBase << node.ModuloExponent - 1)
	node.Location.Port = port
	ip, err := externalIP()
	if err != nil {
		return node, nil
	}
	node.Location.Address = ip
	node.ID = GenID(ip.String())

	go http.Serve(l, nil)
	return node, nil
}

//LeaveRing select the new successor node and leaves the network
func LeaveRing(node Node) error {
	next := node.NextNode
	prev, err := node.Lookup(GenID(node.Location.Address.String()))
	if err != nil {
		return err
	}

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", prev.Location.Address, prev.Location.Port))
	if err != nil {
		return err
	}

	var reply error
	method := "Node.Leave"
	arg := next

	err = client.Call(method, arg, &reply)
	if err != nil {
		return err
	}

	return nil
}

//Join handles a node joining a node
func (node Node) Join(joiningNodeIP net.IP, joiningNodePort int) (Node, error) {
	var newNode Node

	prev, err := node.Lookup(GenID(joiningNodeIP.String()))
	if err != nil {
		return newNode, err
	}

	newNode.Location.RingName = node.Location.RingName
	newNode.ModuloExponent = node.ModuloExponent
	newNode.ModuloBase = node.ModuloBase
	newNode.Modulo = node.Modulo
	next := prev.NextNode

	newNode.NextNode = next
	prev.NextNode.Address = joiningNodeIP
	prev.NextNode.Port = joiningNodePort

	return newNode, nil
}

//Leave handles disconnection of the local node from the node
func (node Node) Leave(next NodeLocation) error {
	node.NextNode = next
	return nil
}

//Lookup finds the node holding the key (scalable implementation)
func (node Node) Lookup(keyID string) (Node, error) {
	return node, nil
}

//SimpleLookup finds the node holding the key (simple implementation)
func (node Node) SimpleLookup(keyID string) (NodeLocation, error) {
	var nodeLocation NodeLocation

	// TODO: Local check

	succ := node.NextNode

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", succ.Address, succ.Port))
	if err != nil {
		return nodeLocation, err
	}

	method := "Node.SimpleLookup"
	arg := keyID

	err = client.Call(method, arg, &nodeLocation)
	if err != nil {
		return nodeLocation, err
	}

	return nodeLocation, nil
}
