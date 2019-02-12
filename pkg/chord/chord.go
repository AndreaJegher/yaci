package chord

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type (
	// Node rapresents a node on a chord node of peers
	Node struct {
		NodeInfo
		Next        NodeInfo
		Pred        NodeInfo
		FingerTable map[uint64]NodeInfo
		Ring        RingInfo
	}

	// NodeInfo holds information about a node
	NodeInfo struct {
		ID      uint64
		Address net.IP
		Port    int
		Running bool
	}

	//RingInfo holds the ring metadata
	RingInfo struct {
		ModuloExponent int
		ModuloBase     int
		Modulo         uint64
		Name           string
	}
)

const (
	sleepTime = 20 * time.Second
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

func dialNode(i NodeInfo) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", i.Address, i.Port))
}

func keyInRange(k uint64, n, s NodeInfo) bool {
	return k > n.ID && k <= s.ID
}

// GenID generate a valid entity identifier
func GenID(item string, modulo uint64) uint64 {
	sum := sha1.Sum([]byte(item))
	return binary.LittleEndian.Uint64(sum[12:]) % modulo
}

// Create creates a new chord ring and returns a new node
func Create(name string, port, base int, exponent int) (Node, error) {
	var n Node

	rpc.Register(n)
	// ERR: check why this -> rpc.HandleHTTP() create error -> multiple registrations for /_goRPC_
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return n, err
	}

	n.Ring.Name = name
	n.Ring.ModuloBase = base
	n.Ring.ModuloExponent = exponent
	n.Ring.Modulo = uint64( math.Pow( float64(n.Ring.ModuloBase), float64(n.Ring.ModuloExponent) ) - 1)
	n.Port = port
	ip, err := externalIP()
	if err != nil {
		return n, nil
	}
	n.Address = ip
	n.ID = GenID(ip.String(), n.Ring.Modulo)
	n.Next.Address = n.Address
	n.Next.Port = n.Port
	n.Running = true

 	// TODO user ServeTLS
	go http.Serve(l, nil)
	return n, nil
}

// Join a chord ring and returns a new node
func Join(i NodeInfo) (Node, error) {
	var n Node
	var next NodeInfo

	c, err := dialNode(i)
	if err != nil {
		return n, err
	}

	err = c.Call("WhichRing", nil, &n.Ring)
	if err != nil {
		return n, err
	}

	// newNode.Pred = nil
	err = n.Lookup(GenID(i.Address.String(), n.Ring.Modulo), &next)
	if err != nil {
		return n, err
	}

	ip, err := externalIP()
	if err != nil {
		return n, err
	}

	n.ID = GenID(ip.String(), n.Ring.Modulo)
	n.Next = next
	n.Running = true

	return n, nil
}

// fixFinger refreshes finger table
func (n Node) fixFinger(key uint64) error {
	var new NodeInfo
	err := n.Lookup(key, &new)
	if err != nil {
		return err
	}
	n.FingerTable[key] = new
	return nil
}

// checkPredecessor check if the predecessor is active
func (n Node) checkPredecessor() error {
	_, err := dialNode(n.Pred)
	return err
}

// closetPreceedingNode return the
func (n Node) closetPreceedingNode(key uint64) NodeInfo {
	prev := 1
	for {
			if val, ok := n.FingerTable[(key - uint64(prev))%n.Ring.Modulo]; ok {
				return val
			}
			prev = prev + 1
	}
	return n.NodeInfo
}

// stabilize verifies immediate successor and notifyies him of itself
func (n Node) stabilize() error {
	var x NodeInfo

	c, err := dialNode(n.Next)
	if err != nil {
		return err
	}

	err = c.Call("Node.GetPredecessor", nil, &x)
	if err != nil {
		return err
	}

	if keyInRange(x.ID, n.NodeInfo, n.Next) {
		n.Next = x
	}

	err = c.Call("Node.Notify", n.NodeInfo, nil)
	if err != nil {
		return err
	}

	return nil
}

// GetPredecessor returns predecessor infos
func (n Node) GetPredecessor(args interface{}, i *NodeInfo) error {
	*i = n.Pred
	return nil
}

// Notify handles predecessors notifications
func (n Node) Notify(i NodeInfo, reply *interface{}) error {
	if n.Pred.Address == nil || (n.Pred.Address != nil && keyInRange(i.ID, n.Pred, n.NodeInfo)) {
		n.Pred = i
	}
	return nil
}

// Lookup finds the node holding the key (scalable implementation)
func (n Node) Lookup(key uint64, i *NodeInfo) error {
	var temp NodeInfo
	if keyInRange(key, n.NodeInfo, n.Next) {
		*i = n.Next
		return nil
	}

	next := n.closetPreceedingNode(key)
	c, err := dialNode(next)
	if err != nil {
		return err
	}

	err = c.Call("Node.Lookup", key, &temp)
	if err != nil {
		return err
	}

	*i = temp
	return nil
}

// SimpleLookup finds the node holding the key (simple implementation)
func (n Node) SimpleLookup(key uint64, i *NodeInfo) error {
	var temp NodeInfo
	if keyInRange(key, n.NodeInfo, n.Next) {
		*i = n.Next
		return nil
	}

	c, err := dialNode(n.Next)
	if err != nil {
		return err
	}

	err = c.Call("Node.SimpleLookup", key, &temp)
	if err != nil {
		return err
	}

	*i = temp
	return nil
}

// WhichRing returns informations about the current ring
func (n Node) WhichRing(args interface{}, r *RingInfo) error {
	*r = n.Ring
	return nil
}
