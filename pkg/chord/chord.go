package chord

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type (
	// Node rapresents a node on a chord node of peers
	Node struct {
		NodeInfo
		Successors        []NodeInfo
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

	// RingInfo holds the ring metadata
	RingInfo struct {
		ModuloExponent       int
		ModuloBase           int
		Modulo               uint64
		Name                 string
		Timeout              time.Duration
		NextBufferLength int
		FingerTableLength uint64
	}

	// EmptyArgs empty args
	EmptyArgs struct{}
)

func (n Node) dialNode(i NodeInfo) (*rpc.Client, bool, error) {
	if n.Address.Equal(i.Address) && n.Port == i.Port {
		return nil, true, errors.New("cannot dial myself")
	}
	c, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", i.Address, i.Port))
	return c, false, err
}

func keyInRange(k uint64, b, e NodeInfo) bool {
	return ((k > b.ID && k <= e.ID) || (b.ID >= e.ID))
}

func serveNode(n *Node) {
	rpc.Register(n)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		var next uint64
		rand.Seed(time.Now().Unix())
		for n.Running {
			err = n.checkPredecessor()
			if err != nil {
				log.Println("Node ", n.ID, " failing check pred ", err)
			}

			err = n.fixFinger(next)
			if err != nil {
				log.Println("Node ", n.ID, " failing fix finger ", err)
			}
			next = (next + rand.Uint64()%1000) % n.Ring.Modulo

			err := n.stabilize()
			if err != nil {
				log.Println("Node ", n.ID, " failing stabilize ", err)
			}

			time.Sleep(n.Ring.Timeout * time.Millisecond)
		}
		l.Close()
	}()
	log.Println("Node", n.ID, "closing", http.Serve(l, nil))
}

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

// GenID generate a valid entity identifier
func GenID(item string, modulo uint64) uint64 {
	sum := sha1.Sum([]byte(item))
	return binary.LittleEndian.Uint64(sum[12:]) % modulo
}

// Create a chord ring and returns a new node
func Create(port int, r RingInfo) (*Node, error) {
	var n Node

	ip, err := externalIP()
	if err != nil {
		return nil, err
	}

	n.Ring = r
	n.Port = port
	n.Address = ip
	n.ID = GenID(n.Address.String(), n.Ring.Modulo)
	n.Running = true
	n.FingerTable = make(map[uint64]NodeInfo)

	n.Successors = append(n.Successors, n.NodeInfo)
	n.Pred.Address = nil

	go serveNode(&n)
	return &n, nil
}

// Join a chord ring through i and returns a new node
func Join(i NodeInfo, port int) (*Node, error) {
	var n Node

	ip, err := externalIP()
	if err != nil {
		return nil, err
	}
	n.Address = ip

	c, s, err := n.dialNode(i)
	if err != nil {
		if s {
			return &n, nil
		}
		return nil, err
	}

	var args EmptyArgs
	err = c.Call("Node.WhichRing", args, &n.Ring)
	if err != nil {
		return &n, err
	}

	n.ID = GenID(n.Address.String(), n.Ring.Modulo)
	n.Port = port
	n.Running = true
	n.FingerTable = make(map[uint64]NodeInfo)
	n.Pred.Address = nil

	var next NodeInfo
	err = c.Call("Node.Lookup", n.ID, &next)
	if err != nil {
		return nil, err
	}
	n.Successors = append(n.Successors, next)

	go serveNode(&n)
	return &n, nil
}

// closetPreceedingNode return the
func (n *Node) closetPreceedingNode(key uint64) NodeInfo {
	var ti NodeInfo
	ti.ID = key
	val := n.NodeInfo
	for k := range n.FingerTable {
		temp := n.FingerTable[k]
		if keyInRange(temp.ID, val, ti) {
			val = temp
		}
	}
	return val
}

// fixFinger refreshes finger table
func (n *Node) fixFinger(key uint64) error {
	var new NodeInfo
	err := n.Lookup(key, &new)
	if err != nil {
		return err
	}
	if uint64(len(n.FingerTable)) >= n.Ring.FingerTableLength % n.Ring.Modulo {
		for k := range n.FingerTable {
			delete(n.FingerTable, k)
			break
		}
	}
	n.FingerTable[key] = new
	return nil
}

// stabilize verifies immediate successor and notifyies him of itself
func (n *Node) stabilize() error {
	var x NodeInfo

	c, self, err := n.dialNode(n.Successors[0])
	if err != nil && !self {
		return err
	}

	var args EmptyArgs
	if !self {
		err = c.Call("Node.GetPredecessor", args, &x)
		if err != nil {
			return err
		}
	} else {
		x = n.Pred
	}

	if x.Address != nil && keyInRange(x.ID, n.NodeInfo, n.Successors[0]) && x.ID != n.ID {
		n.Successors[0] = x
	}

	c, self, err = n.dialNode(n.Successors[0])
	if err != nil && !self {
		return err
	}

	if self {
		n.Notify(n.NodeInfo, &args)
	} else {
		err = c.Call("Node.Notify", n.NodeInfo, &args)
		if err != nil {
			return err
		}
	}

	return nil
}

// checkPredecessor check if the predecessor is active
func (n *Node) checkPredecessor() error {
	_, s, err := n.dialNode(n.Pred)
	if s {
		return nil
	}
	if err != nil {
		n.Pred.Address = nil
		n.Pred.Port = 0
		n.Pred.ID = 0
	}
	return nil
}

// GetPredecessor returns predecessor infos
func (n *Node) GetPredecessor(args EmptyArgs, i *NodeInfo) error {
	*i = n.Pred
	return nil
}

// GetSuccessor returns successor infos
func (n *Node) GetSuccessor(args EmptyArgs, i *NodeInfo) error {
	*i = n.Successors[0]
	return nil
}

// Notify handles predecessors notifications
func (n *Node) Notify(i NodeInfo, reply *EmptyArgs) error {
	if n.Pred.Address == nil || (keyInRange(i.ID, n.Pred, n.NodeInfo) && !((i.ID == n.ID) && n.ID > n.Pred.ID)) {
		n.Pred = i
	}
	return nil
}

// Lookup finds the node holding the key (scalable implementation)
func (n *Node) Lookup(key uint64, i *NodeInfo) error {
	var temp NodeInfo
	if keyInRange(key, n.NodeInfo, n.Successors[0]) {
		if !(key < n.ID && key > n.Successors[0].ID) {
			*i = n.Successors[0]
			return nil
		}
	}

	next := n.closetPreceedingNode(key)
	c, s, err := n.dialNode(next)
	if err != nil {
		if s {
			*i = n.NodeInfo
			return nil
		}
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
func (n *Node) SimpleLookup(key uint64, i *NodeInfo) error {
	var temp NodeInfo
	if keyInRange(key, n.NodeInfo, n.Successors[0]) {
		if !(key < n.ID && key > n.Successors[0].ID) {
			*i = n.Successors[0]
			return nil
		}
	}

	c, s, err := n.dialNode(n.Successors[0])
	if err != nil {
		*i = n.NodeInfo
		if s {
			return nil
		}
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
func (n *Node) WhichRing(args EmptyArgs, r *RingInfo) error {
	*r = n.Ring
	return nil
}
