package chord

import (
	"crypto/sha1"
	"fmt"
	"net"
)

type (
  //Ring rapresents a chord ring of peers
	Ring struct {
    RingID string
    ModuloExponent int
    Modulo int
	}

  //Node rapresents a node in a ring
  Node struct {
  }
)

func genNodeIdentifier(ip net.IP) string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(ip.String())))
}

func genKeyIdentifier(key string) string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(key)))
}

//CreateRing creates a new chord ring and returns an object rapresenting the ring, the rindID
func CreateRing(ringID string) (Ring, error) {
  var ring Ring
  return ring, nil
}

//JoinRing connects to a chord ring and returns an object rapresenting the ring
func JoinRing(ringID string) (Ring, error) {
  var ring Ring
  return ring, nil
}

//MapKey maps a key onto a node
func (ring Ring) MapKey(key string) (Node, error) {
  var node Node
  return node, nil
}
