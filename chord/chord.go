package chord

import (
	"crypto/sha1"
	"fmt"
	"net"
)

type (
  //Ring rapresents a chord ring of peers
	Ring struct {
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

//JoinRing connects to a chord ring and returns an object holding the connection
func JoinRing(ringID string) Ring {
  var ring Ring
  return ring
}

//MapKey maps a key onto a node
func (ring Ring) MapKey(key string) Node {
  var node Node
  return node
}
