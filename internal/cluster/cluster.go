package cluster

import (
	"fmt"
	"log"
	"strings"

	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
)

type Node struct {
	id       string
	addr     string
	members  map[string]struct{}
	addrBook map[string]string
	ring     *hashring.Ring
}

type Replica struct {
	ID string
	Addr string
	IsSelf bool
}

func New(id, addr, peersRaw string, ring *hashring.Ring) (*Node, error) {
	peers, err := parsePeers(peersRaw)
	if err != nil {
		return nil, err
	}

	members := map[string]struct{}{id: {}}  // add self
	addrBook := map[string]string{id: addr} // add self

	for peerId, peerAddr := range peers {
		members[peerId] = struct{}{}
		addrBook[peerId] = peerAddr
	}

	for memberId := range members {
		if err := ring.AddServer(memberId); err != nil {
			return nil, fmt.Errorf("add cluster member %q to ring: %w", memberId, err)
		}
	}
	log.Printf("[%v] sees members %v", id, peers)
	return &Node{
		id:       id,
		addr:     addr,
		members:  members,
		addrBook: addrBook,
		ring:     ring,
	}, nil
}
 
func (node *Node) OwnerAddr(key string) (addr string, isSelf bool, err error) {
	ownerId, err := node.ring.GetServer(key)
	if err != nil {
		return "", false, err
	}
	return node.addrBook[ownerId], ownerId == node.id, nil
}

func (node *Node) Replicas(key string, n int) ([]Replica, error) {
	servers, err := node.ring.GetNServers(key, n)

	if err != nil {
		return nil, err
	}

	res := make([]Replica, 0, n)

	for _, server := range servers {
		addr, ok := node.addrBook[server]
		if !ok {
			return nil, fmt.Errorf("address not found for server %q", server)
		}

		isSelf := node.id == server

		res = append(res, Replica{
			ID: server,
			Addr: addr,
			IsSelf: isSelf,
		})
	}
	return res, nil
}

func parsePeers(raw string) (map[string]string, error) {
	peerMap := make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return peerMap, nil
	}

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		id, addr, found := strings.Cut(entry, "=")
		if !found || id == "" || addr == "" {
			return nil, fmt.Errorf("malformed peer %q, want id=addr", entry)
		}
		peerMap[id] = addr
	}
	return peerMap, nil
}
