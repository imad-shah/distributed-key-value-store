package cluster

import (
	"fmt"
	"strings"

	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
)

type Node struct {
	id string
	addr string
	members map[string]struct{}
	addrBook map[string]string
	ring *hashring.Ring
}

func New(id, addr, peersRaw string, ring *hashring.Ring) (*Node, error){
	peers, err := parsePeers(peersRaw)
	if err != nil {
		return nil, err
	}

	members := map[string]struct{}{id: {}} // add self
	addrBook := map[string]string{id: addr}   // add self

	for peerId, peerAddr := range peers {
		members[peerId] = struct{}{}
		addrBook[peerId] = peerAddr
	}

	for memberId := range members {
		ring.AddServer(memberId)
	}

	return &Node{
		id:id,
		addr:addr,
		members:members,
		addrBook: addrBook,
		ring: ring,
	}, nil
}

func (node *Node) OwnerAddr(key string) (addr string, isSelf bool, err error) {
	ownerId, err := node.ring.GetServer(key)
	if err != nil {
		return "", false, err
	}
	return node.addrBook[ownerId], ownerId == node.id, nil
}

func parsePeers(raw string) (map[string]string, error) {
	peerMap := make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return peerMap, nil
	}

	for _, entry := range strings.Split(raw, ","){
		entry = strings.TrimSpace(entry)
		id, addr, found := strings.Cut(entry, "=")
		if !found || id == "" || addr == "" {
			return nil, fmt.Errorf("malformed peer %q, want id=addr", entry)
		}
		peerMap[id] = addr
	}
	return peerMap, nil
}