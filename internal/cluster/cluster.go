package cluster

import (
	"fmt"

	"github.com/imad-shah/distributed-key-value-store/internal/hashring"
)

type Node struct {
	id       string
	members  map[string]struct{}
	addrBook map[string]string
	ring     *hashring.Ring
}

type Replica struct {
	ID     string
	Addr   string
	IsSelf bool
}

func NewNodeFromConfig(id string, cfg Config, ring *hashring.Ring) (*Node, error) {
	found := false
	members := make(map[string]struct{}, len(cfg.Nodes))
	addrBook := make(map[string]string, len(cfg.Nodes))

	for _, node := range cfg.Nodes {
		members[node.ID] = struct{}{}
		addrBook[node.ID] = node.ReplicaAddress

		if node.ID == id {
			found = true
		}

		if err := ring.AddServer(node.ID); err != nil {
			return nil, fmt.Errorf("add cluster member %q to ring: %w", node.ID, err)
		}

	}
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrNodeIDNotFound, id)
	}

	return &Node{
		id:       id,
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
			ID:     server,
			Addr:   addr,
			IsSelf: isSelf,
		})
	}
	return res, nil
}

func (node *Node) ID() string {
	return node.id
}
