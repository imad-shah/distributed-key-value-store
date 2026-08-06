package hashring

import (
	"errors"
	"hash/fnv"
	"slices"
	"strconv"
	"sync"
)

var (
	ErrEmptyRing = errors.New("hashring: ring is empty")
	ErrServerNotFound = errors.New("hashring: server not found")
	ErrTooManyReplicas = errors.New("hashring: too many replicas, not enough servers")
	ErrDuplicateServer = errors.New("hashring: duplicate server detected")
)

type Ring struct {
	mu      sync.RWMutex
	hashes  []uint64
	ringMap map[uint64]string
	vnodes  uint64
	servers map[string]struct{}
}

func New(vnodes uint64) *Ring {
	if vnodes == 0 {
		panic("hashring: vnodes must be greater than 0")
	}
	return &Ring{
		hashes:  make([]uint64, 0),
		ringMap: make(map[uint64]string),
		servers: make(map[string]struct{}),
		vnodes:  vnodes,
	}
}

func (r *Ring) AddServer(server string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.servers[server]; ok {
		return ErrDuplicateServer
	}
	vnodeHashes := generateVNodes(server, r.vnodes)
	for _, vnode := range vnodeHashes {
		r.hashes = append(r.hashes, vnode)
		r.ringMap[vnode] = server
	}
	slices.Sort(r.hashes)
	r.servers[server] = struct{}{}
	return nil
}

func (r *Ring) GetServer(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 {
		return "", ErrEmptyRing
	}
	keyHash := hashString(key)
	idx, _ := slices.BinarySearch(r.hashes, keyHash)
	if idx >= len(r.hashes) {
		return r.ringMap[r.hashes[0]], nil
	}

	targetHash := r.hashes[idx]
	return r.ringMap[targetHash], nil

}

func (r *Ring) GetNServers(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return nil, ErrEmptyRing
	}

	if n > len(r.servers) {
		return nil, ErrTooManyReplicas
	}

	hashedKey := hashString(key)
	idx, _ := slices.BinarySearch(r.hashes, hashedKey)
	if idx >= len(r.hashes) {
		idx = 0
	}

	res := make([]string, 0, n)
	seen := make(map[string]struct{}, n)

	for len(res) < n {
		server := r.ringMap[r.hashes[idx]]
		if _, ok := seen[server]; !ok {
			seen[server] = struct{}{}
			res = append(res, server)
		}
		idx = (idx + 1) % len(r.hashes)
	}
	return res, nil
}

func (r *Ring) RemoveServer(server string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	vnodeHashes := generateVNodes(server, r.vnodes)

	if r.ringMap[vnodeHashes[0]] != server {
		return ErrServerNotFound
	}

	vnodesSet := make(map[uint64]struct{}, len(vnodeHashes))
	for _, hash := range vnodeHashes {
		vnodesSet[hash] = struct{}{}
	}

	newHashSlice := make([]uint64, 0, len(r.hashes))
	for _, hash := range r.hashes {
		if _, ok := vnodesSet[hash]; !ok {
			newHashSlice = append(newHashSlice, hash)
		}
	}
	r.hashes = newHashSlice

	for hash := range vnodesSet {
		delete(r.ringMap, hash)
	}
	delete(r.servers, server)
	return nil
}

func hashString(s string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(s))
	return hash.Sum64()
}

func generateVNodes(s string, vnodes uint64) []uint64 {
	res := make([]uint64, 0, vnodes)
	for i := range vnodes {
		vnode := strconv.Itoa(int(i)) + s
		virtualHash := hashString(vnode)
		res = append(res, virtualHash)
	}
	return res
}
