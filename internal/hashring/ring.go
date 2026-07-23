package hashring

import (
	"errors"
	"hash/fnv"
	"slices"
	"strconv"
)

var ErrEmptyRing = errors.New("hashring: ring is empty")
var ErrServerNotFound = errors.New("hashring: server not found")

type Ring struct {
	hashes  []uint64
	ringMap map[uint64]string
	vnodes  uint64
}

func New(vnodes uint64) *Ring {
	if vnodes == 0 {
		panic("hashring: vnodes must be greater than 0")
	}
	return &Ring{
		hashes:  make([]uint64, 0),
		ringMap: make(map[uint64]string),
		vnodes:  vnodes,
	}
}

func (r *Ring) AddServer(server string) {
	vnodeHashes := generateVNodes(server, r.vnodes)
	for _, vnode := range vnodeHashes {
		r.hashes = append(r.hashes, vnode)
		r.ringMap[vnode] = server
	}
	slices.Sort(r.hashes)
}

func (r *Ring) GetServer(key string) (string, error) {
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

func (r *Ring) RemoveServer(server string) error {
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
