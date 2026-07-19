package hashring

import (
	"errors"
	"hash/fnv"
	"slices"
)

var ErrEmptyRing = errors.New("hashring: ring is empty")

type Ring struct {
	hashes  []uint64
	ringMap map[uint64]string
	vnodes  uint64
}

func New(vnodes uint64) *Ring {
	return &Ring{
		hashes:  make([]uint64, 0),
		ringMap: make(map[uint64]string),
		vnodes:  vnodes,
	}
}

func (r *Ring) AddServer(s string) {
	serverHash := hashString(s)
	r.hashes = append(r.hashes, serverHash)
	r.ringMap[serverHash] = s
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

func hashString(s string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(s))
	return hash.Sum64()
}
