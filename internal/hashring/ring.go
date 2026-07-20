package hashring

import (
	"errors"
	"hash/fnv"
	"slices"
	"strconv"
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
	for i := range r.vnodes {
		newUUID := strconv.Itoa((int(i))) + s
		virtualHash := hashString(newUUID)
		r.hashes = append(r.hashes, virtualHash)
		r.ringMap[virtualHash] = s
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

// TODO: func (r * Ring) RemoveServer()

func hashString(s string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(s))
	return hash.Sum64()
}
