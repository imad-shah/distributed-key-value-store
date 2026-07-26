package server

import (
	"net"
	"sync"
	"log"
)

type Pool struct {
	mu sync.Mutex
	idle map[string]chan net.Conn
	cap int
}

func NewPool(capacity int) *Pool {
	return &Pool{
		idle: make(map[string]chan net.Conn),
		cap: capacity,
	}
}

func (p *Pool) Get(addr string) (net.Conn, error) {
	p.mu.Lock()
	idleList := p.channelFor(addr)
	p.mu.Unlock()

	select {
	case conn := <-idleList:
		return conn, nil
	default:
		log.Printf("POOL: dialing new connection to %s", addr)
		return net.Dial("tcp", addr)
	}

}

func (p *Pool) Put(addr string, conn net.Conn) {
	p.mu.Lock()
	idleList := p.channelFor(addr)
	p.mu.Unlock()

	select {
	case idleList <- conn:
	default:
		conn.Close()	
	}

}

func (p *Pool) channelFor(addr string) chan net.Conn {
	idleList, ok := p.idle[addr]
	if !ok {
		idleList = make(chan net.Conn, p.cap)
		p.idle[addr] = idleList
	}
	return idleList
}