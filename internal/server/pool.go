package server

import (
	"log"
	"net"
	"sync"
	"time"
)

const (
	idleTimeout = 30 * time.Second
)

type dialFunction func(network, address string) (net.Conn, error)

type pooledConn struct {
	conn      net.Conn
	idleSince time.Time
}

type Pool struct {
	mu   sync.Mutex
	idle map[string]chan pooledConn
	cap  int
	dial dialFunction
}

func NewPool(capacity int) *Pool {
	return &Pool{
		idle: make(map[string]chan pooledConn),
		cap:  capacity,
		dial: net.Dial,
	}
}

func (p *Pool) Get(addr string) (net.Conn, error) {
	p.mu.Lock()
	idleList := p.channelFor(addr)
	p.mu.Unlock()

	for {
		select {
		case pooled := <-idleList:
			if time.Since(pooled.idleSince) > idleTimeout {
				log.Printf("POOL: closing expired connection to %s", addr)
				pooled.conn.Close()
				continue
			}
			log.Printf("POOL: reusing cached connection to %s", addr)
			return pooled.conn, nil
		default:
			return p.DialAlwaysFresh(addr)
		}
	}
}

func (p *Pool) Put(addr string, conn net.Conn) {
	p.mu.Lock()
	idleList := p.channelFor(addr)
	p.mu.Unlock()

	select {
	case idleList <- pooledConn{
		conn:      conn,
		idleSince: time.Now(),
	}:
	default:
		conn.Close()
	}

}

func (p *Pool) channelFor(addr string) chan pooledConn {
	idleList, ok := p.idle[addr]
	if !ok {
		idleList = make(chan pooledConn, p.cap)
		p.idle[addr] = idleList
	}
	return idleList
}

// DialAlwaysFresh will always attempt to establish
// a fresh tcp connection to an address, whereas
// Get() will attempt to reuse a connection first
func (p *Pool) DialAlwaysFresh(addr string) (net.Conn, error) {
	log.Printf("POOL: dialing fresh connection to %s", addr)
	return p.dial("tcp", addr)
}
