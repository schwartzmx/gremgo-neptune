package gremgo

import (
	"sync"
	"time"
)

// Pool maintains a list of connections.
type Pool struct {
	Dial        func() (*Client, error)
	MaxActive   int
	IdleTimeout time.Duration
	MaxLifetime time.Duration
	mu          sync.Mutex
	idle        []*idleConnection
	active      int
	cond        *sync.Cond
	closed      bool
}

// PooledConnection represents a shared and reusable connection.
type PooledConnection struct {
	Pool   *Pool
	Client *Client
	t      time.Time
}

type idleConnection struct {
	pc *PooledConnection
	// t is the time the connection was idled
	t time.Time
}

// Get will return an available pooled connection. Either an idle connection or
// by dialing a new one if the pool does not currently have a maximum number
// of active connections.
func (p *Pool) Get() (*PooledConnection, error) {
	// Lock the pool to keep the kids out.
	p.mu.Lock()

	now := time.Now()

	// Wait loop
	for {
		// Try to grab first available idle connection
		for {
			conn := p.first()
			if conn == nil {
				break
			}
			numFree := len(p.idle)
			copy(p.idle, p.idle[1:])
			p.idle = p.idle[:numFree-1]
			if (p.IdleTimeout > 0 && conn.t.Add(p.IdleTimeout).Before(now)) ||
				(p.MaxLifetime > 0 && conn.pc.t.Add(p.MaxLifetime).Before(now)) {
				conn.pc.Client.Close()
				continue
			}

			// Remove the connection from the idle slice
			p.active++
			p.mu.Unlock()
			pc := &PooledConnection{Pool: p, Client: conn.pc.Client}
			return pc, nil
		}

		// No idle connections, try dialing a new one
		if p.MaxActive == 0 || p.active < p.MaxActive {
			p.active++
			dial := p.Dial

			// Unlock here so that any other connections that need to be
			// dialed do not have to wait.
			p.mu.Unlock()

			dc, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				return nil, err
			}

			pc := &PooledConnection{Pool: p, Client: dc, t: time.Now()}
			return pc, nil
		}

		//No idle connections and max active connections, let's wait.
		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}

		p.cond.Wait()
	}
}

// put pushes the supplied PooledConnection to the top of the idle slice to be reused.
// It is not threadsafe. The caller should manage locking the pool.
func (p *Pool) put(pc *PooledConnection) {
	if p.closed {
		pc.Client.Close()
		return
	}
	idle := &idleConnection{pc: pc, t: time.Now()}
	// Prepend the connection to the front of the slice
	p.idle = append([]*idleConnection{idle}, p.idle...)

}

// purge removes expired idle connections from the pool.
// It is not threadsafe. The caller should manage locking the pool.
func (p *Pool) purge() {
	it := p.IdleTimeout
	ml := p.MaxLifetime
	if it > 0 || ml > 0 {
		var valid []*idleConnection
		now := time.Now()
		for _, v := range p.idle {
			// If the client has an error then exclude it from the pool
			if v.pc.Client.Errored {
				continue
			}

			if it > 0 && v.t.Add(it).Before(now) {
				// Force underlying connection closed
				v.pc.Client.Close()
				continue
			}
			if ml > 0 && v.pc.t.Add(ml).Before(now) {
				v.pc.Client.Close()
				continue
			}
			valid = append(valid, v)
		}
		p.idle = valid
	}
}

// release decrements active and alerts waiters.
// It is not threadsafe. The caller should manage locking the pool.
func (p *Pool) release() {
	if p.closed {
		return
	}
	p.active--
	if p.cond != nil {
		p.cond.Signal()
	}

}

func (p *Pool) first() *idleConnection {
	if len(p.idle) == 0 {
		return nil
	}
	return p.idle[0]
}

// Close closes the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.idle {
		c.pc.Client.Close()
	}
	p.closed = true
}

// Close signals that the caller is finished with the connection and should be
// returned to the pool for future use.
func (pc *PooledConnection) Close() {
	pc.Pool.mu.Lock()
	defer pc.Pool.mu.Unlock()

	pc.Pool.put(pc)
	pc.Pool.release()
}
