package gremgo

import (
	"sync"
	"time"
)

// Pool maintains a list of connections.
type Pool struct {
	Dial        func() (*Client, error)
	MaxOpen     int
	MaxLifetime time.Duration
	mu          sync.Mutex
	idle        []*idleConnection
	open        int
	cond        *sync.Cond
	cleanerCh   chan struct{}
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
}

// Get will return an available pooled connection. Either an idle connection or
// by dialing a new one if the pool does not currently have a maximum number
// of active connections.
func (p *Pool) Get() (*PooledConnection, error) {
	// Lock the pool to keep the kids out.
	p.mu.Lock()

	// Wait loop
	for {
		conn := p.first()
		if conn != nil {
			// Remove the connection from the idle slice
			numIdle := len(p.idle)
			copy(p.idle, p.idle[1:])
			p.idle = p.idle[:numIdle-1]
			p.mu.Unlock()
			pc := &PooledConnection{Pool: p, Client: conn.pc.Client}
			return pc, nil
		}

		// No idle connections, try dialing a new one
		if p.MaxOpen == 0 || p.open < p.MaxOpen {
			p.open++
			dial := p.Dial

			// Unlock here so that any other connections that need to be
			// dialed do not have to wait.
			p.mu.Unlock()

			dc, err := dial()
			if err != nil {
				p.mu.Lock()
				p.open--
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
	if (pc.Client != nil && pc.Client.Errored) ||
		(p.MaxLifetime > 0 && time.Now().After(pc.t.Add(p.MaxLifetime))) {
		p.open--
		pc.Client.Close()
		return
	}
	idle := &idleConnection{pc: pc}
	p.idle = append(p.idle, idle)
	p.startCleanerLocked()
}

func (p *Pool) needStartCleaner() bool {
	return p.MaxLifetime > 0 &&
		len(p.idle) > 0 &&
		p.cleanerCh == nil
}

// startCleanerLocked starts connectionCleaner if needed.
func (p *Pool) startCleanerLocked() {
	if p.needStartCleaner() {
		p.cleanerCh = make(chan struct{}, 1)
		go p.connectionCleaner()
	}
}

func (p *Pool) connectionCleaner() {
	const minInterval = time.Second

	d := p.MaxLifetime
	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-p.cleanerCh: // dbclient was closed.
		}

		ml := p.MaxLifetime
		p.mu.Lock()
		if p.closed || len(p.idle) == 0 || ml <= 0 {
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}
		n := time.Now()
		mlExpiredSince := n.Add(-ml)
		var closing []*idleConnection
		for i := 0; i < len(p.idle); i++ {
			c := p.idle[i]
			if (ml > 0 && c.pc.t.Before(mlExpiredSince)) ||
				c.pc.Client.Errored {
				p.open--
				closing = append(closing, c)
				last := len(p.idle) - 1
				p.idle[i] = p.idle[last]
				p.idle[last] = nil
				p.idle = p.idle[:last]
				i--
			}
		}
		p.mu.Unlock()

		for _, c := range closing {
			if c.pc.Client != nil {
				c.pc.Client.Close()
			}
		}

		t.Reset(d)
	}
}

// release decrements active and alerts waiters.
// It is not threadsafe. The caller should manage locking the pool.
func (p *Pool) release() {
	if p.closed {
		return
	}
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

	if p.cleanerCh != nil {
		close(p.cleanerCh)
	}
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
