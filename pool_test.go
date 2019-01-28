package gremgo

import (
	"testing"
	"time"
)

func TestConnectionCleaner(t *testing.T) {
	n := time.Now()

	// invalid has timedout and should be cleaned up
	invalid := &idleConnection{pc: &PooledConnection{Client: &Client{}, t: n.Add(-1030 * time.Millisecond)}}
	// valid has not yet timed out and should remain in the idle pool
	valid := &idleConnection{pc: &PooledConnection{Client: &Client{}, t: n.Add(1030 * time.Millisecond)}}

	// Pool has a 30 second timeout and an idle connection slice containing both
	// the invalid and valid idle connections
	p := &Pool{MaxLifetime: time.Second * 1, idle: []*idleConnection{invalid, valid}}

	if len(p.idle) != 2 {
		t.Errorf("Expected 2 idle connections, got %d", len(p.idle))
	}

	p.mu.Lock()
	p.startCleanerLocked()
	p.mu.Unlock()

	time.Sleep(1010 * time.Millisecond)
	if len(p.idle) != 1 {
		t.Errorf("Expected 1 idle connection after clean, got %d", len(p.idle))
	}

	if p.idle[0].pc.t != valid.pc.t {
		t.Error("Expected the valid connection to remain in idle pool")
	}

}

func TestPurgeErrorClosedConnection(t *testing.T) {
	n := time.Now()

	p := &Pool{MaxLifetime: time.Second * 1}

	valid := &idleConnection{pc: &PooledConnection{Client: &Client{}, t: n.Add(1030 * time.Millisecond)}}

	client := &Client{}

	closed := &idleConnection{pc: &PooledConnection{Pool: p, Client: client, t: n.Add(1030 * time.Millisecond)}}

	idle := []*idleConnection{valid, closed}

	p.idle = idle

	// Simulate error
	closed.pc.Client.Errored = true

	if len(p.idle) != 2 {
		t.Errorf("Expected 2 idle connections, got %d", len(p.idle))
	}

	p.mu.Lock()
	p.startCleanerLocked()
	p.mu.Unlock()
	time.Sleep(1010 * time.Millisecond)

	if len(p.idle) != 1 {
		t.Errorf("Expected 1 idle connection after clean, got %d", len(p.idle))
	}

	if p.idle[0] != valid {
		t.Error("Expected valid connection to remain in pool")
	}
}

func TestPooledConnectionClose(t *testing.T) {
	pool := &Pool{}
	pc := &PooledConnection{Pool: pool}

	if len(pool.idle) != 0 {
		t.Errorf("Expected 0 idle connection, got %d", len(pool.idle))
	}

	pc.Close()

	if len(pool.idle) != 1 {
		t.Errorf("Expected 1 idle connection, got %d", len(pool.idle))
	}

	idled := pool.idle[0]

	if idled == nil {
		t.Error("Expected to get connection")
	}
}

func TestFirst(t *testing.T) {
	n := time.Now()
	pool := &Pool{MaxOpen: 1, MaxLifetime: 30 * time.Millisecond}
	idled := []*idleConnection{
		&idleConnection{pc: &PooledConnection{Pool: pool, Client: &Client{}, t: n.Add(-45 * time.Millisecond)}}, // expired
		&idleConnection{pc: &PooledConnection{Pool: pool, Client: &Client{}, t: n.Add(-45 * time.Millisecond)}}, // expired
		&idleConnection{pc: &PooledConnection{Pool: pool, Client: &Client{}}},                                   // valid
	}
	pool.idle = idled

	if len(pool.idle) != 3 {
		t.Errorf("Expected 3 idle connection, got %d", len(pool.idle))
	}

	// Get should return the last idle connection and clean the others
	c := pool.first()

	if c != pool.idle[0] {
		t.Error("Expected to get first connection in idle slice")
	}

	// Empty pool should return nil
	emptyPool := &Pool{}

	c = emptyPool.first()

	if c != nil {
		t.Errorf("Expected nil, got %T", c)
	}
}

func TestGetAndDial(t *testing.T) {
	n := time.Now()

	pool := &Pool{MaxLifetime: time.Millisecond * 30}

	invalid := &idleConnection{pc: &PooledConnection{Pool: pool, Client: &Client{}, t: n.Add(-30 * time.Millisecond)}}

	idle := []*idleConnection{invalid}
	pool.idle = idle

	client := &Client{}
	pool.Dial = func() (*Client, error) {
		return client, nil
	}

	if len(pool.idle) != 1 {
		t.Error("Expected 1 idle connection")
	}

	if pool.idle[0] != invalid {
		t.Error("Expected invalid connection")
	}

	pool.mu.Lock()
	pool.startCleanerLocked()
	pool.mu.Unlock()
	time.Sleep(1010 * time.Millisecond)

	conn, err := pool.Get()

	if err != nil {
		t.Error(err)
	}

	if len(pool.idle) != 0 {
		t.Errorf("Expected 0 idle connections, got %d", len(pool.idle))
	}

	if conn.Client != client {
		t.Error("Expected correct client to be returned")
	}

	if pool.open != 1 {
		t.Errorf("Expected 1 opened connection, got %d", pool.open)
	}

	// Close the connection and ensure it was returned to the idle pool
	conn.Close()

	if len(pool.idle) != 1 {
		t.Error("Expected connection to be returned to idle pool")
	}

	if pool.open != 1 {
		t.Errorf("Expected 1 opened connections, got %d", pool.open)
	}

	// Get a new connection and ensure that it is the now idling connection
	conn, err = pool.Get()

	if err != nil {
		t.Error(err)
	}

	if conn.Client != client {
		t.Error("Expected the same connection to be reused")
	}

	if pool.open != 1 {
		t.Errorf("Expected 1 opened connection, got %d", pool.open)
	}
}
