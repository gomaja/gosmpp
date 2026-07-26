package gosmpp

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newPipePair returns a connection whose peer drains everything written to it,
// so daemons can run without a real peer. The returned func closes both ends.
func newPipePair() (*Connection, func()) {
	client, peer := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			if _, err := peer.Read(buf); err != nil {
				return
			}
		}
	}()
	return NewConnection(client), func() {
		_ = peer.Close()
		_ = client.Close()
		<-done
	}
}

// TestStartCloseConcurrent runs start and close against each other on the same
// instance. Both register daemons with a sync.WaitGroup and wait on it, so an
// unsynchronised pair lets Add run concurrently with Wait, which the race
// detector reports and which sync.WaitGroup documents as undefined behaviour.
// Run with -race for this to be meaningful.
func TestStartCloseConcurrent(t *testing.T) {
	const iterations = 50

	t.Run("transmittable", func(t *testing.T) {
		for range iterations {
			conn, cleanup := newPipePair()
			v := newTransmittable(conn, Settings{
				WriteTimeout: time.Second,
				EnquireLink:  50 * time.Millisecond,
			}, nil)

			raceStartAndClose(func() { v.start() }, func() { _ = v.close(StoppingProcessOnly) })
			cleanup()
		}
	})

	t.Run("receivable", func(t *testing.T) {
		for range iterations {
			conn, cleanup := newPipePair()
			v := newReceivable(conn, Settings{ReadTimeout: time.Second}, nil)

			raceStartAndClose(func() { v.start() }, func() { _ = v.close(StoppingProcessOnly) })
			cleanup()
		}
	})

	t.Run("transceivable", func(t *testing.T) {
		for range iterations {
			conn, cleanup := newPipePair()
			v := newTransceivable(conn, Settings{
				ReadTimeout:  time.Second,
				WriteTimeout: time.Second,
				EnquireLink:  50 * time.Millisecond,
			}, nil)

			raceStartAndClose(func() { v.start() }, func() { _ = v.Close() })
			cleanup()
		}
	})
}

// raceStartAndClose releases start and close simultaneously and waits for both.
func raceStartAndClose(start, closeFn func()) {
	var wg sync.WaitGroup
	wg.Add(2)

	release := make(chan struct{})
	go func() {
		defer wg.Done()
		<-release
		start()
	}()
	go func() {
		defer wg.Done()
		<-release
		closeFn()
	}()

	close(release)
	wg.Wait()
}

// TestCloseWithoutStart covers the ordering where close arrives on an instance
// that was never started: it must not block waiting for a registration that is
// never going to happen, and a start afterwards must stay a no-op.
func TestCloseWithoutStart(t *testing.T) {
	t.Run("transmittable", func(t *testing.T) {
		conn, cleanup := newPipePair()
		defer cleanup()

		v := newTransmittable(conn, Settings{WriteTimeout: time.Second}, nil)
		requireReturns(t, func() { _ = v.close(StoppingProcessOnly) })
		requireReturns(t, v.start)
	})

	t.Run("receivable", func(t *testing.T) {
		conn, cleanup := newPipePair()
		defer cleanup()

		v := newReceivable(conn, Settings{ReadTimeout: time.Second}, nil)
		requireReturns(t, func() { _ = v.close(StoppingProcessOnly) })
		requireReturns(t, v.start)
	})

	t.Run("transceivable", func(t *testing.T) {
		conn, cleanup := newPipePair()
		defer cleanup()

		v := newTransceivable(conn, Settings{
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		}, nil)
		requireReturns(t, func() { _ = v.Close() })
		requireReturns(t, v.start)
	})
}

// TestStartIdempotent checks that repeat starts do not register extra daemons,
// which would leave close waiting on a counter that never drains.
func TestStartIdempotent(t *testing.T) {
	conn, cleanup := newPipePair()
	defer cleanup()

	v := newTransceivable(conn, Settings{
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}, nil)

	v.start()
	v.start()
	v.start()

	requireReturns(t, func() { _ = v.Close() })
}

// requireReturns fails the test if fn has not returned within a few seconds,
// which is how a deadlock surfaces here rather than as a hung test binary.
func requireReturns(t *testing.T, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "call did not return within 5s")
	}
}
