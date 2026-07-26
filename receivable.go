package gosmpp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linxGnu/gosmpp/pdu"
)

type receivable struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	settings Settings
	conn     *Connection

	// started is closed by start once every daemon has been registered with wg.
	// close waits for it before calling wg.Wait, so that Add can never run
	// concurrently with Wait. startOnce keeps that close to a single call.
	started   chan struct{}
	startOnce sync.Once

	aliveState   int32
	requestStore RequestStore
}

func newReceivable(conn *Connection, settings Settings, requestStore RequestStore) *receivable {
	r := &receivable{
		settings:     settings,
		conn:         conn,
		started:      make(chan struct{}),
		requestStore: requestStore,
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	return r
}

func (t *receivable) close(state State) (err error) {
	if atomic.CompareAndSwapInt32(&t.aliveState, Alive, Closed) {
		// cancel to notify stop
		t.cancel()

		// set read deadline for current blocking read
		_ = t.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

		// Make sure daemon registration is finished before waiting on wg, so that
		// Add is never called concurrently with Wait. aliveState is already Closed
		// here, so a start that has not run yet registers nothing and returns.
		t.awaitStarted()

		// wait daemons
		t.wg.Wait()

		// close connection to notify daemons to stop
		if state != StoppingProcessOnly {
			err = t.conn.Close()
		}

		// notify receiver closed
		if t.settings.OnClosed != nil {
			t.settings.OnClosed(state)
		}
	}
	return
}

func (t *receivable) closing(state State) {
	go func() {
		_ = t.close(state)
	}()
}

// start registers and launches the read daemon. It is safe to call concurrently
// with close: whichever runs first wins, and the other becomes a no-op or waits.
// Calling it more than once has no additional effect.
func (t *receivable) start() {
	t.startOnce.Do(func() {
		defer close(t.started)

		// Already closed: register nothing, so close's Wait sees a settled counter.
		if atomic.LoadInt32(&t.aliveState) != Alive {
			return
		}

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.loop()
		}()
	})
}

// awaitStarted blocks until daemon registration has settled, running the startup
// path itself if start was never called so that it cannot block indefinitely.
func (t *receivable) awaitStarted() {
	t.start()
	<-t.started
}

func (t *receivable) loop() {
	var err error
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		// read pdu from conn
		var p pdu.PDU
		if err = t.conn.SetReadTimeout(t.settings.ReadTimeout); err == nil {
			p, err = pdu.Parse(t.conn)
		}
		if err != nil {
			if atomic.LoadInt32(&t.aliveState) == Alive {
				if t.settings.OnReceivingError != nil {
					t.settings.OnReceivingError(err)
				}
				t.closing(InvalidStreaming)
			}
			return
		}

		var closeOnUnbind bool
		if p != nil {
			if t.settings.WindowedRequestTracking != nil && t.settings.OnExpectedPduResponse != nil {
				closeOnUnbind = t.handleWindowPdu(p)
			} else if t.settings.OnAllPDU != nil {
				closeOnUnbind = t.handleAllPdu(p)
			} else {
				closeOnUnbind = t.handleOrClose(p)
			}
			if closeOnUnbind {
				t.closing(UnbindClosing)
			}
		}

	}
}

func (t *receivable) handleWindowPdu(p pdu.PDU) (closing bool) {
	if t.settings.WindowedRequestTracking != nil && t.settings.OnExpectedPduResponse != nil && p != nil {
		// This case must match the same request item list in transmittable write func
		switch pp := p.(type) {
		case *pdu.CancelSMResp,
			*pdu.DataSMResp,
			*pdu.DeliverSMResp,
			*pdu.EnquireLinkResp,
			*pdu.QuerySMResp,
			*pdu.ReplaceSMResp,
			*pdu.SubmitMultiResp,
			*pdu.SubmitSMResp:
			if t.settings.OnExpectedPduResponse != nil {
				ctx, cancelFunc := context.WithTimeout(context.Background(), t.settings.StoreAccessTimeOut)
				defer cancelFunc()
				request, ok := t.requestStore.Get(ctx, p.GetSequenceNumber())
				if ok {
					_ = t.requestStore.Delete(ctx, p.GetSequenceNumber())
					response := Response{
						PDU:             p,
						OriginalRequest: request,
					}
					t.settings.OnExpectedPduResponse(response)
				} else if t.settings.OnUnexpectedPduResponse != nil {
					t.settings.OnUnexpectedPduResponse(p)
				}
			}
		case *pdu.EnquireLink:
			if t.settings.EnableAutoRespond {
				t.settings.response(pp.GetResponse())
			} else if t.settings.OnReceivedPduRequest != nil {
				r, _ := t.settings.OnReceivedPduRequest(p)
				t.settings.response(r)

			}
		case *pdu.Unbind:
			if t.settings.EnableAutoRespond {
				t.settings.response(pp.GetResponse())

				// wait to send response before closing
				time.Sleep(50 * time.Millisecond)
				closing = true
			} else if t.settings.OnReceivedPduRequest != nil {
				r, closeBind := t.settings.OnReceivedPduRequest(p)
				t.settings.response(r)
				if closeBind {
					time.Sleep(50 * time.Millisecond)
					closing = true
				}
			}
		default:
			if t.settings.OnReceivedPduRequest != nil {
				r, closeBind := t.settings.OnReceivedPduRequest(p)
				t.settings.response(r)
				if closeBind {
					time.Sleep(50 * time.Millisecond)
					closing = true
				}
			}
		}
	}
	return
}

func (t *receivable) handleAllPdu(p pdu.PDU) (closing bool) {
	if t.settings.OnAllPDU != nil && p != nil {
		r, closeBind := t.settings.OnAllPDU(p)
		t.settings.response(r)
		if closeBind {
			time.Sleep(50 * time.Millisecond)
			closing = true
		}
	}
	return
}

func (t *receivable) handleOrClose(p pdu.PDU) (closing bool) {
	if p != nil {
		switch pp := p.(type) {
		case *pdu.EnquireLink:
			t.settings.response(pp.GetResponse())

		case *pdu.Unbind:
			t.settings.response(pp.GetResponse())
			// wait to send response before closing
			time.Sleep(50 * time.Millisecond)

			closing = true

		default:
			var responded bool
			if p.CanResponse() {
				t.settings.response(p.GetResponse())
				responded = true
			}

			if t.settings.OnPDU != nil {
				t.settings.OnPDU(p, responded)
			}
		}
	}
	return
}
