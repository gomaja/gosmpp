package pdu

import (
	"errors"
	"io"

	"github.com/linxGnu/gosmpp/data"
)

// BindResp PDU.
type BindResp struct {
	base
	SystemID string
}

// NewBindResp returns BindResp.
func NewBindResp(req BindRequest) (c *BindResp) {
	c = &BindResp{
		base: newBase(),
	}
	c.SequenceNumber = req.SequenceNumber

	switch req.BindingType {
	case Transceiver:
		c.CommandID = data.BIND_TRANSCEIVER_RESP

	case Receiver:
		c.CommandID = data.BIND_RECEIVER_RESP

	case Transmitter:
		c.CommandID = data.BIND_TRANSMITTER_RESP
	}

	return
}

// NewBindTransmitterResp returns new bind transmitter resp.
func NewBindTransmitterResp() PDU {
	c := &BindResp{
		base: newBase(),
	}
	c.CommandID = data.BIND_TRANSMITTER_RESP
	return c
}

// NewBindTransceiverResp returns new bind transceiver resp.
func NewBindTransceiverResp() PDU {
	c := &BindResp{
		base: newBase(),
	}
	c.CommandID = data.BIND_TRANSCEIVER_RESP
	return c
}

// NewBindReceiverResp returns new bind receiver resp.
func NewBindReceiverResp() PDU {
	c := &BindResp{
		base: newBase(),
	}
	c.CommandID = data.BIND_RECEIVER_RESP
	return c
}

// CanResponse implements PDU interface.
func (c *BindResp) CanResponse() bool {
	return false
}

// GetResponse implements PDU interface.
func (c *BindResp) GetResponse() PDU {
	return nil
}

// Marshal implements PDU interface.
func (c *BindResp) Marshal(b *ByteBuffer) {
	c.base.marshal(b, func(w *ByteBuffer) {
		w.Grow(len(c.SystemID) + 1)

		_ = w.WriteCString(c.SystemID)
	})
}

// Unmarshal implements PDU interface.
//
// The system_id field is read unconditionally, since Marshal always writes it.
// An absent field is tolerated so that responses omitting system_id still parse,
// matching how SubmitSMResp handles its message_id.
func (c *BindResp) Unmarshal(b *ByteBuffer) error {
	return c.base.unmarshal(b, func(w *ByteBuffer) (err error) {
		c.SystemID, err = w.ReadCString()
		if errors.Is(err, io.EOF) {
			return nil
		}
		return
	})
}
