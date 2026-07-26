package pdu

import (
	"bytes"
	"testing"

	"github.com/linxGnu/gosmpp/data"

	"github.com/stretchr/testify/require"
)

func TestBindResponse(t *testing.T) {
	t.Run("receiver", func(t *testing.T) {
		v := NewBindReceiverResp().(*BindResp)
		require.False(t, v.CanResponse())
		require.Nil(t, v.GetResponse())
		v.SequenceNumber = 13

		v.SystemID = "system_id_fake"

		validate(t,
			v,
			"0000001f80000001000000000000000d73797374656d5f69645f66616b6500",
			data.BIND_RECEIVER_RESP,
		)
	})

	t.Run("transmitter", func(t *testing.T) {
		v := NewBindTransmitterResp().(*BindResp)
		require.False(t, v.CanResponse())
		require.Nil(t, v.GetResponse())
		v.SequenceNumber = 13

		v.SystemID = "system_id_fake"

		validate(t,
			v,
			"0000001f80000002000000000000000d73797374656d5f69645f66616b6500",
			data.BIND_TRANSMITTER_RESP,
		)
	})

	t.Run("transceiver", func(t *testing.T) {
		v := NewBindTransceiverResp().(*BindResp)
		require.False(t, v.CanResponse())
		require.Nil(t, v.GetResponse())
		v.SequenceNumber = 13

		v.SystemID = "system_id_fake"

		validate(t,
			v,
			"0000001f80000009000000000000000d73797374656d5f69645f66616b6500",
			data.BIND_TRANSCEIVER_RESP,
		)
	})
}

// TestBindResponseRoundTrip checks that a marshalled BindResp parses back, for every
// combination of binding type, command status and system_id presence. A rejected
// receiver or transmitter bind previously failed here, because Marshal always wrote
// the system_id C-string while Unmarshal read it back only for a transceiver response
// or an ESME_ROK status, leaving the trailing NUL to be misread as an optional parameter.
func TestBindResponseRoundTrip(t *testing.T) {
	bindings := []struct {
		name string
		typ  BindingType
	}{
		{"receiver", Receiver},
		{"transmitter", Transmitter},
		{"transceiver", Transceiver},
	}
	statuses := []struct {
		name  string
		value data.CommandStatusType
	}{
		{"ok", data.ESME_ROK},
		{"error", data.ESME_RBINDFAIL},
	}
	systemIDs := []struct {
		name  string
		value string
	}{
		{"emptySystemID", ""},
		{"populatedSystemID", "system_id_fake"},
	}

	for _, binding := range bindings {
		for _, status := range statuses {
			for _, systemID := range systemIDs {
				t.Run(binding.name+"/"+status.name+"/"+systemID.name, func(t *testing.T) {
					req := NewBindRequest(binding.typ)
					v := req.GetResponse().(*BindResp)
					v.CommandStatus = status.value
					v.SystemID = systemID.value

					b := NewBuffer(nil)
					v.Marshal(b)

					p, err := Parse(bytes.NewReader(b.Bytes()))
					require.NoError(t, err)

					resp, ok := p.(*BindResp)
					require.True(t, ok)
					require.Equal(t, systemID.value, resp.SystemID)
					require.Equal(t, status.value, resp.CommandStatus)
				})
			}
		}
	}

	t.Run("headerOnly", func(t *testing.T) {
		// A response carrying no body at all: command_length is exactly the header size.
		p, err := Parse(bytes.NewReader(fromHex("00000010800000010000000d00000004")))
		require.NoError(t, err)

		resp, ok := p.(*BindResp)
		require.True(t, ok)
		require.Empty(t, resp.SystemID)
	})

	t.Run("errorWithOptionalParam", func(t *testing.T) {
		v := NewBindReceiverResp().(*BindResp)
		v.SequenceNumber = 13
		v.CommandStatus = data.ESME_RBINDFAIL
		v.SystemID = "system_id_fake"
		v.RegisterOptionalParam(Field{
			Tag:  TagAdditionalStatusInfoText,
			Data: []byte("bind failed\x00"),
		})

		b := NewBuffer(nil)
		v.Marshal(b)

		p, err := Parse(bytes.NewReader(b.Bytes()))
		require.NoError(t, err)

		resp, ok := p.(*BindResp)
		require.True(t, ok)
		require.Equal(t, "system_id_fake", resp.SystemID)
		require.Equal(t, []byte("bind failed\x00"), resp.OptionalParameters[TagAdditionalStatusInfoText].Data)
	})
}
