package pdu

import (
	"testing"

	"github.com/linxGnu/gosmpp/errors"

	"github.com/stretchr/testify/require"
)

func TestParsePDU(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		buf := NewBuffer(fromHex("00000010800000060000000000000001"))
		_, err := Parse(buf)
		require.Nil(t, err)
	})

	t.Run("submit_sm_resp with command_status != 0 ", func(t *testing.T) {
		buf := NewBuffer(fromHex("00000010800000040000005800000001"))
		_, err := Parse(buf)
		require.Nil(t, err)
	})

	t.Run("eof", func(t *testing.T) {
		buf := NewBuffer(nil)
		_, err := Parse(buf)
		require.NotNil(t, err)
	})

	t.Run("invalidCmdLength", func(t *testing.T) {
		buf := NewBuffer(fromHex("0000000f800000060000000000000001"))
		_, err := Parse(buf)
		require.Equal(t, errors.ErrInvalidPDU, err)

		buf = NewBuffer(fromHex("3800000f800000060000000000000001"))
		_, err = Parse(buf)
		require.Equal(t, errors.ErrInvalidPDU, err)
	})

	t.Run("invalidBody", func(t *testing.T) {
		buf := NewBuffer(fromHex("0000001e00000003000000000000000161776179001c1d416c69636572"))
		_, err := Parse(buf)
		require.NotNil(t, err)
	})

	t.Run("invalidPayload", func(t *testing.T) {
		buf := NewBuffer(fromHex("000000118000000400000000000000010012"))
		var b base
		require.NotNil(t, b.unmarshal(buf, func(buf *ByteBuffer) error {
			return nil
		}))

		buf = NewBuffer(fromHex("000000118000000400000000000000010012333333333333333333"))
		require.NotNil(t, b.unmarshal(buf, func(buf *ByteBuffer) error {
			_, _ = buf.ReadN(8)
			return nil
		}))
	})
}

func TestGetOptionalMessagePayload(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		v := newBase()
		v.RegisterOptionalParam(Field{Tag: TagMessagePayload, Data: []byte("hello")})

		payload, found := v.GetOptionalMessagePayload()
		require.True(t, found)
		require.Equal(t, []byte("hello"), payload)
	})

	t.Run("absent", func(t *testing.T) {
		v := newBase()

		payload, found := v.GetOptionalMessagePayload()
		require.False(t, found)
		require.Nil(t, payload)
	})

	t.Run("presentButEmpty", func(t *testing.T) {
		v := newBase()
		v.RegisterOptionalParam(Field{Tag: TagMessagePayload, Data: []byte{}})

		payload, found := v.GetOptionalMessagePayload()
		require.False(t, found)
		require.Nil(t, payload)
	})

	t.Run("otherTagsIgnored", func(t *testing.T) {
		v := newBase()
		v.RegisterOptionalParam(Field{Tag: TagDestBearerType, Data: []byte{95}})

		payload, found := v.GetOptionalMessagePayload()
		require.False(t, found)
		require.Nil(t, payload)
	})

	t.Run("returnedSliceAliasesPDU", func(t *testing.T) {
		v := newBase()
		v.RegisterOptionalParam(Field{Tag: TagMessagePayload, Data: []byte("hello")})

		payload, found := v.GetOptionalMessagePayload()
		require.True(t, found)

		payload[0] = 'j'

		again, found := v.GetOptionalMessagePayload()
		require.True(t, found)
		require.Equal(t, []byte("jello"), again)
	})

	t.Run("fromUnmarshalledPDU", func(t *testing.T) {
		v := newBase()
		require.Nil(t, v.unmarshalOptionalParam(fromHex("042400056869746865")))

		payload, found := v.GetOptionalMessagePayload()
		require.True(t, found)
		require.Equal(t, []byte("hithe"), payload)
	})
}
