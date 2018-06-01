package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTopic(t *testing.T) {
	input := [][]byte{
		[]byte("/"),
		[]byte("a/b"),
		[]byte("a/b/"),
		[]byte("/a/b"),
		[]byte("a/b/c"),
		[]byte("/a/b/c"),
		[]byte("/asdf/bse/dewer"),
		[]byte("/a"),
		[]byte("/a//b"),
		[]byte("/+/b/c"),
		[]byte("/a/+/c"),
	}
	out := [][]uint32{
		nil,
		nil,
		nil,
		nil,
		nil,
		[]uint32{3238259379, 500706888, 1027807523},
		[]uint32{1753631938, 324405670, 3531030695},
		nil,
		nil,
		nil,
		nil,
	}
	for i, v := range input {
		ids, _ := ParseTopic(v, true)
		assert.Equal(t, out[i], ids)
	}
}

func TestAppidAndSendTag(t *testing.T) {
	topic := []byte("/")
	_, _, err := AppidAndSendTag(topic)
	assert.Error(t, err)

	topic = []byte("/a")
	_, _, err = AppidAndSendTag(topic)
	assert.Error(t, err)

	topic = []byte("/a/b")
	_, _, err = AppidAndSendTag(topic)
	assert.Error(t, err)

	topic = []byte("/a/b/c")
	_, _, err = AppidAndSendTag(topic)
	assert.Error(t, err)

	topic = []byte("/1234567890/b/c")
	_, _, err = AppidAndSendTag(topic)
	assert.Error(t, err)

	topic = []byte("/1234567890/1/c")
	appid, sendtag, _ := AppidAndSendTag(topic)
	assert.EqualValues(t, []byte("1234567890"), appid)
	assert.EqualValues(t, '1', sendtag)

	topic = []byte("/1234567890/12/c")
	_, _, err = AppidAndSendTag(topic)
	assert.Error(t, err)

	topic = []byte("/1234567890/1/")
	_, _, err = AppidAndSendTag(topic)
	assert.Error(t, err)
}
