package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var mockMsgs = []*PubMsg{
	&PubMsg{[]byte("001"), []byte("test"), []byte("hello world1"), false, 0, 1},
	&PubMsg{[]byte("002"), []byte("test"), []byte("hello world2"), false, 0, 1},
	&PubMsg{[]byte("003"), []byte("test"), []byte("hello world3"), false, 0, 1},
	&PubMsg{[]byte("004"), []byte("test"), []byte("hello world4"), false, 0, 1},
	&PubMsg{[]byte("005"), []byte("test"), []byte("hello world5"), false, 0, 1},
	&PubMsg{[]byte("006"), []byte("test"), []byte("hello world6"), false, 0, 1},
	&PubMsg{[]byte("007"), []byte("test"), []byte("hello world7"), false, 0, 1},
	&PubMsg{[]byte("008"), []byte("test"), []byte("hello world8"), false, 0, 1},
	&PubMsg{[]byte("009"), []byte("test"), []byte("hello world9"), false, 0, 1},
	&PubMsg{[]byte("010"), []byte("test"), []byte("hello world10"), false, 0, 1},
	&PubMsg{[]byte("011"), []byte("test"), []byte("hello world11"), false, 0, 1},
}

func TestPubMsgsPackUnpack(t *testing.T) {
	packed := PackPubMsgs(mockMsgs, MSG_PUB)
	unpacked, err := UnpackPubMsgs(packed[1:])

	assert.NoError(t, err)
	assert.Equal(t, mockMsgs, unpacked)
}

func TestSubPackUnpack(t *testing.T) {
	topic := []byte("test")

	packed := PackSub(topic)
	ntopic := UnpackSub(packed[5:])

	assert.Equal(t, topic, ntopic)

}

func TestAckPackUnpack(t *testing.T) {
	var acks []Ack
	for _, m := range mockMsgs {
		acks = append(acks, Ack{m.Topic, m.ID})
	}

	packed := PackAck(acks, MSG_PUBACK)
	unpacked := UnpackAck(packed[1:])

	assert.Equal(t, acks, unpacked)
}

func TestMsgCountPackUnpack(t *testing.T) {
	count := 10

	packed := PackMsgCount(count)
	ncount := UnpackMsgCount(packed[1:])

	assert.Equal(t, count, ncount)
}

func TestPullPackUnpack(t *testing.T) {
	msgid := []byte("00001")
	count := 10

	packed := PackPullMsg(count, msgid)
	ncount, nmsgid := UnPackPullMsg(packed[1:])

	assert.Equal(t, count, ncount)
	assert.Equal(t, msgid, nmsgid)
}

func TestTimerMsgPackUnpack(t *testing.T) {
	tmsg := &TimerMsg{[]byte("0001"), []byte("test"), []byte("timer msg emit!"), time.Now().Unix(), 10}
	packed := PackTimerMsg(tmsg, MSG_PUB_TIMER)
	unpacked := UnpackTimerMsg(packed[5:])

	assert.Equal(t, tmsg, unpacked)
}

func TestSubAckPackUnpack(t *testing.T) {
	tp := []byte("test")
	packed := PackSubAck(tp)
	unpacked := UnpackSubAck(packed[5:])

	assert.Equal(t, tp, unpacked)
}

func TestPackAckCount(t *testing.T) {
	count := MAX_PULL_COUNT

	packed := PackAckCount(count)
	ucount := UnpackAckCount(packed[1:])

	assert.Equal(t, count, ucount)

	count = -1

	packed = PackAckCount(count)
	ucount = UnpackAckCount(packed[1:])

	assert.Equal(t, count, ucount)
}

func BenchmarkPubMsgPack(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PackPubMsgs(mockMsgs, MSG_PUB)
	}
}

func BenchmarkPubMsgUnpack(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	packed := PackPubMsgs(mockMsgs, MSG_PUB)
	for i := 0; i < b.N; i++ {
		UnpackPubMsgs(packed[5:])
	}
}
