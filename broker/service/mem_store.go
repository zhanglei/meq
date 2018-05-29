package service

import (
	"bytes"
	"sync"
	"time"

	"github.com/chaingod/talent"

	"github.com/weaveworks/mesh"

	"github.com/jadechat/meq/proto"
)

type MemStore struct {
	In        chan []*proto.PubMsg
	DB        map[string]map[string]*proto.PubMsg
	DBIndex   map[string][]string
	DBIDIndex map[string]string

	timerDB []*proto.TimerMsg

	bk    *Broker
	cache []*proto.PubMsg

	ackCache []proto.Ack

	//cluster
	send         mesh.Gossip
	pn           mesh.PeerName
	msgSyncCache []*proto.PubMsg
	ackSyncCache []proto.Ack

	sync.RWMutex
}

const (
	MaxCacheLength    = 10000
	MaxMemFlushLength = 100

	MaxSyncMsgLen = 1000
	MaxSyncAckLen = 1000
)

const (
	MEM_MSG_ADD = 'a'
	MEM_MSG_ACK = 'b'
)

func (ms *MemStore) Init() {
	ms.In = make(chan []*proto.PubMsg, MaxCacheLength)
	ms.DB = make(map[string]map[string]*proto.PubMsg)
	ms.DBIndex = make(map[string][]string)
	ms.DBIDIndex = make(map[string]string)
	ms.cache = make([]*proto.PubMsg, 0, MaxCacheLength)
	ms.msgSyncCache = make([]*proto.PubMsg, 0, MaxSyncMsgLen)
	ms.ackSyncCache = make([]proto.Ack, 0, MaxSyncAckLen)

	go func() {
		ms.bk.wg.Add(1)
		defer ms.bk.wg.Done()
		for ms.bk.running {
			msgs := <-ms.In
			ms.Lock()
			ms.cache = append(ms.cache, msgs...)
			ms.Unlock()
		}
	}()

	go func() {
		ms.bk.wg.Add(1)
		defer ms.bk.wg.Done()

		for ms.bk.running {
			time.Sleep(100 * time.Millisecond)
			ms.Flush()
		}
	}()

	go func() {
		ms.bk.wg.Add(1)
		defer ms.bk.wg.Done()

		for ms.bk.running {
			time.Sleep(2 * time.Second)
			ms.FlushAck()
		}
	}()

	go func() {
		ackTimer := time.NewTicker(200 * time.Millisecond).C
		msgTimer := time.NewTicker(200 * time.Millisecond).C
		for {
			select {
			case <-ackTimer: // sync acks
				if len(ms.ackSyncCache) > 0 {
					ms.Lock()
					m := proto.PackAck(ms.ackSyncCache, MEM_MSG_ACK)
					ms.ackSyncCache = make([]proto.Ack, 0, MAX_CHANNEL_LEN)
					ms.Unlock()

					ms.send.GossipBroadcast(MemMsg(m))
				}
			case <-msgTimer: // sync msgs
				if len(ms.msgSyncCache) > 0 {
					ms.Lock()
					m := proto.PackPubMsgs(ms.msgSyncCache, MEM_MSG_ADD)
					ms.msgSyncCache = make([]*proto.PubMsg, 0, MAX_CHANNEL_LEN)
					ms.Unlock()

					ms.send.GossipBroadcast(MemMsg(m))
				}
			}
		}
	}()
}

func (ms *MemStore) Close() {
	close(ms.In)
}

func (ms *MemStore) Put(msgs []*proto.PubMsg) {
	if len(msgs) > 0 {
		var nmsgs []*proto.PubMsg
		for _, msg := range msgs {
			if msg.QoS == proto.QOS1 {
				// qos 1 need to be persistent
				nmsgs = append(nmsgs, msg)
			}
		}

		if len(nmsgs) > 0 {
			ms.In <- nmsgs

			ms.Lock()
			ms.msgSyncCache = append(ms.msgSyncCache, msgs...)
			ms.Unlock()
		}
	}
}

func (ms *MemStore) ACK(acks []proto.Ack) {
	if len(acks) > 0 {
		ms.Lock()
		ms.ackCache = append(ms.ackCache, acks...)
		ms.ackSyncCache = append(ms.ackSyncCache, acks...)
		ms.Unlock()
	}
}

func (ms *MemStore) Flush() {
	temp := ms.cache
	if len(temp) > 0 {
		for _, msg := range temp {
			ms.RLock()
			if _, ok := ms.DBIDIndex[talent.Bytes2String(msg.ID)]; ok {
				ms.RUnlock()
				continue
			}
			ms.RUnlock()

			t := talent.Bytes2String(msg.Topic)
			ms.Lock()
			_, ok := ms.DB[t]
			if !ok {
				ms.DB[t] = make(map[string]*proto.PubMsg)
			}

			//@performance
			ms.DB[t][talent.Bytes2String(msg.ID)] = msg
			ms.DBIndex[t] = append(ms.DBIndex[t], talent.Bytes2String(msg.ID))
			ms.DBIDIndex[talent.Bytes2String(msg.ID)] = t
			ms.Unlock()
		}
		ms.Lock()
		ms.cache = ms.cache[len(temp):]
		ms.Unlock()
	} else {
		// rejust the cache cap length
		ms.Lock()
		if len(ms.cache) == 0 && cap(ms.cache) != MaxCacheLength {
			ms.cache = make([]*proto.PubMsg, 0, MaxCacheLength)
		}
		ms.Unlock()
	}
}

func (ms *MemStore) Get(t []byte, count int, offset []byte) []*proto.PubMsg {
	topic := string(t)

	ms.Lock()
	defer ms.Unlock()
	msgs := ms.DB[topic]
	index := ms.DBIndex[topic]

	var newMsgs []*proto.PubMsg

	if bytes.Compare(offset, proto.MSG_NEWEST_OFFSET) == 0 {
		if count == 0 { // get all messages
			for i := len(index) - 1; i >= 0; i-- {
				msg := msgs[index[i]]
				newMsgs = append(newMsgs, msg)
			}

			return newMsgs
		}

		c := 0

		for i := len(index) - 1; i >= 0; i-- {
			if c >= count {
				break
			}
			msg := msgs[index[i]]
			newMsgs = append(newMsgs, msg)
			c++
		}

	} else {
		// find the position of the offset
		pos := -1
		ot := string(offset)
		for i, id := range index {
			if id == ot {
				pos = i
			}
		}

		// can't find the message  or the message is the last one
		// just return empty
		if pos == -1 || pos == len(index)-1 {
			return newMsgs
		}

		if count == 0 {
			// msg push/im pull messages before offset
			for i := pos - 1; i >= 0; i-- {
				msg := msgs[index[i]]
				newMsgs = append(newMsgs, msg)
			}
		} else {
			c := 0
			// msg push/im pull messages before offset
			for i := pos - 1; i >= 0; i-- {
				if c >= count {
					break
				}
				msg := msgs[index[i]]
				newMsgs = append(newMsgs, msg)
				c++
			}
		}
	}

	return newMsgs
}

func (ms *MemStore) GetCount(topic []byte) int {
	t := string(topic)
	ms.Lock()
	defer ms.Unlock()

	var count int
	for _, m := range ms.DB[t] {
		if !m.Acked {
			count++
		}
	}
	return count
}

func (ms *MemStore) FlushAck() {
	if len(ms.ackCache) == 0 {
		return
	}

	temp := ms.ackCache
	for _, ack := range temp {
		// lookup topic
		ms.RLock()
		t, ok := ms.DBIDIndex[string(ack.Msgid)]
		if !ok {
			ms.RUnlock()
			// newCache = append(newCache, msgid)
			continue
		}

		// set message status to acked
		msg := ms.DB[t][string(ack.Msgid)]
		ms.RUnlock()
		msg.Acked = true
	}

	ms.Lock()
	ms.ackCache = ms.ackCache[len(temp):]
	ms.Unlock()
}

func (ms *MemStore) PutTimerMsg(m *proto.TimerMsg) {
	ms.Lock()
	ms.timerDB = append(ms.timerDB, m)
	ms.Unlock()
}

func (ms *MemStore) GetTimerMsg() []*proto.PubMsg {
	now := time.Now().Unix()

	var newM []*proto.TimerMsg
	var msgs []*proto.PubMsg
	ms.Lock()
	for _, m := range ms.timerDB {
		if m.Trigger <= now {
			msgs = append(msgs, &proto.PubMsg{m.ID, m.Topic, m.Payload, false, proto.TIMER_MSG, 1})
		} else {
			newM = append(newM, m)
		}
	}
	ms.timerDB = newM
	ms.Unlock()

	ms.Put(msgs)

	return msgs
}

/* -------------------------- cluster part -----------------------------------------------*/
// Return a copy of our complete state.
func (ms *MemStore) Gossip() (complete mesh.GossipData) {
	return
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (ms *MemStore) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	return
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (ms *MemStore) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	command := buf[4]
	switch command {
	case MEM_MSG_ADD:
		msgs, _ := proto.UnpackPubMsgs(buf[5:])
		ms.In <- msgs

	case MEM_MSG_ACK:
		msgids := proto.UnpackAck(buf[5:])
		ms.Lock()
		for _, msgid := range msgids {
			ms.ackCache = append(ms.ackCache, msgid)
		}
		ms.Unlock()
	}
	return
}

// Merge the gossiped data represented by buf into our state.
func (ms *MemStore) OnGossipUnicast(src mesh.PeerName, buf []byte) error {
	return nil
}

func (ms *MemStore) register(send mesh.Gossip) {
	ms.send = send
}

type MemMsg []byte

func (mm MemMsg) Encode() [][]byte {
	return [][]byte{mm}
}

func (mm MemMsg) Merge(new mesh.GossipData) (complete mesh.GossipData) {
	return
}
