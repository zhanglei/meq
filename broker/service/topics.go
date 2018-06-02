package service

import (
	"bytes"
	"encoding/gob"

	"github.com/weaveworks/mesh"
	"go.uber.org/zap"
)

type Topics struct {
	bk   *Broker
	send mesh.Gossip
	pn   mesh.PeerName
	td   *TopicDatas
}

func (ts *Topics) Init(bk *Broker) {
	ts.bk = bk
	ts.td = &TopicDatas{
		Td: make(map[string]TopicData),
	}
}

type TopicDatas struct {
	Td map[string]TopicData
}

type TopicData struct {
	Unread int
}

func (ts *Topics) Gossip() (complete mesh.GossipData) {
	return ts.td
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (ts *Topics) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	var td TopicDatas
	err = gob.NewDecoder(bytes.NewReader(buf)).Decode(&td)
	if err != nil {
		L.Info("on gossip broadcast decode error", zap.Error(err))
		return
	}

	return
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (ts *Topics) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	return
}

// Merge the gossiped data represented by buf into our state.
func (ts *Topics) OnGossipUnicast(src mesh.PeerName, buf []byte) error {
	return nil
}

func (ts *Topics) register(send mesh.Gossip) {
	ts.send = send
}

func (st *TopicDatas) Encode() [][]byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(err)
	}

	return [][]byte{buf.Bytes()}
}

// Merge merges the other GossipData into this one,
// and returns our resulting, complete state.
func (st *TopicDatas) Merge(osubs mesh.GossipData) (complete mesh.GossipData) {
	return
}
