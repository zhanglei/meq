package service

import (
	"github.com/jadechat/meq/proto"
)

type Storage interface {
	Init()
	Close()

	Put([]*proto.PubMsg)
	ACK([]proto.Ack)

	Get([]byte, int, []byte) []*proto.PubMsg
	GetCount([]byte) int

	PutTimerMsg(*proto.TimerMsg)
	GetTimerMsg() []*proto.PubMsg
}
