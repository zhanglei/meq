package service

import (
	"net"

	"github.com/jadechat/meq/proto"
	"github.com/jadechat/meq/proto/mqtt"
)

type pushPacket struct {
	msgs []*proto.PubMsg
	from uint64
}

func pushOnline(from uint64, bk *Broker, msgs []*proto.PubMsg, broadcast bool) {
	topics := make(map[string][]*proto.PubMsg)
	for _, msg := range msgs {
		t := string(msg.Topic)
		topics[t] = append(topics[t], msg)
	}

	for t, msgs := range topics {
		var sesses []TopicSess
		var err error
		if broadcast {
			sesses, err = bk.subtrie.Lookup([]byte(t))
		} else {
			sesses, err = bk.subtrie.LookupExactly([]byte(t))
		}

		if err != nil {
			continue
		}
		for _, sess := range sesses {
			if broadcast { // change the topic to the concrete subscrite topic
				for _, m := range msgs {
					m.Topic = sess.Topic
				}
			}

			if sess.Sess.Addr == bk.cluster.peer.name {
				if sess.Sess.Cid == from {
					continue
				}
				bk.RLock()
				c, ok := bk.clients[sess.Sess.Cid]
				bk.RUnlock()
				if !ok {
					bk.Lock()
					delete(bk.clients, sess.Sess.Cid)
					bk.Unlock()
				} else {
					c.msgSender <- msgs
				}
			} else {
				bk.router.route(sess.Sess, msgs)
			}
		}
	}
}

func pushOne(conn net.Conn, m []*proto.PubMsg) error {
	msg := mqtt.Publish{
		Header: &mqtt.StaticHeader{
			QOS: 0,
		},
		Payload: proto.PackPubMsgs(m, proto.MSG_PUB),
	}
	_, err := msg.EncodeTo(conn)
	return err
}
