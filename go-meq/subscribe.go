package meq

import (
	"errors"
	"time"

	"github.com/jadechat/meq/proto"
	"github.com/jadechat/meq/proto/mqtt"
)

type MsgHandler func(*proto.PubMsg)

func (c *Connection) Subscribe(topic []byte, f MsgHandler) error {
	_, err := proto.ParseTopic(topic, true)
	if err != nil {
		return err
	}
	sub := &sub{}
	hc := make(chan *proto.PubMsg, 10000)
	sub.ch = hc
	sub.handler = f
	c.subs[string(topic)] = sub

	c.msgid++
	mid := c.msgid
	c.subid[mid] = [][]byte{topic}

	submsg := mqtt.Subscribe{
		MessageID:     c.msgid,
		Subscriptions: []mqtt.TopicQOSTuple{mqtt.TopicQOSTuple{1, topic}},
	}

	submsg.EncodeTo(c.conn)

	// wait for subscribe ok,at max 5 seconds
	n := 0
	for !sub.acked {
		if n > 10 {
			return errors.New("subscribe failed")
		}
		time.Sleep(500 * time.Millisecond)
	}

	go func() {
		for {
			select {
			case m := <-hc:
				f(m)
				if m.QoS == proto.QOS1 && !m.Acked {
					c.ackch <- proto.Ack{m.Topic, m.ID}
				}
			case <-c.close:
				return
			}
		}
	}()

	return nil
}
