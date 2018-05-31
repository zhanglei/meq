package service

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jadechat/meq/proto"
	"github.com/jadechat/meq/proto/mqtt"
	"go.uber.org/zap"
)

// For controlling dynamic buffer sizes.
const (
	headerSize  = 4
	maxBodySize = 65536 * 16 // 1MB
)

type client struct {
	cid  uint64 // do not exceeds max(int32)
	conn net.Conn
	bk   *Broker

	msgSender chan []*proto.PubMsg
	ackSender chan []proto.Ack

	subs map[string][]byte

	closed bool
}

func initClient(cid uint64, conn net.Conn, bk *Broker) *client {
	return &client{
		cid:       cid,
		conn:      conn,
		bk:        bk,
		msgSender: make(chan []*proto.PubMsg, MAX_CHANNEL_LEN),
		ackSender: make(chan []proto.Ack, MAX_CHANNEL_LEN),
		subs:      make(map[string][]byte),
	}
}
func (c *client) readLoop() error {
	c.bk.wg.Add(1)
	defer func() {
		c.closed = true
		// unsub topics
		for topic, group := range c.subs {
			c.bk.subtrie.UnSubscribe([]byte(topic), group, c.cid, c.bk.cluster.peer.name)
			//@todo
			// aync + batch
			submsg := SubMessage{CLUSTER_UNSUB, []byte(topic), group, c.cid}
			c.bk.cluster.peer.send.GossipBroadcast(submsg)
		}
		c.bk.wg.Done()
		if err := recover(); err != nil {
			return
		}
	}()

	reader := bufio.NewReaderSize(c.conn, 65536)
	for !c.closed {
		c.conn.SetDeadline(time.Now().Add(time.Second * proto.MAX_IDLE_TIME))

		msg, err := mqtt.DecodePacket(reader)
		if err != nil {
			return err
		}

		switch msg.Type() {
		case mqtt.TypeOfSubscribe:
			packet := msg.(*mqtt.Subscribe)
			for _, sub := range packet.Subscriptions {
				t, q := proto.GetTopicAndQueue(sub.Topic)
				c.bk.subtrie.Subscribe(t, q, c.cid, c.bk.cluster.peer.name)
				submsg := SubMessage{CLUSTER_SUB, t, q, c.cid}
				c.bk.cluster.peer.send.GossipBroadcast(submsg)

				c.subs[string(t)] = q
				count := c.bk.store.GetCount(t)
				// push out the count of the unread messages
				msg := mqtt.Publish{
					Header: &mqtt.StaticHeader{
						QOS: 0,
					},
					Topic:   t,
					Payload: proto.PackMsgCount(count),
				}
				msg.EncodeTo(c.conn)
			}

			ack := mqtt.Suback{
				MessageID: packet.MessageID,
			}
			ack.EncodeTo(c.conn)
		case mqtt.TypeOfUnsubscribe:

		case mqtt.TypeOfPingreq:
			pong := mqtt.Pingresp{}
			pong.EncodeTo(c.conn)
		case mqtt.TypeOfDisconnect:
			return nil

		case mqtt.TypeOfPublish:
			packet := msg.(*mqtt.Publish)
			if len(packet.Payload) > 0 {
				cmd := packet.Payload[0]
				switch cmd {
				case proto.MSG_PULL:
					count, offset := proto.UnPackPullMsg(packet.Payload[1:])
					fmt.Println(count, string(offset))
					// pulling out the all messages is not allowed
					if count > MAX_MESSAGE_PULL_COUNT || count <= 0 {
						return fmt.Errorf("the pull count %d is larger than :%d or equal/smaller than 0", count, MAX_MESSAGE_PULL_COUNT)
					}

					// check the topic is already subed
					_, ok := c.subs[string(packet.Topic)]
					if !ok {
						return errors.New("pull messages without subscribe the topic:" + string(packet.Topic))
					}

					msgs := c.bk.store.Get(packet.Topic, count, offset, true)
					c.msgSender <- msgs
				case proto.MSG_PUB:
					// clients publish  messages to a concrete topic
					// single publish will store the messages according to the message qos
					ms, err := proto.UnpackPubMsgs(packet.Payload[1:])
					if err != nil {
						return err
					}

					// save the messages
					c.bk.store.Put(ms)
					// push to online clients in all nodes
					pushOnline(c.cid, c.bk, ms, false)

					// ack the msgs to client of sender
					var acks []proto.Ack
					for _, m := range ms {
						if m.QoS == proto.QOS1 {
							acks = append(acks, proto.Ack{m.Topic, m.ID})
						}
					}
					c.ackSender <- acks

				case proto.MSG_BROADCAST:
					// clients publish messges to a broadcast topic
					// broadcast will not store the messages
					ms, err := proto.UnpackPubMsgs(packet.Payload[1:])
					if err != nil {
						return err
					}
					pushOnline(c.cid, c.bk, ms, true)

				case proto.MSG_PUBACK_COUNT:
					topic := packet.Topic
					count := proto.UnpackAckCount(packet.Payload[1:])
					if count < 0 && count != proto.ACK_ALL_COUNT {
						return fmt.Errorf("malice ack count, topic:%s,count:%d", string(topic), count)
					}

					c.bk.store.AckCount(topic, count)
				case proto.MSG_PUBACK: // clients receive the publish message
					acks := proto.UnpackAck(packet.Payload[1:])
					if len(acks) > 0 {
						// ack the messages
						c.bk.store.ACK(acks)
					}
				}
			}
		}
	}

	// Start read buffer.
	// header := make([]byte, headerSize)
	// for !c.closed {
	// 	// read header
	// 	var bl uint64
	// 	if _, err := talent.ReadFull(c.conn, header, proto.MAX_IDLE_TIME); err != nil {
	// 		return err
	// 	}
	// 	if bl, _ = binary.Uvarint(header); bl <= 0 || bl >= maxBodySize {
	// 		return fmt.Errorf("packet not valid,header:%v,bl:%v", header, bl)
	// 	}

	// 	// read body
	// 	buf := make([]byte, bl)
	// 	if _, err := talent.ReadFull(c.conn, buf, proto.MAX_IDLE_TIME); err != nil {
	// 		return err
	// 	}
	// 	switch buf[0] {

	// 	case proto.MSG_UNSUB: // clients unsubscribe the specify topic
	// 		topic, group := proto.UnpackSub(buf[1:])
	// 		if topic == nil {
	// 			return errors.New("the unsub topic is null")
	// 		}

	// 		c.bk.subtrie.UnSubscribe(topic, group, c.cid, c.bk.cluster.peer.name)
	// 		//@todo
	// 		// aync + batch
	// 		submsg := SubMessage{CLUSTER_UNSUB, topic, group, c.cid}
	// 		c.bk.cluster.peer.send.GossipBroadcast(submsg)

	// 		delete(c.subs, string(topic))

	// 	case proto.MSG_PUB_TIMER, proto.MSG_PUB_RESTORE:
	// 		m := proto.UnpackTimerMsg(buf[1:])
	// 		now := time.Now().Unix()
	// 		// trigger first
	// 		if m.Trigger == 0 {
	// 			if m.Delay != 0 {
	// 				m.Trigger = now + int64(m.Delay)
	// 			}
	// 		}
	// 		if m.Trigger > now {
	// 			c.bk.store.PutTimerMsg(m)
	// 		}

	// 		// ack the timer msg
	// 		c.ackSender <- []proto.Ack{proto.Ack{m.Topic, m.ID}}
	// 	}
	// }

	return nil
}

func (c *client) sendLoop() {
	scache := make([]*proto.PubMsg, 0, MAX_MESSAGE_BATCH)
	acache := make([]proto.Ack, 0, MAX_CHANNEL_LEN)
	defer func() {
		c.closed = true
		c.conn.Close()
		if err := recover(); err != nil {
			L.Warn("panic happend in write loop", zap.Error(err.(error)), zap.Stack("stack"), zap.Uint64("cid", c.cid))
			return
		}
	}()

	for !c.closed {
		select {
		case msgs := <-c.msgSender:
			var err error
			if len(msgs)+len(scache) < MAX_MESSAGE_BATCH {
				scache = append(scache, msgs...)
			} else {
				new := append(scache, msgs...)
				batches := (len(new) / MAX_MESSAGE_BATCH) + 1
				for i := 1; i <= batches; i++ {
					if i < batches {
						err = pushOne(c.conn, new[(i-1)*MAX_MESSAGE_BATCH:i*MAX_MESSAGE_BATCH])
					} else {
						err = pushOne(c.conn, new[(i-1)*MAX_MESSAGE_BATCH:])
					}
				}
				scache = scache[:0]
			}
			if err != nil {
				L.Info("push one error", zap.Error(err))
				return
			}
		case acks := <-c.ackSender:
			acache = append(acache, acks...)

		case <-time.After(500 * time.Millisecond):
			var err error
			if len(scache) > 0 {
				err = pushOne(c.conn, scache)
				scache = scache[:0]
			}

			if len(acache) > 0 {
				msg := mqtt.Publish{
					Header: &mqtt.StaticHeader{
						QOS: 0,
					},
					Payload: proto.PackAck(acache, proto.MSG_PUBACK),
				}
				msg.EncodeTo(c.conn)
			}

			if err != nil {
				L.Info("push one error", zap.Error(err))
				return
			}
		}
	}
}

func (c *client) waitForConnect() error {
	reader := bufio.NewReaderSize(c.conn, 65536)
	c.conn.SetDeadline(time.Now().Add(time.Second * proto.MAX_IDLE_TIME))

	msg, err := mqtt.DecodePacket(reader)
	if err != nil {
		return err
	}

	if msg.Type() == mqtt.TypeOfConnect {
		// packet := msg.(*mqtt.Connect)
		// c.username = string(packet.Username)

		// reply the connect ack
		ack := mqtt.Connack{ReturnCode: 0x00}
		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
		return nil
	}

	return errors.New("first packet is not MSG_CONNECT")
}
