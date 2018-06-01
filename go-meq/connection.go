package meq

import (
	"bufio"
	"errors"
	"math/rand"
	"net"
	"time"

	"github.com/jadechat/meq/proto"
	"github.com/jadechat/meq/proto/mqtt"
	"go.uber.org/zap"
)

type ConfigOption struct {
	Hosts []string
}

type Connection struct {
	subs  map[string]*sub
	subid map[uint16][][]byte

	conn   net.Conn
	close  chan struct{}
	closed bool

	ackch chan proto.Ack

	msgid uint16

	logger *zap.Logger
}

type sub struct {
	handler MsgHandler
	unread  int
	ch      chan *proto.PubMsg
	acked   bool
}

func Connect(conf *ConfigOption) (*Connection, error) {
	if len(conf.Hosts) == 0 {
		return nil, errors.New("meq servers hosts cant be empty")
	}

	c := &Connection{
		close:  make(chan struct{}, 1),
		logger: initLogger(),
		ackch:  make(chan proto.Ack, 10000),
	}

	c.subs = make(map[string]*sub)
	c.subid = make(map[uint16][][]byte)
	host := conf.Hosts[rand.Intn(len(conf.Hosts))]
	conn, err := net.Dial("tcp", host)
	if err != nil {
		c.logger.Debug("dial to server error", zap.String("host", host), zap.Error(err))
		return nil, err
	}

	// connect to server
	ack := mqtt.Connect{}
	if _, err := ack.EncodeTo(conn); err != nil {
		return nil, err
	}

	// wait for connect ack sent from server
	reader := bufio.NewReaderSize(conn, 65536)
	conn.SetDeadline(time.Now().Add(time.Second * proto.MAX_IDLE_TIME))

	msg, err := mqtt.DecodePacket(reader)
	if err != nil {
		return nil, err
	}

	if msg.Type() == mqtt.TypeOfConnack {
		c.conn = conn

		go c.ping()
		go c.writeLoop()
		go c.loop(conf)

		return c, nil
	}

	return nil, errors.New("connecting to sever failed")
}

func (c *Connection) loop(conf *ConfigOption) {
	for !c.closed {
		c.readLoop()
		host := conf.Hosts[rand.Intn(len(conf.Hosts))]
		conn, err := net.Dial("tcp", host)
		if err != nil {
			c.logger.Debug("dial to server error", zap.String("host", host), zap.Error(err))
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// connect to server
		ack := mqtt.Connect{}
		if _, err := ack.EncodeTo(conn); err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// wait for connect ack sent from server
		reader := bufio.NewReaderSize(conn, 65536)
		conn.SetDeadline(time.Now().Add(time.Second * proto.MAX_IDLE_TIME))

		msg, err := mqtt.DecodePacket(reader)
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if msg.Type() != mqtt.TypeOfConnack {
			conn.Close()
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		c.conn = conn

		go func() {
			// 重新订阅所有历史topic
			for topic, sub := range c.subs {
				c.Subscribe([]byte(topic), sub.handler)
			}
		}()
	}
}

func (c *Connection) Close() {
	c.closed = true

	c.conn.Close()
	c.close <- struct{}{}

}

func (c *Connection) readLoop() error {
	defer func() {
		c.conn.Close()
	}()

	reader := bufio.NewReaderSize(c.conn, 65536)
	for {
		c.conn.SetDeadline(time.Now().Add(time.Second * MAX_CONNECTION_IDLE_TIME))

		msg, err := mqtt.DecodePacket(reader)
		if err != nil {
			return err
		}

		switch msg.Type() {
		case mqtt.TypeOfPublish:
			m := msg.(*mqtt.Publish)
			pl := m.Payload
			if len(pl) > 0 {
				switch pl[0] {
				case proto.MSG_COUNT:
					count := proto.UnpackMsgCount(pl[1:])
					c.subs[string(m.Topic)].unread = count
				case proto.MSG_PUB:
					msgs, _ := proto.UnpackPubMsgs(pl[1:])
					for _, m := range msgs {
						hc := c.subs[string(m.Topic)].ch
						hc <- m
					}
				}
			}

		case mqtt.TypeOfSuback:
			m := msg.(*mqtt.Suback)
			topics, ok := c.subid[m.MessageID]
			if ok {
				for _, topic := range topics {
					sub, ok := c.subs[string(topic)]
					if ok {
						sub.acked = true
					}
				}
			}
		case mqtt.TypeOfPingresp:

		}
	}
}

func (c *Connection) ping() {
	for {
		select {
		case <-time.NewTicker(10 * time.Second).C:
			m := mqtt.Pingreq{}
			m.EncodeTo(c.conn)
		case <-c.close:
			return
		}
	}
}

func (c *Connection) writeLoop() {
	ackcache := make([]proto.Ack, 0, 1000)
	for {
		select {
		case ack := <-c.ackch:
			ackcache = append(ackcache, ack)
			if len(ackcache) == proto.CacheFlushLen {
				msg := mqtt.Publish{
					Header: &mqtt.StaticHeader{
						QOS: 0,
					},
					Payload: proto.PackAck(ackcache, proto.MSG_PUBACK),
				}
				msg.EncodeTo(c.conn)
				ackcache = ackcache[:0]
			}
		case <-time.NewTicker(1 * time.Second).C:
			if len(ackcache) > 0 {
				msg := mqtt.Publish{
					Header: &mqtt.StaticHeader{
						QOS: 0,
					},
					Payload: proto.PackAck(ackcache, proto.MSG_PUBACK),
				}
				msg.EncodeTo(c.conn)
				ackcache = ackcache[:0]
			}
		case <-c.close:
			if len(ackcache) > 0 {
				msg := mqtt.Publish{
					Header: &mqtt.StaticHeader{
						QOS: 0,
					},
					Payload: proto.PackAck(ackcache, proto.MSG_PUBACK),
				}
				msg.EncodeTo(c.conn)
				ackcache = ackcache[:0]
				return
			}
		}
	}
}
