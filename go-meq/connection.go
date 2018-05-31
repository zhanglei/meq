package meq

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"time"

	"github.com/chaingod/talent"
	"github.com/jadechat/meq/proto"
	"go.uber.org/zap"
)

type ConfigOption struct {
	Hosts []string
}

type Connection struct {
	subs map[string]*sub

	conn   net.Conn
	close  chan struct{}
	closed bool

	ackch chan proto.Ack

	logger *zap.Logger
}

type sub struct {
	queue   []byte
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

	host := conf.Hosts[rand.Intn(len(conf.Hosts))]
	conn, err := net.Dial("tcp", host)
	if err != nil {
		c.logger.Debug("dial to server error", zap.String("host", host), zap.Error(err))
		return nil, err
	}

	// connect to server
	msg := proto.PackConnect()
	conn.Write(msg)

	// wait for connect ack sent from server
	_, err = talent.ReadFull(conn, msg, 0)
	if err != nil {
		c.logger.Debug("read from server error", zap.String("host", host), zap.Error(err))
		conn.Close()
		return nil, err
	}
	if msg[4] != proto.MSG_CONNECT_OK {
		conn.Close()
		return nil, errors.New("first packet receved from server is not connect ack")
	}

	c.conn = conn

	go c.ping()
	go c.writeLoop()
	go c.loop(conf)

	return c, nil
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
		msg := proto.PackConnect()
		conn.Write(msg)

		// wait for connect ack sent from server
		_, err = talent.ReadFull(conn, msg, 0)
		if err != nil {
			c.logger.Debug("read from server error", zap.String("host", host), zap.Error(err))
			conn.Close()
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		if msg[4] != proto.MSG_CONNECT_OK {
			conn.Close()
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		c.conn = conn

		go func() {
			// 重新订阅所有历史topic
			for topic, sub := range c.subs {
				c.Subscribe([]byte(topic), sub.queue, sub.handler)
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
	for {
		header := make([]byte, 4)
		_, err := talent.ReadFull(c.conn, header, MAX_CONNECTION_IDLE_TIME)
		if err != nil {
			return err
		}

		hl, _ := binary.Uvarint(header)
		if hl <= 0 {
			return errors.New("read packet header error")
		}
		msg := make([]byte, hl)
		talent.ReadFull(c.conn, msg, MAX_CONNECTION_IDLE_TIME)

		switch msg[0] {
		case proto.MSG_PONG:
		case proto.MSG_SUBACK:
			topic := proto.UnpackSubAck(msg[1:])
			sub, ok := c.subs[string(topic)]
			if ok {
				sub.acked = true
			}
		case proto.MSG_PUB:
			msgs, _ := proto.UnpackPubMsgs(msg[1:])
			for _, m := range msgs {
				hc := c.subs[string(m.Topic)].ch
				hc <- m
			}
		case proto.MSG_COUNT:
			topic, count := proto.UnpackMsgCount(msg[1:])
			c.subs[string(topic)].unread = count
		}
	}
}

func (c *Connection) ping() {
	for {
		select {
		case <-time.NewTicker(10 * time.Second).C:
			msg := proto.PackPing()
			c.conn.SetWriteDeadline(time.Now().Add(MAX_WRITE_WAIT_TIME))
			c.conn.Write(msg)
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
				msg := proto.PackAck(ackcache, proto.MSG_PUBACK)
				c.conn.SetWriteDeadline(time.Now().Add(MAX_WRITE_WAIT_TIME))
				c.conn.Write(msg)
				ackcache = ackcache[:0]
			}
		case <-time.NewTicker(1 * time.Second).C:
			if len(ackcache) > 0 {
				msg := proto.PackAck(ackcache, proto.MSG_PUBACK)
				c.conn.SetWriteDeadline(time.Now().Add(MAX_WRITE_WAIT_TIME))
				c.conn.Write(msg)
				ackcache = ackcache[:0]
			}
		case <-c.close:
			if len(ackcache) > 0 {
				msg := proto.PackAck(ackcache, proto.MSG_PUBACK)
				c.conn.SetWriteDeadline(time.Now().Add(MAX_WRITE_WAIT_TIME))
				c.conn.Write(msg)
				ackcache = ackcache[:0]
				return
			}
		}
	}
}
