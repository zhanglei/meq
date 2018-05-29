package meq

import "github.com/jadechat/meq/proto"

func (c *Connection) Publish(msgs []*proto.PubMsg) error {
	b := proto.PackPubMsgs(msgs, proto.MSG_PUB)
	_, err := c.conn.Write(b)
	return err
}

func (c *Connection) Broadcast(msgs []*proto.PubMsg) error {
	b := proto.PackPubMsgs(msgs, proto.MSG_BROADCAST)
	_, err := c.conn.Write(b)
	return err
}
