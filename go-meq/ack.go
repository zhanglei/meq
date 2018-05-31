package meq

import (
	"fmt"

	"github.com/jadechat/meq/proto"
	"github.com/jadechat/meq/proto/mqtt"
)

func (c *Connection) AckCount(topic []byte, count int) error {
	if count <= 0 && count != proto.ACK_ALL_COUNT {
		return fmt.Errorf("ack count cant below 0,except count == proto.ACK_ALL_COUNT")
	}

	if count > proto.MAX_PULL_COUNT {
		return fmt.Errorf("ack count cant greater than proto.MAX_PULL_COUNT")
	}

	msg := mqtt.Publish{
		Header: &mqtt.StaticHeader{
			QOS: 0,
		},
		Topic:   topic,
		Payload: proto.PackAckCount(count),
	}
	msg.EncodeTo(c.conn)

	return nil
}
