package meq

import (
	"fmt"
	"time"

	"github.com/jadechat/meq/proto"
)

func (c *Connection) AckCount(topic []byte, count int) error {
	if count <= 0 && count != proto.ACK_ALL_COUNT {
		return fmt.Errorf("ack count cant below 0,except count == proto.ACK_ALL_COUNT")
	}

	if count > proto.MAX_PULL_COUNT {
		return fmt.Errorf("ack count cant greater than proto.MAX_PULL_COUNT")
	}

	msg := proto.PackAckCount(topic, count)
	c.conn.SetWriteDeadline(time.Now().Add(MAX_WRITE_WAIT_TIME))
	c.conn.Write(msg)

	return nil
}
