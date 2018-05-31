package meq

import (
	"errors"
	"fmt"
	"time"

	"github.com/jadechat/meq/proto"
)

func (c *Connection) UnreadCount(topic []byte) int {
	return c.subs[string(topic)].unread
}

func (c *Connection) PullMsgs(topic []byte, count int, offset []byte) error {
	// check the topic is push or pull prop
	// only the pull prop can continue
	sub, ok := c.subs[string(topic)]
	if !ok {
		// subscribe to the topic failed
		return errors.New("subscribe topic failed,please re-subscribe")
	}

	if !sub.acked {
		return errors.New("subscribe topic failed,please re-subscribe")
	}

	if offset == nil {
		offset = proto.MSG_NEWEST_OFFSET
	}

	if count <= 0 {
		return errors.New("messages count cant below 0")
	}

	if count > proto.MAX_PULL_COUNT {
		return fmt.Errorf("messages count cant larger than %d", proto.MAX_PULL_COUNT)
	}

	// 拉取最新消息
	msg := proto.PackPullMsg(topic, count, offset)
	c.conn.SetWriteDeadline(time.Now().Add(MAX_WRITE_WAIT_TIME))
	c.conn.Write(msg)

	return nil
}
