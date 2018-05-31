package main

import (
	"fmt"

	meq "github.com/jadechat/meq/go-meq"
	"github.com/jadechat/meq/proto"
)

func sub(conn *meq.Connection) {
	err := conn.Subscribe([]byte(topic), []byte("robot1"), func(m *proto.PubMsg) {
		// if m.ID[len(m.ID)-1] == 48 && m.ID[len(m.ID)-2] == 48 && m.ID[len(m.ID)-3] == 48 && m.ID[len(m.ID)-4] == 48 {
		fmt.Println("收到消息：", string(m.ID), m.QoS, m.Acked)
		// }
	})

	if err != nil {
		panic(err)
	}

	// 先拉取10条消息
	err = conn.PullMsgs([]byte(topic), proto.MAX_PULL_COUNT, proto.MSG_NEWEST_OFFSET)
	if err != nil {
		panic(err)
	}

	unread := conn.UnreadCount([]byte(topic))
	fmt.Println("未读消息数量：", unread)

	conn.AckCount([]byte(topic), proto.MAX_PULL_COUNT)

	select {}

	// fmt.Println("累积消费未ACK消息数：", n1)
}
