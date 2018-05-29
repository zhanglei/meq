package proto

import (
	"encoding/binary"

	"github.com/golang/snappy"
)

func PackMsg(m PubMsg, cmd byte) []byte {
	msgid := m.ID
	payload := m.Payload
	// header
	msg := make([]byte, 1+4+2+len(msgid)+4+len(payload)+1+2+len(m.Topic)+1+1)
	binary.PutUvarint(msg[:4], uint64(1+2+len(msgid)+4+len(payload)+1+2+len(m.Topic)+1+1))
	msg[4] = cmd
	// msgid
	binary.PutUvarint(msg[5:7], uint64(len(msgid)))
	copy(msg[7:7+len(msgid)], msgid)

	// payload
	binary.PutUvarint(msg[7+len(msgid):11+len(msgid)], uint64(len(payload)))
	copy(msg[11+len(msgid):11+len(msgid)+len(payload)], payload)

	// acked
	if m.Acked {
		msg[11+len(msgid)+len(payload)] = '1'
	} else {
		msg[11+len(msgid)+len(payload)] = '0'
	}

	// topic
	binary.PutUvarint(msg[11+len(msgid)+len(payload)+1:11+len(msgid)+len(payload)+3], uint64(len(m.Topic)))
	copy(msg[11+len(msgid)+len(payload)+3:11+len(msgid)+len(payload)+3+len(m.Topic)], m.Topic)

	// type
	binary.PutUvarint(msg[11+len(msgid)+len(payload)+3+len(m.Topic):11+len(msgid)+len(payload)+3+len(m.Topic)+1], uint64(m.Type))

	// qos
	binary.PutUvarint(msg[11+len(msgid)+len(payload)+3+len(m.Topic)+1:11+len(msgid)+len(payload)+3+len(m.Topic)+2], uint64(m.QoS))

	return msg
}

func UnpackMsg(b []byte) (PubMsg, error) {
	// msgid
	ml, _ := binary.Uvarint(b[:2])
	msgid := b[2 : 2+ml]

	// payload
	pl, _ := binary.Uvarint(b[2+ml : 6+ml])
	payload := b[6+ml : 6+ml+pl]

	//acked
	var acked bool
	if b[6+ml+pl] == '1' {
		acked = true
	}

	// topic
	tl, _ := binary.Uvarint(b[6+ml+pl+1 : 6+ml+pl+3])
	topic := b[6+ml+pl+3 : 6+ml+pl+3+tl]

	// type
	tp, _ := binary.Uvarint(b[6+ml+pl+3+tl : 7+ml+pl+3+tl])

	// qos
	qos, _ := binary.Uvarint(b[7+ml+pl+3+tl : 8+ml+pl+3+tl])
	return PubMsg{msgid, topic, payload, acked, int8(tp), int8(qos)}, nil
}

func PackSub(topic []byte, group []byte) []byte {
	if group == nil {
		group = DEFAULT_GROUP
	}

	tl := uint64(len(topic))
	gl := uint64(len(group))

	msg := make([]byte, 4+1+2+tl+gl)
	// 设置header
	binary.PutUvarint(msg[:4], 1+2+tl+gl)
	// 设置control flag
	msg[4] = MSG_SUB

	// topic len
	binary.PutUvarint(msg[5:7], tl)
	// topic
	copy(msg[7:7+tl], topic)
	// group
	copy(msg[7+tl:], group)

	return msg
}

func UnpackSub(b []byte) ([]byte, []byte) {
	// read topic length
	var tl uint64
	if tl, _ = binary.Uvarint(b[:2]); tl <= 0 {
		return nil, nil
	}

	return b[2 : 2+tl], b[2+tl:]
}

func PackSubAck(t []byte) []byte {
	tl := uint64(len(t))
	msg := make([]byte, 4+1+tl)
	binary.PutUvarint(msg[:4], 1+tl)
	msg[4] = MSG_SUBACK

	copy(msg[5:], t)

	return msg
}

func UnpackSubAck(b []byte) []byte {
	return b
}

func PackAck(acks []Ack, cmd byte) []byte {
	body := PackAckBody(acks, cmd)
	msg := make([]byte, len(body)+4)
	binary.PutUvarint(msg[:4], uint64(len(body)))
	copy(msg[4:], body)
	return msg
}

func PackAckBody(acks []Ack, cmd byte) []byte {
	total := 1 + 4 + 4*len(acks)
	for _, ack := range acks {
		total += (len(ack.Msgid) + len(ack.Topic))
	}

	body := make([]byte, total)
	// command
	body[0] = cmd
	// msgs count
	binary.PutUvarint(body[1:5], uint64(len(acks)))

	last := 5
	for _, ack := range acks {
		ml := len(ack.Msgid)
		tl := len(ack.Topic)
		binary.PutUvarint(body[last:last+2], uint64(ml))
		copy(body[last+2:last+2+ml], ack.Msgid)
		binary.PutUvarint(body[last+2+ml:last+4+ml], uint64(tl))
		copy(body[last+4+ml:last+4+ml+tl], ack.Topic)
		last = last + 4 + ml + tl
	}

	return body
}

func UnpackAck(b []byte) []Ack {
	msl, _ := binary.Uvarint(b[:4])
	acks := make([]Ack, msl)

	var last uint64 = 4
	index := 0
	bl := uint64(len(b))
	for {
		if last >= bl {
			break
		}
		ack := Ack{}

		ml, _ := binary.Uvarint(b[last : last+2])
		ack.Msgid = b[last+2 : last+2+ml]

		tl, _ := binary.Uvarint(b[last+2+ml : last+4+ml])
		ack.Topic = b[last+4+ml : last+4+ml+tl]

		acks[index] = ack

		index++
		last = last + 4 + ml + tl
	}

	return acks
}

func PackPing() []byte {
	msg := make([]byte, 5)
	binary.PutUvarint(msg[:4], 1)
	msg[4] = MSG_PING

	return msg
}

func PackPong() []byte {
	msg := make([]byte, 5)
	binary.PutUvarint(msg[:4], 1)
	msg[4] = MSG_PONG
	return msg
}

func PackConnect() []byte {
	msg := make([]byte, 5)
	binary.PutUvarint(msg[:4], 1)
	msg[4] = MSG_CONNECT
	return msg
}

func PackConnectOK() []byte {
	msg := make([]byte, 5)
	binary.PutUvarint(msg[:4], 1)
	msg[4] = MSG_CONNECT_OK

	return msg
}

func PackMsgCount(topic []byte, count int) []byte {
	msg := make([]byte, 4+1+2+len(topic)+4)
	binary.PutUvarint(msg[:4], uint64(1+2+len(topic)+4))
	msg[4] = MSG_COUNT
	binary.PutUvarint(msg[5:7], uint64(len(topic)))
	copy(msg[7:7+len(topic)], topic)
	binary.PutUvarint(msg[7+len(topic):11+len(topic)], uint64(count))
	return msg
}

func UnpackMsgCount(b []byte) ([]byte, int) {
	tl, _ := binary.Uvarint(b[:2])
	topic := b[2 : 2+tl]

	count, _ := binary.Uvarint(b[2+tl : 6+tl])
	return topic, int(count)
}

func PackPullMsg(topic []byte, count int, msgid []byte) []byte {
	tl := uint64(len(topic))
	msg := make([]byte, 4+1+2+len(topic)+1+len(msgid))
	binary.PutUvarint(msg[:4], uint64(1+2+len(topic)+1+len(msgid)))
	msg[4] = MSG_PULL
	binary.PutUvarint(msg[5:7], tl)
	copy(msg[7:7+tl], topic)
	binary.PutUvarint(msg[7+tl:8+tl], uint64(count))
	copy(msg[8+tl:8+int(tl)+len(msgid)], msgid)
	return msg
}

func UnPackPullMsg(b []byte) ([]byte, int, []byte) {
	var tl uint64
	if tl, _ = binary.Uvarint(b[0:2]); tl <= 0 {
		return nil, 0, nil
	}

	count, _ := binary.Uvarint(b[2+tl : 3+tl])
	return b[2 : 2+tl], int(count), b[3+tl:]
}

func PackTimerMsg(m *TimerMsg, cmd byte) []byte {
	ml := uint64(len(m.ID))
	tl := uint64(len(m.Topic))
	pl := uint64(len(m.Payload))
	msg := make([]byte, 4+1+2+ml+2+tl+4+pl+8+4)

	//header
	binary.PutUvarint(msg[:4], 1+2+ml+2+tl+4+pl+8+4)
	//command
	msg[4] = cmd
	//msgid
	binary.PutUvarint(msg[5:7], ml)
	copy(msg[7:7+ml], m.ID)
	//topic
	binary.PutUvarint(msg[7+ml:9+ml], tl)
	copy(msg[9+ml:9+ml+tl], m.Topic)
	//payload
	binary.PutUvarint(msg[9+ml+tl:13+ml+tl], pl)
	copy(msg[13+ml+tl:13+ml+tl+pl], m.Payload)
	//trigger time
	binary.PutVarint(msg[13+ml+tl+pl:21+ml+tl+pl], m.Trigger)
	//delay
	binary.PutUvarint(msg[21+ml+tl+pl:25+ml+tl+pl], uint64(m.Delay))
	return msg
}

func UnpackTimerMsg(b []byte) *TimerMsg {
	//msgid 2
	ml, _ := binary.Uvarint(b[:2])
	msgid := b[2 : 2+ml]
	//topic 2
	tl, _ := binary.Uvarint(b[2+ml : 4+ml])
	topic := b[4+ml : 4+ml+tl]
	//payload 4
	pl, _ := binary.Uvarint(b[4+ml+tl : 8+ml+tl])
	payload := b[8+ml+tl : 8+ml+tl+pl]
	//trigger time 8
	st, _ := binary.Varint(b[8+ml+tl+pl : 16+ml+tl+pl])
	//delay 4
	delay, _ := binary.Uvarint(b[16+ml+tl+pl : 20+ml+tl+pl])
	return &TimerMsg{msgid, topic, payload, st, int(delay)}
}

func PackPubMsgs(ms []*PubMsg, cmd byte) []byte {
	bl := 11 * len(ms)
	for _, m := range ms {
		bl += (len(m.ID) + len(m.Topic) + len(m.Payload))
	}
	body := make([]byte, bl)

	last := 0
	for _, m := range ms {
		ml, tl, pl := len(m.ID), len(m.Topic), len(m.Payload)
		//msgid
		binary.PutUvarint(body[last:last+2], uint64(ml))
		copy(body[last+2:last+2+ml], m.ID)
		//topic
		binary.PutUvarint(body[last+2+ml:last+4+ml], uint64(tl))
		copy(body[last+4+ml:last+4+ml+tl], m.Topic)
		//payload
		binary.PutUvarint(body[last+4+ml+tl:last+8+ml+tl], uint64(pl))
		copy(body[last+8+ml+tl:last+8+ml+tl+pl], m.Payload)
		//Acked
		if m.Acked {
			body[last+8+ml+tl+pl] = '1'
		} else {
			body[last+8+ml+tl+pl] = '0'
		}
		//type
		binary.PutUvarint(body[last+9+ml+tl+pl:last+10+ml+tl+pl], uint64(m.Type))
		//qos
		binary.PutUvarint(body[last+10+ml+tl+pl:last+11+ml+tl+pl], uint64(m.QoS))
		last = last + 11 + ml + tl + pl
	}

	// 压缩body
	cbody := snappy.Encode(nil, body)

	//header
	msg := make([]byte, len(cbody)+9)
	binary.PutUvarint(msg[:4], uint64(len(cbody)+5))
	msg[4] = cmd
	binary.PutUvarint(msg[5:9], uint64(len(ms)))

	copy(msg[9:], cbody)
	return msg
}

func UnpackPubMsgs(m []byte) ([]*PubMsg, error) {
	msl, _ := binary.Uvarint(m[:4])
	msgs := make([]*PubMsg, msl)
	// decompress
	b, err := snappy.Decode(nil, m[4:])
	if err != nil {
		return nil, err
	}
	var last uint64
	bl := uint64(len(b))
	index := 0
	for {
		if last >= bl {
			break
		}
		//msgid
		ml, _ := binary.Uvarint(b[last : last+2])
		msgid := b[last+2 : last+2+ml]
		//topic
		tl, _ := binary.Uvarint(b[last+2+ml : last+4+ml])
		topic := b[last+4+ml : last+4+ml+tl]
		//payload
		pl, _ := binary.Uvarint(b[last+4+ml+tl : last+8+ml+tl])
		payload := b[last+8+ml+tl : last+8+ml+tl+pl]
		//acked
		var acked bool
		if b[last+8+ml+tl+pl] == '1' {
			acked = true
		}
		//type
		tp, _ := binary.Uvarint(b[last+9+ml+tl+pl : last+10+ml+tl+pl])
		// qos
		qos, _ := binary.Uvarint(b[last+10+ml+tl+pl : last+11+ml+tl+pl])
		msgs[index] = &PubMsg{msgid, topic, payload, acked, int8(tp), int8(qos)}

		index++
		last = last + 11 + ml + tl + pl
	}

	return msgs, nil
}
