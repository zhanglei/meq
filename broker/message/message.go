package message

// Message  store data struct
type Message struct {
	TTL       int64  `msg:"ttl"` // ttl , message expire  time
	StoreTime int64  `msg:"t"`   // message store time
	Payload   []byte `msg:"v"`   // mqtt message body
}
