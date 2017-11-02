package storage

// Storage storage
type Storage interface {
	Init(param map[string]interface{}) error // init storage server
	Subscription(ssid int32, ttype int, usid int32) error
	UnSubscription(ssid int32, ttype int, usid int32) error
	Store(ssid int32, ttype int, usid int32, payload []byte, msgid int64, ttl int64) error
	Get(ssid int32, ttype int, usid int32, limit int) ([][]byte, error)
	Ack(ssid int32, ttype int, usid int32, msgid int64) error
	Del(ssid int32, ttype int, usid int32, msgid int64) error
	Len(ssid int32, ttype int, usid int32) (int, error)
	Name() string
}
