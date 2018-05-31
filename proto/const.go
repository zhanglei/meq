package proto

const (
	MSG_PUB           = 'a'
	MSG_PUBACK        = 'b'
	MSG_PUB_TIMER     = 'c'
	MSG_PUB_TIMER_ACK = 'd'
	MSG_PUB_RESTORE   = 'e'

	MSG_SUB    = 'f'
	MSG_SUBACK = 'g'
	MSG_UNSUB  = 'h'

	MSG_PING = 'i'
	MSG_PONG = 'j'

	MSG_COUNT = 'k'
	MSG_PULL  = 'l'

	MSG_CONNECT    = 'm'
	MSG_CONNECT_OK = 'n'

	MSG_BROADCAST = 'o'
)

const (
	NORMAL_MSG = 0
	TIMER_MSG  = 1
)

const (
	QOS0 = 0
	QOS1 = 1
)

var (
	DEFAULT_GROUP     = []byte("meq.io")
	MSG_NEWEST_OFFSET = []byte("0")

	MAX_PULL_COUNT = 100

	CacheFlushLen = 100
)
