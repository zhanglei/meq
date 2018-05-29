package meq

import (
	"time"

	"github.com/chaingod/talent"
)

// 18byte
func GenMessageID() []byte {
	return talent.Int64ToBytes(time.Now().UnixNano())
}
