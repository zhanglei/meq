package service

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jadechat/meq/proto"
	"github.com/stretchr/testify/assert"
)

var testNamespace = "test"

func TestFdbPutAndGet(t *testing.T) {
	// init broker
	b := NewBroker("../broker.yaml")
	b.conf.Store.Engine = "fdb"
	b.conf.Store.Namespace = testNamespace
	b.Start()
	defer b.Shutdown()
	// put into fdb
	b.store.Put(mockExactMsgs)
	// get from fdb
	msgs := b.store.Get(mockExactMsgs[0].Topic, 20, mockExactMsgs[0].ID)

	assert.Equal(t, mockExactMsgs[1:], msgs)

	f := b.store.(*FdbStore)
	f.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.ClearRange(f.sp)
		return
	})

}

func TestFdbPutAndGetFromNewest(t *testing.T) {
	// init broker
	b := NewBroker("../broker.yaml")
	b.conf.Store.Engine = "fdb"
	b.conf.Store.Namespace = testNamespace
	b.Start()
	defer b.Shutdown()
	// put into fdb
	b.store.Put(mockExactMsgs)
	// get from fdb
	msgs := b.store.Get(mockExactMsgs[0].Topic, 20, proto.MSG_NEWEST_OFFSET)

	assert.Equal(t, mockExactMsgs, msgs)

	f := b.store.(*FdbStore)
	f.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.ClearRange(f.sp)
		return
	})

}
