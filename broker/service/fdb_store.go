package service

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jadechat/meq/proto"
	"go.uber.org/zap"
)

type FdbStore struct {
	bk *Broker
	db fdb.Database
	sp subspace.Subspace
}

func (f *FdbStore) Init() {
	fdb.MustAPIVersion(510)
	f.db = fdb.MustOpenDefault()
	sp, err := directory.CreateOrOpen(f.db, []string{f.bk.conf.Store.Namespace}, nil)
	if err != nil {
		L.Fatal("init fdb(foundationDB) error", zap.Error(err))
	}
	f.sp = sp
}

func (f *FdbStore) Close() {

}

func (f *FdbStore) Put(msgs []*proto.PubMsg) {
	_, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		for _, msg := range msgs {
			key := f.sp.Pack(tuple.Tuple{msg.Topic, msg.ID})
			tr.Set(key, PackStoreMessage(msg))
		}
		return
	})
	if err != nil {
		L.Info("put messsage error", zap.Error(err))
	}
}

func (f *FdbStore) ACK([]proto.Ack) {

}

var (
	fdbStoreBegin = []byte("0")
	fdbStoreEnd   = []byte("ff")
)

func (f *FdbStore) Get(t []byte, count int, offset []byte) []*proto.PubMsg {
	var msgs []*proto.PubMsg
	_, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		pr, _ := fdb.PrefixRange(t)
		pr.Begin = f.sp.Pack(tuple.Tuple{t, fdbStoreBegin})
		if bytes.Compare(offset, proto.MSG_NEWEST_OFFSET) == 0 {
			pr.End = f.sp.Pack(tuple.Tuple{t, fdbStoreEnd})
		} else {
			pr.End = f.sp.Pack(tuple.Tuple{t, offset})
		}

		ir := tr.GetRange(pr, fdb.RangeOptions{Limit: count, Reverse: true}).Iterator()
		for ir.Advance() {
			b := ir.MustGet().Value
			m := UnpackStoreMessage(b)
			msgs = append(msgs, m)
		}
		return
	})

	if err != nil {
		L.Info("get messsage error", zap.Error(err))
	}

	return msgs
}

func (f *FdbStore) GetCount([]byte) int {
	return 0
}

func (f *FdbStore) PutTimerMsg(*proto.TimerMsg) {

}

func (f *FdbStore) GetTimerMsg() []*proto.PubMsg {
	return nil
}
