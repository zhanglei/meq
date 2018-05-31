package service

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jadechat/meq/proto"
	"go.uber.org/zap"
)

type FdbStore struct {
	bk     *Broker
	dbs    []*database
	pubchs [](chan []*proto.PubMsg)
	ackchs [](chan []proto.Ack)

	sync.RWMutex
}

type database struct {
	db fdb.Database
	sp subspace.Subspace
}

const (
	FdbCacheInitLen = 1000
)

func (f *FdbStore) Init() {
	fdb.MustAPIVersion(510)

	f.dbs = make([]*database, f.bk.conf.Store.FDB.Threads)
	f.pubchs = make([](chan []*proto.PubMsg), f.bk.conf.Store.FDB.Threads)
	f.ackchs = make([](chan []proto.Ack), f.bk.conf.Store.FDB.Threads)

	for i := 0; i < f.bk.conf.Store.FDB.Threads; i++ {
		go f.process(i)
	}
}

func (f *FdbStore) Close() {

}

func (f *FdbStore) process(i int) {
	fdb.MustAPIVersion(510)
	db := fdb.MustOpenDefault()
	sp, err := directory.CreateOrOpen(db, []string{f.bk.conf.Store.FDB.Namespace}, nil)
	if err != nil {
		L.Fatal("init fdb(foundationDB) error", zap.Error(err))
	}
	f.dbs[i] = &database{db, sp}

	pubch := make(chan []*proto.PubMsg, FdbCacheInitLen)
	ackch := make(chan []proto.Ack, FdbCacheInitLen)
	f.pubchs[i] = pubch
	f.ackchs[i] = ackch

	msgcache := make([]*proto.PubMsg, 0, FdbCacheInitLen)
	ackcache := make([]proto.Ack, 0, FdbCacheInitLen)

	for f.bk.running || len(pubch) > 0 {
		select {
		case msgs := <-pubch:
			msgcache = append(msgcache, msgs...)
			if len(msgcache) >= proto.CacheFlushLen {
				put(db, sp, msgcache)
				msgcache = msgcache[:0]
			}
		case acks := <-ackch:
			ackcache = append(ackcache, acks...)
			if len(ackcache) >= proto.CacheFlushLen {
				ack(db, sp, ackcache)
				ackcache = ackcache[:0]
			}
		case <-time.NewTicker(1 * time.Second).C:
			if len(msgcache) > 0 {
				put(db, sp, msgcache)
				msgcache = msgcache[:0]
			}

			if len(ackcache) > 0 {
				ack(db, sp, ackcache)
				ackcache = ackcache[:0]
			}

		}
	}
}

var putcounts uint64 = 0

// deliver to the fdb processor with robin strategy
func (f *FdbStore) Put(msgs []*proto.PubMsg) {
	// for lock free solution
	i := putcounts % uint64(f.bk.conf.Store.FDB.Threads)
	f.pubchs[i] <- msgs

	putcounts++
}

func put(db fdb.Database, sp subspace.Subspace, msgs []*proto.PubMsg) {
	_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		for _, msg := range msgs {
			key := sp.Pack(tuple.Tuple{msg.Topic, msg.ID})
			tr.Set(key, PackStoreMessage(msg))
		}
		return
	})
	if err != nil {
		L.Info("put messsage error", zap.Error(err))
	}
}

var ackcounts uint64 = 0

func (f *FdbStore) ACK(acks []proto.Ack) {
	// for lock free solution
	i := putcounts % uint64(f.bk.conf.Store.FDB.Threads)
	f.ackchs[i] <- acks

	putcounts++
}

func ack(db fdb.Database, sp subspace.Subspace, acks []proto.Ack) {
	_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		for _, ack := range acks {
			key := sp.Pack(tuple.Tuple{ack.Topic, ack.Msgid})
			b := tr.Get(key).MustGet()
			// update msg acked to true(1)
			// msgid
			ml, _ := binary.Uvarint(b[:2])
			// payload
			pl, _ := binary.Uvarint(b[2+ml : 6+ml])
			// update ack
			b[6+ml+pl] = '1'

			tr.Set(key, b)
		}
		return
	})
	if err != nil {
		L.Info("put messsage error", zap.Error(err))
	}
}

var (
	fdbStoreBegin = []byte("0")
	fdbStoreEnd   = []byte("ff")
)

var getcounts uint64 = 0

func (f *FdbStore) Get(t []byte, count int, offset []byte, acked bool) []*proto.PubMsg {
	i := getcounts % uint64(f.bk.conf.Store.FDB.Threads)
	getcounts++

	d := f.dbs[i]
	var msgs []*proto.PubMsg
	_, err := d.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		pr, _ := fdb.PrefixRange(t)
		pr.Begin = d.sp.Pack(tuple.Tuple{t, fdbStoreBegin})
		if bytes.Compare(offset, proto.MSG_NEWEST_OFFSET) == 0 {
			pr.End = d.sp.Pack(tuple.Tuple{t, fdbStoreEnd})
		} else {
			pr.End = d.sp.Pack(tuple.Tuple{t, offset})
		}

		//@performance
		//get one by one in advance
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
