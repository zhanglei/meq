package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jadechat/meq/proto"
)

func main() {
	fdb.MustAPIVersion(510)

	db := fdb.MustOpen(fdb.DefaultClusterFile, []byte("DB"))

	nmqDir, err := directory.CreateOrOpen(db, []string{"nmqtest"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	testApp := nmqDir.Sub("testapp")
	testTopic := "testtopic"

	// k := testApp.Pack(tuple.Tuple{testTopic})
	// incrKey(db, k, 100)
	// n, _ := getKey(db, k)
	// fmt.Println(n)
	// decrKey(db, k, 10)
	// n, _ = getKey(db, k)
	// fmt.Println(n)
	// st := time.Now()
	v := make([]byte, 256)
	// wg := sync.WaitGroup{}
	// wg.Add(100)

	// for k := 0; k < 100; k++ {
	// 	go func(k int) {
	// 		defer wg.Done()
	// 		db := fdb.MustOpen(fdb.DefaultClusterFile, []byte("DB"))

	// 		nmqDir, err := directory.CreateOrOpen(db, []string{"nmqtest"}, nil)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}

	// 		testApp := nmqDir.Sub("testapp")
	// 		testTopic := "testtopic"

	// 		for j := 0; j < 20; j++ {
	// 存储消息
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		for i := 1; i < 100; i++ {
			SCKey := testApp.Pack(tuple.Tuple{testTopic, strconv.Itoa(i), fmt.Sprintf("%03d", i)})
			tr.Set(SCKey, v)
		}
		return
	})
	// 		}

	// 	}(k)
	// }

	// wg.Wait()

	// fmt.Println("插入耗时：", time.Now().Sub(st).Nanoseconds()/1e6)
	// 查询消息
	var keys []string
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		pr, _ := fdb.PrefixRange([]byte(testTopic))
		pr.Begin = testApp.Pack(tuple.Tuple{testTopic, "1", "0"})
		pr.End = testApp.Pack(tuple.Tuple{testTopic, "3", "ff"})
		ir := tr.GetRange(pr, fdb.RangeOptions{Limit: 5, Reverse: true}).Iterator()
		for ir.Advance() {
			k := ir.MustGet().Key
			keys = append(keys, string(k))
		}
		return
	})

	fmt.Println(keys)

	// 删除消息
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.ClearRange(testApp.Sub(testTopic))
		return
	})
}

func incrKey(tor fdb.Transactor, k fdb.Key, n int) error {
	_, e := tor.Transact(func(tr fdb.Transaction) (interface{}, error) {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, int64(n))
		if err != nil {
			return nil, err
		}
		one := buf.Bytes()
		tr.Add(k, one)
		return nil, nil
	})
	return e
}

func decrKey(tor fdb.Transactor, k fdb.Key, delta int) error {
	_, e := tor.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if delta != proto.ACK_ALL_COUNT {
			on, err := getKey(tor, k)
			if err != nil {
				return nil, err
			}
			if on-int64(delta) <= 0 {
				goto SET_0
			}
			buf := new(bytes.Buffer)
			err = binary.Write(buf, binary.LittleEndian, int64(-delta))
			if err != nil {
				return nil, err
			}
			negativeOne := buf.Bytes()
			tr.Add(k, negativeOne)
			return nil, nil
		}

	SET_0:
		// ack all,set count to 0
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, int64(0))
		if err != nil {
			return nil, err
		}
		tr.Set(k, buf.Bytes())
		return nil, nil
	})

	return e
}

func getKey(tor fdb.Transactor, k fdb.Key) (int64, error) {
	val, e := tor.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.Get(k).Get()
	})
	if e != nil {
		return 0, e
	}
	if val == nil {
		return 0, nil
	}
	byteVal := val.([]byte)
	var numVal int64
	readE := binary.Read(bytes.NewReader(byteVal), binary.LittleEndian, &numVal)
	if readE != nil {
		return 0, readE
	} else {
		return numVal, nil
	}
}
