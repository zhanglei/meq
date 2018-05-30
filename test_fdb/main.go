package main

import (
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func main() {
	fdb.MustAPIVersion(510)
	db := fdb.MustOpenDefault()

	nmqDir, err := directory.CreateOrOpen(db, []string{"nmqtest"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	testApp := nmqDir.Sub("testapp")
	testTopic := "testtopic"
	testa := "testa"
	// 存储消息
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		for i := 1; i < 70; i++ {
			SCKey := testApp.Pack(tuple.Tuple{testTopic, testa, fmt.Sprintf("%02d", i), fmt.Sprintf("%02d", i-1)})
			tr.Set(SCKey, []byte("11111"))
		}
		return
	})

	// 查询消息
	var keys []string
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		pr, _ := fdb.PrefixRange([]byte(testTopic))
		pr.Begin = testApp.Pack(tuple.Tuple{testTopic, testa, "0"})
		pr.End = testApp.Pack(tuple.Tuple{testTopic, testa, "ff"})
		ir := tr.GetRange(pr, fdb.RangeOptions{Limit: 111, Reverse: true}).Iterator()
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
