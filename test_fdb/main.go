package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func main() {
	fdb.MustAPIVersion(510)

	st := time.Now()
	v := make([]byte, 256)
	wg := sync.WaitGroup{}
	wg.Add(20)

	for k := 0; k < 20; k++ {
		go func(k int) {
			defer wg.Done()

			db := fdb.MustOpenDefault()

			nmqDir, err := directory.CreateOrOpen(db, []string{"nmqtest"}, nil)
			if err != nil {
				log.Fatal(err)
			}

			testApp := nmqDir.Sub("testapp")
			testTopic := "testtopic"
			testa := "testa"

			for j := 0; j < 1000; j++ {
				// 存储消息
				_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
					for i := 1; i < 100; i++ {
						SCKey := testApp.Pack(tuple.Tuple{testTopic, testa, fmt.Sprintf("%03d", i), fmt.Sprintf("%04d", j), fmt.Sprintf("%04d", k)})
						tr.Set(SCKey, v)
					}
					return
				})
			}

		}(k)
	}

	wg.Wait()

	fmt.Println("插入耗时：", time.Now().Sub(st).Nanoseconds()/1e6)
	// 查询消息
	// var keys []string
	// _, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
	// 	pr, _ := fdb.PrefixRange([]byte(testTopic))
	// 	pr.Begin = testApp.Pack(tuple.Tuple{testTopic, testa, "0"})
	// 	pr.End = testApp.Pack(tuple.Tuple{testTopic, testa, "ff"})
	// 	ir := tr.GetRange(pr, fdb.RangeOptions{Limit: 111, Reverse: true}).Iterator()
	// 	for ir.Advance() {
	// 		k := ir.MustGet().Key
	// 		keys = append(keys, string(k))
	// 	}
	// 	return
	// })

	// fmt.Println(keys)

	db := fdb.MustOpenDefault()

	nmqDir, err := directory.CreateOrOpen(db, []string{"nmqtest"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	testApp := nmqDir.Sub("testapp")
	testTopic := "testtopic"

	// 删除消息
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.ClearRange(testApp.Sub(testTopic))
		return
	})
}
