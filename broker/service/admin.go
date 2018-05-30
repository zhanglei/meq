package service

import (
	"fmt"
	"net/http"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/labstack/echo"
)

type admin struct {
	bk *Broker
}

func (ad *admin) Init(bk *Broker) {
	ad.bk = bk

	e := echo.New()
	e.POST("/clear/store", ad.clearStore)
	e.Logger.Fatal(e.Start(ad.bk.conf.Admin.Addr))
}

func (ad *admin) clearStore(c echo.Context) error {
	token := c.FormValue("token")
	if token != ad.bk.conf.Admin.Token {
		return c.String(http.StatusOK, "invalid admin token")
	}

	f, ok := ad.bk.store.(*FdbStore)
	if !ok {
		return c.String(http.StatusOK, "not fdb store engine,ignore")
	}

	_, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		tr.ClearRange(f.sp)
		return
	})
	if err != nil {
		return c.String(http.StatusOK, fmt.Sprintf("error happens: %v", err))
	}

	return c.String(http.StatusOK, "clear store ok")
}
