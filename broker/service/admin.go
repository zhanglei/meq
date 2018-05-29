package service

import (
	"github.com/labstack/echo"
)

type admin struct {
	bk *Broker
}

func (ad *admin) Init(bk *Broker) {
	ad.bk = bk

	e := echo.New()
	e.Logger.Fatal(e.Start(ad.bk.conf.Admin.Addr))
}
