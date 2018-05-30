package service

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/chaingod/talent"
	"go.uber.org/zap"
)

type Broker struct {
	wg          *sync.WaitGroup
	running     bool
	runningTime time.Time
	listener    net.Listener

	clients map[uint64]*client

	store   Storage
	router  *Router
	timer   *Timer
	cluster *cluster

	subtrie   *SubTrie
	subSynced bool

	conf *Config
	sync.RWMutex
}

func NewBroker(path string) *Broker {
	b := &Broker{
		wg:      &sync.WaitGroup{},
		clients: make(map[uint64]*client),
		subtrie: NewSubTrie(),
	}
	// init base config
	b.conf = initConfig(path)
	InitLogger(b.conf.Common.LogPath, b.conf.Common.LogLevel, b.conf.Common.IsDebug)
	L.Info("base configuration loaded")

	return b
}

func (b *Broker) Start() {

	addr := net.JoinHostPort(b.conf.Broker.Host, b.conf.Broker.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		L.Fatal("Fatal error when listening", zap.Error(err), zap.String("addr", addr))
	}
	b.listener = l

	go b.Accept()

	b.running = true
	b.runningTime = time.Now()

	// init store
	switch b.conf.Store.Engine {
	case "memory":
		b.store = &MemStore{
			bk: b,
		}
	case "fdb":
		b.store = &FdbStore{
			bk: b,
		}
	}

	b.store.Init()

	// init cluster
	b.cluster = &cluster{
		bk: b,
	}
	go b.cluster.Init()

	// init Router
	b.router = &Router{
		bk: b,
	}
	b.router.Init()

	// init timer
	b.timer = &Timer{
		bk: b,
	}
	b.timer.Init()

	// init admin
	ad := &admin{}
	go ad.Init(b)

	go func() {
		log.Println(http.ListenAndServe("localhost:6070", nil))
	}()
}
func (b *Broker) Shutdown() {
	b.running = false
	b.listener.Close()

	for _, c := range b.clients {
		c.conn.Close()
	}
	b.cluster.Close()
	b.store.Close()
	b.router.Close()
	b.timer.Close()

	L.Sync()
	b.wg.Wait()
}

func (b *Broker) Accept() {
	tmpDelay := ACCEPT_MIN_SLEEP
	var id uint64
	for b.running {
		conn, err := b.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				L.Error("Temporary Client Accept Error ", zap.Error(err))
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else if b.running {
				L.Error("Client Accept Error", zap.Error(err))
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		id++
		go b.process(conn, id)
	}
}

func (b *Broker) process(conn net.Conn, id uint64) {
	defer func() {
		b.Lock()
		delete(b.clients, id)
		b.Unlock()
		conn.Close()
		L.Info("client closed", zap.Uint64("conn_id", id))
	}()

	b.wg.Add(1)
	defer b.wg.Done()

	L.Info("new client", zap.Uint64("conn_id", id), zap.String("ip", conn.RemoteAddr().String()))

	cli := initClient(id, conn, b)

	b.Lock()
	b.clients[id] = cli
	b.Unlock()

	err := cli.waitForConnect()
	if err != nil {
		fmt.Println("cant receive connect packet from client", err, zap.Uint64("cid", id))
		return
	}

	go cli.sendLoop()
	err = cli.readLoop()
	if err != nil {
		if !talent.IsEOF(err) {
			L.Info("client read loop error", zap.Error(err), zap.Uint64("cid", cli.cid))
		}
	}
}
