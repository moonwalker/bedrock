package streams

import (
	"sync"

	"github.com/nats-io/nats.go"
)

const (
	CONN_POOL_SIZE = 100
)

type NatsConnPool struct {
	url     string
	mutex   *sync.RWMutex
	pool    chan *nats.Conn
	options []nats.Option
}

func NewNatsConnPool(url string, options ...nats.Option) *NatsConnPool {
	return &NatsConnPool{
		url:     url,
		mutex:   new(sync.RWMutex),
		pool:    make(chan *nats.Conn, CONN_POOL_SIZE),
		options: options,
	}
}

func (this *NatsConnPool) GetConnection() (*nats.Conn, error) {
	connect := func() (*nats.Conn, error) {
		return nats.Connect(this.url, this.options...)
	}

	this.mutex.RLock()
	defer this.mutex.RUnlock()

	var nc *nats.Conn
	var err error
	select {
	case nc = <-this.pool:
		// reuse exists pool
		if nc.IsConnected() != true {
			// close to be sure
			nc.Close()
			// disconnected conn, create new *nats.Conn
			nc, err = connect()
		}
	default:
		// create *nats.Conn
		nc, err = connect()
	}

	return nc, err
}

func (this *NatsConnPool) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	close(this.pool)
	for nc := range this.pool {
		err := nc.Drain()
		if err != nil {
			return err
		}
	}

	this.pool = make(chan *nats.Conn, CONN_POOL_SIZE)

	return nil
}
