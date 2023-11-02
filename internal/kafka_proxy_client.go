package internal

import (
	"encoding/json"
	"fmt"
	"github.com/dan-and-dna/kafka-proxy-client/client"
	"sync"
)

var (
	proxyClient *KafkaProxyClient
	once        sync.Once
)

type KafkaProxyClient struct {
	conn  *client.Client
	ready bool
	app   string
	sync.RWMutex
}

func GetSingleInst() *KafkaProxyClient {
	if proxyClient == nil {
		once.Do(func() {
			proxyClient = new(KafkaProxyClient)
		})
	}

	return proxyClient
}

func (c *KafkaProxyClient) Init(app, address string) error {
	return c.Recreate(app, address)
}

func (c *KafkaProxyClient) Recreate(app, address string) error {
	if c == nil {
		return nil
	}

	c.Lock()
	defer c.Unlock()
	if c.ready {
		c.conn.Close()
	}

	c.ready = false
	conn := client.New()

	err := conn.Dial(address)
	if err != nil {
		return err
	}

	c.conn = conn
	c.ready = true
	c.app = app

	return nil
}

func (c *KafkaProxyClient) Stop() {
	if c == nil {
		return
	}

	c.Lock()
	defer c.Unlock()
	if !c.ready {
		return
	}

	c.ready = false
	c.conn.Close()
}

func (c *KafkaProxyClient) Publish(topic string, msg interface{}) error {
	if c == nil {
		return nil
	}

	c.RLock()
	defer c.RUnlock()
	if !c.ready {
		return nil
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	message := client.ProxyMessage{
		Topic: fmt.Sprintf("%s_%s", c.app, topic),
		Msg:   string(buf),
	}

	return c.conn.Publish(message)
}
