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

func (c *KafkaProxyClient) Init(address string) error {
	return c.Recreate(address)
}

func (c *KafkaProxyClient) Recreate(address string) error {
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

/*
Publish 发布消息给指定平台

app: 平台名

topic: kafka topic
*/
func (c *KafkaProxyClient) Publish(app, topic string, msg interface{}) error {
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

	if app != "" {
		topic = fmt.Sprintf("%s_%s", app, topic)
	}

	message := client.ProxyMessage{
		Topic: topic,
		Msg:   string(buf),
	}

	return c.conn.Publish(message)
}

/*
PublishCross 发布（无平台）

topic: kafka topic
*/
func (c *KafkaProxyClient) PublishCross(topic string, msg interface{}) error {
	return c.Publish("", topic, msg)
}
