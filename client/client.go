package client

import (
	"encoding/json"
	"net"
	"sync"
)

type ProxyMessage struct {
	Topic string `json:"topic"`
	Key   string `json:"key,omitempty"`
	Msg   string `json:"msg"`
}

type Client struct {
	serverAddr string
	ready      bool
	conn       net.Conn
	sync.RWMutex
}

func New() *Client {
	client := &Client{}
	client.ready = false
	client.conn = nil
	return client
}

func (client *Client) Dial(address string) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return err
	}

	client.conn = conn
	client.ready = true

	return nil
}

func (client *Client) Close() {
	client.Lock()
	defer client.Unlock()
	if !client.ready {
		return
	}

	client.ready = false

	// 强刷一波
	_ = client.conn.Close()
	client.conn = nil
}

/*
Publish 发布消息

pMessage 消息
*/
func (client *Client) Publish(pMessage ProxyMessage) error {
	// 先序列化
	buf, err := json.Marshal(pMessage)
	if err != nil {
		return err
	}

	client.RLock()
	defer client.RUnlock()
	if !client.ready {
		return nil
	}

	_, err = client.conn.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
