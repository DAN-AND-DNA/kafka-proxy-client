package kafka_proxy_client

import "github.com/dan-and-dna/kafka-proxy-client/internal"

/*
Init 初始化

app 平台名

address 地址
*/
func Init(app, address string) error {
	return internal.GetSingleInst().Init(app, address)
}

/*
Recreate 重建

app 平台名

address 地址
*/
func Recreate(app, address string) error {
	return internal.GetSingleInst().Recreate(app, address)
}

func Stop() {
	internal.GetSingleInst().Stop()
}

/*
Publish 发布一个消息

msg 消息结构体

	type A struct {
		Name string `json:"name"`
	}
*/
func Publish(topic string, msg interface{}) error {
	return internal.GetSingleInst().Publish(topic, msg)
}
