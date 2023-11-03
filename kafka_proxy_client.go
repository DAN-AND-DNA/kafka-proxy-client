package kafka_proxy_client

import "github.com/dan-and-dna/kafka-proxy-client/internal"

/*
Init 初始化

app 平台名

address 地址
*/
func Init(address string) error {
	return internal.GetSingleInst().Init(address)
}

/*
Recreate 重建

app 平台名

address 地址
*/
func Recreate(address string) error {
	return internal.GetSingleInst().Recreate(address)
}

func Stop() {
	internal.GetSingleInst().Stop()
}

/*
Publish 发布消息给指定平台

app: 平台名

topic: kafka topic

msg 消息结构体

	type A struct {
		Name string `json:"name"`
	}
*/
func Publish(app, topic string, msg interface{}) error {
	return internal.GetSingleInst().Publish(app, topic, msg)
}

/*
PublishCross 发布（无平台）

topic: kafka topic

msg 消息结构体

	type A struct {
		Name string `json:"name"`
	}
*/
func PublishCross(topic string, msg interface{}) error {
	return internal.GetSingleInst().PublishCross(topic, msg)
}
