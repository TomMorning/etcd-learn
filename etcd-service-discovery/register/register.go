package main

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func NewServiceRegister(endpoints []string, key string, val string, lease int64) (*serviceRegister, error) {
	// 创建etcd client
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatal(err)
	}

	// 创建注册服务对象
	ser := &serviceRegister{
		cli: client,
		key: key,
		val: val,
	}

	// 设置租约并keepalive
	err = ser.setLeaseAndKeepAlive(lease)
	if err != nil {
		return nil, err
	}
	return ser, nil
}

type serviceRegister struct {
	cli           *clientv3.Client                        // etcd client
	leaseId       clientv3.LeaseID                        // 租约id
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse // 续租keepalive chan
	key           string
	val           string
}

// 监听续租状态
func (s *serviceRegister) ListenLeaseRespChan() {
	for response := range s.keepAliveChan {
		log.Println("续租成功", response)
	}
	log.Println("关闭续租")
}

func (s *serviceRegister) setLeaseAndKeepAlive(lease int64) error {
	// 设置租约时间
	resp, err := s.cli.Grant(context.Background(), lease)
	if err != nil {
		return err
	}

	// 注册服务并绑定租约
	_, err = s.cli.Put(context.Background(), s.key, s.val, clientv3.WithLease(resp.ID))
	if err != nil {
		return err
	}

	// 设置续租
	leaseRespChan, err := s.cli.KeepAlive(context.Background(), resp.ID)
	if err != nil {
		return err
	}

	s.leaseId = resp.ID
	s.keepAliveChan = leaseRespChan
	log.Printf("LeaseId: %s", s.leaseId)
	log.Printf("Put key: %s, val: %s, success!", s.key, s.val)
	return nil
}

func main() {
	var endpoints = []string{"localhost:2379"}
	// 新建注册服务
	ser, err := NewServiceRegister(endpoints, "/web/node1", "localhost:8000", 5)
	if err != nil {
		log.Fatalln(err)
	}
	// 监听续约chan
	go ser.ListenLeaseRespChan()

	select {}
}
