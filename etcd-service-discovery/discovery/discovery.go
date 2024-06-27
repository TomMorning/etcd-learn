package main

import (
	"context"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync"
	"time"
)

// 服务发现结构体
type ServiceDiscovery struct {
	cli        *clientv3.Client  // etcd client
	serverList map[string]string // 服务列表
	lock       sync.Mutex
}

// NewServiceDiscovery 新建发现服务
func NewServiceDiscovery(endpoints []string) *ServiceDiscovery {
	// 新建etdc client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil
	}
	// 新建服务发现对象
	return &ServiceDiscovery{
		cli:        cli,
		serverList: make(map[string]string),
	}
}

func (s *ServiceDiscovery) Close() error {
	return s.cli.Close()
}

// WatchService 监听服务
func (s *ServiceDiscovery) WatchService(prefix string) error {
	// 获取当前service列表
	response, err := s.cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range response.Kvs {
		s.SetServiceList(string(kv.Key), string(kv.Value))
	}

	// 监听前缀
	go s.watcher(prefix)
	return nil
}

func (s *ServiceDiscovery) SetServiceList(key string, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.serverList[key] = value
	log.Println("Put key: ", key, " value: ", value)
}

func (s *ServiceDiscovery) watcher(prefix string) {
	ch := s.cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	log.Println("Watch prefix: %s now", prefix)
	for wresp := range ch {
		for _, event := range wresp.Events {
			switch event.Type {
			case mvccpb.PUT:
				s.SetServiceList(string(event.Kv.Key), string(event.Kv.Value))
			case mvccpb.DELETE:
				s.DelServiceList(string(event.Kv.Key))
			}
		}
	}
}

func (s *ServiceDiscovery) DelServiceList(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.serverList, key)
	log.Println("Del key: ", key)
}

func main() {
	var endpoints = []string{"localhost:2379"}
	ser := NewServiceDiscovery(endpoints)
	defer ser.Close()
	ser.WatchService("/web/")
	ser.WatchService("/gRPC/")

	// 定时打印服务列表
	for {
		select {
		case <-time.Tick(10 * time.Second):
			log.Println(ser.serverList)
		}
	}
}
