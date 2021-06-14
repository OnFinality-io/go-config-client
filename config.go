package config_client

import (
	"bytes"
	"context"
	"fmt"
	cmap "github.com/orcaman/concurrent-map"
	clientv3 "go.etcd.io/etcd/client/v3"
	"reflect"
	"sync"
	"time"
)

var (
	_configOnce sync.Once
	_conf       *ConfigClient
	structMap   = cmap.New() // To cache struct data
	DialTimeout = 5 * time.Second
)

type Updater interface {
	ConfigUpdated(raw []byte, o interface{})
}

type ConfigSrv struct {
	RawData    []byte
	StructData interface{}
}

type ConfigClient struct {
	Client    *clientv3.Client
	Namespace string
}

type ConfigInitParam struct {
	Endpoints   []string
	DialTimeout *time.Duration
	Namespace   string
	Username    string
	Password    string
}

func GetInstance() *ConfigClient {
	return _conf
}

func Init(param *ConfigInitParam) (*ConfigClient, error) {
	var rErr error
	_configOnce.Do(func() {
		dialTimeout := DialTimeout
		if param.DialTimeout != nil {
			dialTimeout = *param.DialTimeout
		}
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   param.Endpoints,
			DialTimeout: dialTimeout,
			Username:    param.Username,
			Password:    param.Password,
		})
		if err != nil {
			rErr = err
			fmt.Printf("cli err %v\n", err)
			return
		}

		_conf = &ConfigClient{Client: cli, Namespace: param.Namespace}
		resp, err := cli.Get(context.TODO(), param.Namespace, clientv3.WithPrefix())
		if err != nil {
			rErr = err
			return
		}

		for _, kvs := range resp.Kvs {
			key := string(kvs.Key)
			fmt.Printf("Init configs from etcd. %q value: %q\n", kvs.Key, kvs.Value)
			//rawDataMap.Set(key, kvs.Value)
			structMap.Set(key, &ConfigSrv{RawData: kvs.Value})
		}

		go _conf.watch()

	})
	return _conf, rErr
}

func (configClient *ConfigClient) watch() {
	watchChan := configClient.Client.Watch(context.TODO(), configClient.Namespace, clientv3.WithPrefix())
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			//fmt.Printf("Event received! %s executed on %q with value %q\n", event.Type, event.Kv.Key, event.Kv.Value)
			key := string(event.Kv.Key)
			if event.Type == clientv3.EventTypeDelete {
				structMap.Remove(key)
			} else {
				rawData := event.Kv.Value
				if val, hit := structMap.Get(key); hit {
					if configSrv, ok := val.(*ConfigSrv); ok {
						if bytes.Compare(configSrv.RawData, rawData) != 0 {
							configClient.toRefreshData(configSrv, rawData, key)
						}
					}
				} else {
					structMap.Set(key, &ConfigSrv{RawData: rawData})
				}
			}

		}
	}
}

func (configClient *ConfigClient) toRefreshData(configSrv *ConfigSrv, rawData []byte, key string) {
	configSrv.RawData = rawData
	if configSrv.StructData != nil {
		structType := reflect.TypeOf(configSrv.StructData)
		// Currently, Only supported pointer
		if structType.Kind() == reflect.Ptr {
			structPointer := reflect.New(structType.Elem()).Interface()
			err := byteToStruct(rawData, structPointer)
			if err == nil {
				configSrv.StructData = structPointer
			} else {
				fmt.Printf("Unmarshal Err: key:%s, err:%v\n", key, err)
			}

		} else {
			// TODO non-pointer
			//structPointer := reflect.New(structType).Elem().Interface()
			//byteToStruct(rawData,&structPointer)
		}

	}

	if sv, ok := configSrv.StructData.(Updater); ok {
		fmt.Printf("v implements String(): %s\n", sv) // note: sv, not v
		sv.ConfigUpdated(rawData, configSrv.StructData)
	}
}

func (configClient *ConfigClient) Get(key string, OType interface{}) (interface{}, error) {
	key = fmt.Sprintf("%s%s", configClient.Namespace, key)
	if val, hit := structMap.Get(key); hit {
		if configSrv, ok := val.(*ConfigSrv); ok {
			if configSrv.StructData == nil {
				err := byteToStruct(configSrv.RawData, OType)
				if err != nil {
					fmt.Printf("Unmarshal Err: key:%s, err:%v\n", key, err)
					return nil, err
				}
				configSrv.StructData = OType
				return OType, nil
			} else {
				return configSrv.StructData, nil
			}

		}
	}
	return nil, fmt.Errorf("[%s] Not found", key)
}

func (configClient *ConfigClient) Put(key string, val string) error {
	_, err := configClient.Client.Put(context.TODO(), key, val)
	return err
}

func (configClient *ConfigClient) CloseConfig() {
	if configClient.Client != nil && configClient.Client != nil {
		configClient.Client.Close()
	}
}
