package config_client

import (
	"encoding/json"
)

func byteToStruct(b []byte,obj interface{}) error{
	if err := json.Unmarshal(b, obj); err != nil {
		return err
	}
	return nil
}