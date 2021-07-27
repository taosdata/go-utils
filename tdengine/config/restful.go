package config

import (
	"os"
	"strconv"

	"github.com/taosdata/go-utils/tdengine/common"
)

type TDengineRestful struct {
	Address         string
	AuthType        string
	Username        string
	Password        string
	MaxConnsPerHost int
}

func (conf *TDengineRestful) Init() {
	if conf.Address == "" {
		if val := os.Getenv("TDENGINE_ADDRESS"); val != "" {
			conf.Address = val
		} else {
			conf.Address = "http://127.0.0.1:6041"
		}
	}
	if conf.AuthType == "" {
		if val := os.Getenv("TDENGINE_AUTHTYPE"); val != "" {
			conf.AuthType = val
		} else {
			conf.AuthType = common.BasicAuthType
		}
	}
	if conf.Username == "" {
		if val := os.Getenv("TDENGINE_USERNAME"); val != "" {
			conf.Username = val
		} else {
			conf.Username = "root"
		}
	}
	if conf.Password == "" {
		if val := os.Getenv("TDENGINE_PASSWORD"); val != "" {
			conf.Password = val
		} else {
			conf.Password = "taosdata"
		}
	}
	if conf.MaxConnsPerHost == 0 {
		if val := os.Getenv("TDENGINE_MAX_CONNS_PER_HOST"); val != "" {
			v, err := strconv.Atoi(val)
			if err != nil {
				panic(err)
			}
			conf.MaxConnsPerHost = v
		} else {
			conf.MaxConnsPerHost = -1
		}
	}
}
