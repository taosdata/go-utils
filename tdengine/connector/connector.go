// +build !windows

package connector

import (
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/config"
	"fmt"
)

func NewTDengineConnector(connectorType string, conf interface{}) (TDengineConnector, error) {
	switch connectorType {
	case common.TDengineRestfulConnectorType:
		restfulConfig := conf.(*config.TDengineRestful)
		return NewRestfulConnector(restfulConfig)
	case common.TDengineGoConnectorType:
		goConfig := conf.(*config.TDengineGo)
		return NewGoConnector(goConfig)
	default:
		return nil, fmt.Errorf("unsupported TDengine connector type %s", connectorType)
	}
}
