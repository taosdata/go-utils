// +build windows

package connector

import (
	"errors"
	"fmt"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/config"
)

func NewTDengineConnector(connectorType string, conf interface{}) (TDengineConnector, error) {
	switch connectorType {
	case common.TDengineRestfulConnectorType:
		restfulConfig := conf.(*config.TDengineRestful)
		return NewRestfulConnector(restfulConfig)
	case common.TDengineGoConnectorType:
		return nil, errors.New("not support on windows")
	default:
		return nil, fmt.Errorf("unsupported TDengine connector type %s", connectorType)
	}
}
