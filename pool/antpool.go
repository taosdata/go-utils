package pool

import (
	"github.com/panjf2000/ants/v2"
	"github.com/taosdata/go-utils/pool/config"
)

var GoroutinePool *ants.Pool

func init() {
	var err error
	GoroutinePool, err = ants.NewPool(config.GoPoolSize)
	if err != nil {
		panic(err)
	}
}
