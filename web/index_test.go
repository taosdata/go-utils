package web_test

import (
	"github.com/gin-gonic/gin"
	"github.com/taosdata/go-utils/web"
	"testing"
)

type TestController struct {
}

func (t *TestController) Init(router gin.IRouter) {
	panic("implement me")
}

func TestAddController(t *testing.T) {
	type args struct {
		controller web.ServiceController
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "default",
			args: args{controller: &TestController{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			web.AddController(tt.args.controller)
		})
	}
}
