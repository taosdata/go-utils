package web

import (
	"github.com/gin-contrib/cors"
	"reflect"
	"testing"
	"time"
)

func TestCorsConfig_GetConfig(t *testing.T) {
	type fields struct {
		AllowAllOrigins  bool
		AllowOrigins     []string
		AllowHeaders     []string
		ExposeHeaders    []string
		AllowCredentials bool
		AllowWebSockets  bool
	}
	tests := []struct {
		name   string
		fields fields
		want   cors.Config
	}{
		{
			name:   "default",
			fields: fields{},
			want: cors.Config{
				AllowAllOrigins:        false,
				AllowOrigins:           []string{"http://127.0.0.1"},
				AllowOriginFunc:        nil,
				AllowMethods:           []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
				AllowHeaders:           []string{"Origin", "Content-Length", "Content-Type"},
				AllowCredentials:       false,
				ExposeHeaders:          []string{"Authorization"},
				MaxAge:                 time.Hour * 12,
				AllowWildcard:          true,
				AllowBrowserExtensions: false,
				AllowWebSockets:        false,
				AllowFiles:             false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &CorsConfig{
				AllowAllOrigins:  tt.fields.AllowAllOrigins,
				AllowOrigins:     tt.fields.AllowOrigins,
				AllowHeaders:     tt.fields.AllowHeaders,
				ExposeHeaders:    tt.fields.ExposeHeaders,
				AllowCredentials: tt.fields.AllowCredentials,
				AllowWebSockets:  tt.fields.AllowWebSockets,
			}
			if got := conf.GetConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCorsConfig_Init(t *testing.T) {
	type fields struct {
		AllowAllOrigins  bool
		AllowOrigins     []string
		AllowHeaders     []string
		ExposeHeaders    []string
		AllowCredentials bool
		AllowWebSockets  bool
	}
	tests := []struct {
		name   string
		fields fields
		want   *CorsConfig
	}{
		{
			name:   "default",
			fields: fields{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &CorsConfig{
				AllowAllOrigins:  tt.fields.AllowAllOrigins,
				AllowOrigins:     tt.fields.AllowOrigins,
				AllowHeaders:     tt.fields.AllowHeaders,
				ExposeHeaders:    tt.fields.ExposeHeaders,
				AllowCredentials: tt.fields.AllowCredentials,
				AllowWebSockets:  tt.fields.AllowWebSockets,
			}
			conf.Init()
		})
	}
}
