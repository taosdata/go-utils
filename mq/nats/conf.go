package nats

import (
	"flag"
	"log"
	"os"
	"strconv"
)

type Config struct {
	Addr               string // nats://127.0.0.1:4222
	MaxReconnects      int    // 5
	ReconnectWait      int    // 2s
	Token              string
	Username           string
	Password           string
	EmbeddedServerPort int
}

func (nats *Config) Init() {
	if nats.Addr == "" {
		if val := os.Getenv("NATS_ADDR"); val != "" {
			nats.Addr = val
		}
		if nats.Addr == "" {
			nats.Addr = "nats://127.0.0.1:4222"
		}
	}
	flag.StringVar(&nats.Addr, "nats.addr", nats.Addr, "nats address")

	if nats.MaxReconnects == 0 {
		if val := os.Getenv("NATS_MAX_RECONNECTS"); val != "" {
			i, err := strconv.Atoi(val)
			if err != nil {
				log.Fatalf("illegal envvar NATS_MAX_RECONNECTS: %s", val)
			}
			nats.MaxReconnects = i
		}
		if nats.MaxReconnects == 0 {
			nats.MaxReconnects = 5
		}
	}
	flag.IntVar(&nats.MaxReconnects, "nats.maxReconnects", nats.MaxReconnects, "nats max reconnects")

	if nats.ReconnectWait == 0 {
		if val := os.Getenv("NATS_RECONNECT_WAIT"); val != "" {
			i, err := strconv.Atoi(val)
			if err != nil {
				log.Fatalf("illegal envvar NATS_RECONNECT_WAIT: %s", val)
			}
			nats.ReconnectWait = i
		}
		if nats.ReconnectWait == 0 {
			nats.ReconnectWait = 2
		}
	}
	flag.IntVar(&nats.ReconnectWait, "nats.reconnectWait", nats.ReconnectWait, "nats reconnect wait")

	if nats.Token == "" {
		if val := os.Getenv("NATS_TOKEN"); val != "" {
			nats.Token = val
		}
	}
	flag.StringVar(&nats.Token, "nats.token", nats.Token, "nats token")

	if nats.Username == "" {
		if val := os.Getenv("NATS_USERNAME"); val != "" {
			nats.Username = val
		}
	}
	flag.StringVar(&nats.Username, "nats.username", nats.Username, "nats username")

	if nats.Password == "" {
		if val := os.Getenv("NATS_PASSWORD"); val != "" {
			nats.Password = val
		}
	}
	flag.StringVar(&nats.Token, "nats.password", nats.Token, "nats password")
}
