package mqtt

import (
	"flag"
	"os"
	"strconv"
)

type Config struct {
	Enabled    bool   `json:"enabled"`
	Address    string `json:"address"`
	ClientID   string `json:"clientID"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	KeepAlive  int64  `json:"keepAlive"`
	CAPath     string `json:"caPath"`
	CertPath   string `json:"certPath"`
	KeyPath    string `json:"keyPath"`
	ShareTopic bool   `json:"shareTopic"`
}

func (mqtt *Config) Init() {
	if val := os.Getenv("MQTT_ENABLED"); val != "" {
		mqtt.Enabled, _ = strconv.ParseBool(val)
	}

	flag.BoolVar(&mqtt.Enabled, "mqtt.enabled", mqtt.Enabled, "mqtt enabled")

	if addr := os.Getenv("MQTT_ADDR"); addr != "" {
		mqtt.Address = addr
	}

	if mqtt.Address == "" {
		mqtt.Address = "tcp://127.0.0.1:1883"
	}

	flag.StringVar(&mqtt.Address, "mqtt.addr", mqtt.Address, "mqtt address")

	if val := os.Getenv("MQTT_CLIENT_ID"); val != "" {
		mqtt.ClientID = val
	}

	flag.StringVar(&mqtt.ClientID, "mqtt.clientID", mqtt.ClientID, "mqtt clientID")

	if val := os.Getenv("MQTT_USERNAME"); val != "" {
		mqtt.Username = val
	}

	flag.StringVar(&mqtt.Username, "mqtt.username", mqtt.Username, "mqtt username")

	if val := os.Getenv("MQTT_PASSWORD"); val != "" {
		mqtt.Password = val
	}

	flag.StringVar(&mqtt.Password, "mqtt.password", mqtt.Password, "mqtt password")

	if val := os.Getenv("MQTT_KEEP_ALIVE"); val != "" {
		mqtt.KeepAlive, _ = strconv.ParseInt(val, 10, 64)
	}

	if mqtt.KeepAlive == 0 {
		mqtt.KeepAlive = 30
	}

	flag.Int64Var(&mqtt.KeepAlive, "mqtt.keepAlive", mqtt.KeepAlive, "mqtt keep alive, default is 30s")

	if val := os.Getenv("MQTT_CA_PATH"); val != "" {
		mqtt.CAPath = val
	}

	if mqtt.CAPath == "" {
		mqtt.CAPath = "ca.crt"
	}

	flag.StringVar(&mqtt.CAPath, "mqtt.caPath", mqtt.CAPath, "mqtt ca path")

	if val := os.Getenv("MQTT_CERT_PATH"); val != "" {
		mqtt.CertPath = val
	}

	if mqtt.CertPath == "" {
		mqtt.CertPath = "server.crt"
	}

	flag.StringVar(&mqtt.CertPath, "mqtt.certPath", mqtt.CertPath, "mqtt cert path")

	if val := os.Getenv("MQTT_KEY_PATH"); val != "" {
		mqtt.KeyPath = val
	}

	if mqtt.KeyPath == "" {
		mqtt.KeyPath = "server.key"
	}

	flag.StringVar(&mqtt.KeyPath, "mqtt.keyPath", mqtt.CertPath, "mqtt key path")
}
