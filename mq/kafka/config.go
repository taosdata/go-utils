package kafka

import (
	"flag"
	"os"
	"strconv"
)

type Config struct {
	Brokers    string
	Version    string
	SASLEnable bool
	User       string
	Password   string
}

func (kafka *Config) Init() {
	if kafka.Brokers == "" {
		if val := os.Getenv("KAFKA_BROKERS"); val != "" {
			kafka.Brokers = val
		}
	}
	flag.StringVar(&kafka.Brokers, "kafka.brokers", kafka.Brokers, "kafka brokers")

	if kafka.Version == "" {
		if val := os.Getenv("KAFKA_VERSION"); val != "" {
			kafka.Version = val
		}
	}
	flag.StringVar(&kafka.Version, "kafka.version", kafka.Version, "kafka version")

	if !kafka.SASLEnable {
		if val := os.Getenv("KAFKA_SASL_ENABLE"); val != "" {
			kafka.SASLEnable, _ = strconv.ParseBool(val)
		}
	}
	flag.BoolVar(&kafka.SASLEnable, "kafka.saslEnable", kafka.SASLEnable, "kafka sasl enable")

	if kafka.User == "" {
		if val := os.Getenv("KAFKA_USER"); val != "" {
			kafka.User = val
		}
	}
	flag.StringVar(&kafka.User, "kafka.user", kafka.User, "kafka user")

	if kafka.Password == "" {
		if val := os.Getenv("KAFKA_PASSWORD"); val != "" {
			kafka.Password = val
		}
	}
	flag.StringVar(&kafka.Password, "kafka.password", kafka.Password, "kafka password")
}
