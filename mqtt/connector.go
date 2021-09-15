package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/go-utils/util"
	"io/ioutil"
	"strings"
	"time"
)

type Connector struct {
	logger    logrus.FieldLogger
	Connected bool
	start     bool
	client    mqtt.Client
	config    *Config
	pool      *ants.Pool
	onConnect func()
}

func newTLSConfig(conf *Config) *tls.Config {
	// logger.Debugf("mqtt NewTLSConfig: %s %s %s", caFile, certFile, keyFile)
	// var err error
	// var cert tls.Certificate

	certpool := x509.NewCertPool()
	//certpool.AppendCertsFromPEM([]byte(setting.CAPath))
	caCert, err := ioutil.ReadFile(conf.CAPath)
	if err != nil {
		panic(err)
	}
	certpool.AppendCertsFromPEM(caCert)

	//cert, err := tls.X509KeyPair([]byte(setting.ClientCert), []byte(setting.Key))
	cert, err := tls.LoadX509KeyPair(conf.CertPath, conf.KeyPath)
	if err != nil {
		panic(err)
	}

	return &tls.Config{
		// RootCAs = certs used to verify server cert.
		RootCAs: certpool,
		// ClientAuth = whether to request cert from server.
		// Since the server is set up for SSL, this happens
		// anyways.
		ClientAuth: tls.NoClientCert,
		// ClientCAs = certs used to validate client cert.
		ClientCAs: nil,
		// InsecureSkipVerify = verify that cert contents
		// match server. IP matches what is in cert etc.
		InsecureSkipVerify: true,
		// Certificates = list of certs client sends to server.
		Certificates: []tls.Certificate{cert},
	}
}

func (conn *Connector) Subscribe(topic string, qos byte, handler func(topic string, msg []byte)) bool {
	return conn.SubscribeWithReceiveTime(topic, qos, func(topic string, msg []byte, time time.Time) {
		handler(topic, msg)
	})
}

func (conn *Connector) SubscribeWithReceiveTime(topic string, qos byte, handler func(topic string, msg []byte, time time.Time)) bool {
	err := conn.client.Subscribe(topic, qos, func(client mqtt.Client, message mqtt.Message) {
		now := time.Now()
		antsError := ants.Submit(func() {
			handler(message.Topic(), message.Payload(), now)
		})
		if antsError != nil {
			conn.logger.WithError(antsError).Error("creat ants ")
		}
	}).Error()
	if err != nil {
		conn.logger.WithError(err).Error("mqtt subscribe topic:", topic)
		return false
	}
	return true
}

func (conn *Connector) connect(conf Config) {
	if !conn.start {
		return
	}

	conn.logger.Info("mqtt connect")

	opts := mqtt.NewClientOptions()
	opts.ClientID = conf.ClientID
	if opts.ClientID == "" {
		opts.ClientID = util.UUID()
	}
	opts.Username = conf.Username
	opts.Password = conf.Password
	opts.CleanSession = true
	opts.KeepAlive = conf.KeepAlive
	opts.AutoReconnect = true
	if strings.HasPrefix(conf.Address, "ssl") {
		opts.TLSConfig = newTLSConfig(&conf)
	}
	opts.AddBroker(conf.Address)

	opts.OnConnect = func(c mqtt.Client) {
		conn.Connected = true
		if conn.onConnect != nil {
			conn.onConnect()
		}
	}

	opts.OnConnectionLost = func(c mqtt.Client, e error) {
		conn.logger.WithError(e).Warn("mqtt connection lost")
		conn.onDisconnected()
	}

	client := mqtt.NewClient(opts)
	conn.client = client
	conn.Connected = true

	token := client.Connect()
	for token.Wait() && token.Error() != nil {
		conn.logger.WithError(token.Error()).Error("could not connect to mqtt broker, will retry after 30s")
		time.Sleep(30 * time.Second)
		token = client.Connect()
	}
	conn.logger.Info("connect to Config Server...")
}

func (conn *Connector) onDisconnected() {
	conn.Connected = false
}

func (conn *Connector) Stop() {
	conn.start = false
	if conn.client != nil {
		conn.client.Disconnect(1000)
	}
}

func (conn *Connector) Publish(topic string, qos byte, retained bool, payload []byte) error {
	if conn.client == nil || !conn.Connected {
		return errors.New("mqtt server not connected")
	}

	return conn.client.Publish(topic, qos, retained, payload).Error()
}

func NewConnector(config Config, pool *ants.Pool, onConnect func()) *Connector {
	conn := &Connector{
		start:     true,
		pool:      pool,
		onConnect: onConnect,
	}

	go conn.connect(config)
	return conn
}
