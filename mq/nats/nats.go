package nats

import (
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/go-utils/mq"
	"strings"
	"time"
)

type Nats struct {
	nc               *nats.Conn
	ec               *nats.EncodedConn
	logger           logrus.FieldLogger
	OnConnect        mq.OnConnectHandler
	OnConnectionLost mq.ConnectionLostHandler
}

func (natsMQ *Nats) SetOnConnectHandler(onConn mq.OnConnectHandler) *Nats {
	natsMQ.OnConnect = onConn
	return natsMQ
}

func (natsMQ *Nats) SetConnectionLostHandler(onLost mq.ConnectionLostHandler) *Nats {
	natsMQ.OnConnectionLost = onLost
	return natsMQ
}

func (natsMQ *Nats) connect(conf *Config) error {
	url := conf.Addr
	if url == "" {
		url = nats.DefaultURL
	}

	opts := []nats.Option{
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			natsMQ.logger.Infof("Got disconnected! Reason: %q\n", err)
			if natsMQ.OnConnectionLost != nil {
				natsMQ.OnConnectionLost(err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			natsMQ.logger.Infof("Got reconnected to %v!\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			natsMQ.logger.Infof("Connection closed. Reason: %q\n", nc.LastError())
		}),
	}
	if conf.MaxReconnects > 0 {
		opts = append(opts, nats.MaxReconnects(conf.MaxReconnects))
	}
	if conf.ReconnectWait > 0 {
		opts = append(opts, nats.ReconnectWait(time.Duration(conf.ReconnectWait)*time.Second))
	}
	if conf.Token != "" {
		opts = append(opts, nats.Token(conf.Token))
	}
	if conf.Username != "" {
		opts = append(opts, nats.UserInfo(conf.Username, conf.Password))
	}
	var err error
	natsMQ.nc, err = nats.Connect(url, opts...)
	if err != nil {
		return err
	}
	//natsMQ.ec, err = nats.NewEncodedConn(natsMQ.nc, nats.JSON_ENCODER)
	natsMQ.ec, err = nats.NewEncodedConn(natsMQ.nc, JSON_ENCODER)
	if err != nil {
		return err
	}
	return nil
}

func (natsMQ *Nats) Stop() {
	if natsMQ.ec != nil {
		natsMQ.ec.Close()
	}
}

func (natsMQ *Nats) Publish(topic string, data interface{}) error {
	if natsMQ.ec == nil {
		return errors.New("broker not connected")
	}
	return natsMQ.ec.Publish(topic, data)
}

func (natsMQ *Nats) PublishWithKey(topic, key string, data interface{}, properties mq.Properties) error {
	return natsMQ.Publish(topic, data)
}

func (natsMQ *Nats) Subscribe(topic string, fn mq.Handler) (mq.Subscriber, error) {
	if natsMQ.ec == nil {
		return nil, errors.New("nats not connected")
	}
	topic = changeTopic(topic)
	return natsMQ.ec.Subscribe(topic, fn)
}

func (natsMQ *Nats) QueueSubscribe(topic, queue string, fn mq.Handler) (mq.Subscriber, error) {
	if natsMQ.ec == nil {
		return nil, errors.New("nats not connected")
	}
	topic = changeTopic(topic)
	return natsMQ.ec.QueueSubscribe(topic, queue, fn)
}

func (natsMQ *Nats) KeyQueueSubscribe(topic, queue string, cb mq.Handler) (mq.Subscriber, error) {
	return natsMQ.QueueSubscribe(topic, queue, cb)
}

func (natsMQ *Nats) BindRecvQueueChan(topic, queue string, channel interface{}) (mq.Subscriber, error) {
	if natsMQ.nc == nil {
		return nil, errors.New("nats not connected")
	}
	topic = changeTopic(topic)
	return natsMQ.ec.BindRecvQueueChan(topic, queue, channel)
}

func (natsMQ *Nats) Request(subject string, v interface{}, vPtr interface{}, timeout time.Duration) error {
	return natsMQ.ec.Request(subject, v, vPtr, timeout)
}

func (natsMQ *Nats) String() string {
	return "nats"
}

func (natsMQ *Nats) TopicRegexSupported() bool {
	return true
}

func changeTopic(topic string) string {
	if strings.HasSuffix(topic, ".*") {
		return strings.TrimSuffix(topic, ".*") + ".>"
	} else if strings.HasSuffix(topic, "*") {
		return strings.TrimSuffix(topic, "*") + ".>"
	}
	return topic
}

func Run(conf *Config, logger logrus.FieldLogger, options mq.Options) mq.MQ {
	m := &Nats{
		logger:           logger,
		OnConnect:        options.OnConnect,
		OnConnectionLost: options.OnConnectionLost,
	}
	reconnectWait := conf.ReconnectWait
	if reconnectWait <= 0 {
		reconnectWait = 2
	}
	go func() {
		for {
			err := m.connect(conf)
			if err != nil {
				logger.WithError(err).Error("connect to nats broker error")
				time.Sleep(time.Duration(reconnectWait) * time.Second)
			} else {
				break
			}
		}
		logger.Info("nats server connected")
		if m.OnConnect != nil {
			m.OnConnect()
		}
	}()
	return m
}
