package kafka

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/go-utils/json"
	"github.com/taosdata/go-utils/mq"
	"github.com/taosdata/go-utils/util"
	"strings"
	"time"
)

type Kafka struct {
	client           sarama.Client
	producer         sarama.AsyncProducer
	OnConnect        mq.OnConnectHandler
	OnConnectionLost mq.ConnectionLostHandler
	logger           logrus.FieldLogger
}

func (kafkaMQ *Kafka) BindRecvQueueChan(topic, queue string, channel interface{}) (mq.Subscriber, error) {
	return newConsumerWithChan(kafkaMQ.client, kafkaMQ.logger, topic, queue, channel)
}

func (kafkaMQ *Kafka) connect(conf *Config) error {
	version, err := sarama.ParseKafkaVersion(conf.Version)
	if err != nil {
		return fmt.Errorf("Error parsing Config version: %v", err)
	}

	addrList := strings.Split(conf.Brokers, ",")
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = version
	kafkaConfig.MetricRegistry = metrics.DefaultRegistry

	if conf.SASLEnable {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = conf.User
		kafkaConfig.Net.SASL.Password = conf.Password
	}
	//kafkaConfig.Consumer.Group.Rebalance.Strategy

	kafkaMQ.client, err = sarama.NewClient(addrList, kafkaConfig)
	if err != nil {
		return err
	}
	kafkaMQ.producer, err = sarama.NewAsyncProducerFromClient(kafkaMQ.client)
	return err
}

func (kafkaMQ *Kafka) Stop() {
	if kafkaMQ.client != nil && !kafkaMQ.client.Closed() {
		kafkaMQ.client.Close()
	}
}

func (kafkaMQ *Kafka) PublishWithKey(topic, key string, data interface{}, headers mq.Properties) error {
	if kafkaMQ.producer == nil {
		return errors.New("broker not connected")
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		kafkaMQ.logger.Error(err)
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(jsonData),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	if len(headers) > 0 {
		for k, v := range headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   sarama.ByteEncoder(k),
				Value: sarama.ByteEncoder(v),
			})
		}
	}
	kafkaMQ.producer.Input() <- msg

	return nil
}

func (kafkaMQ *Kafka) Publish(topic string, data interface{}) error {
	return kafkaMQ.PublishWithKey(topic, "", data, nil)
}

func (kafkaMQ *Kafka) Subscribe(topic string, cb mq.Handler) (mq.Subscriber, error) {
	return kafkaMQ.QueueSubscribe(topic, util.UUID(), cb)
}

func (kafkaMQ *Kafka) QueueSubscribe(topic, queue string, cb mq.Handler) (mq.Subscriber, error) {
	if kafkaMQ.client == nil || kafkaMQ.client.Closed() {
		return nil, errors.New("kafkaMQ client not connect")
	}
	return newConsumer(kafkaMQ.client, kafkaMQ.logger, topic, queue, cb)
}

func (kafkaMQ *Kafka) KeyQueueSubscribe(topic, queue string, cb mq.Handler) (mq.Subscriber, error) {
	return kafkaMQ.QueueSubscribe(topic, queue, cb)
}

func (kafkaMQ *Kafka) String() string {
	return "kafkaMQ"
}

func (kafkaMQ *Kafka) TopicRegexSupported() bool {
	return false
}

func Run(conf *Config, logger logrus.FieldLogger, options mq.Options) mq.MQ {
	prometheusClient := prometheusmetrics.NewPrometheusProvider(
		metrics.DefaultRegistry,
		"broker",
		"kafka",
		prometheus.DefaultRegisterer,
		1*time.Second,
	)
	go prometheusClient.UpdatePrometheusMetrics()

	m := &Kafka{
		logger:           logger,
		OnConnect:        options.OnConnect,
		OnConnectionLost: options.OnConnectionLost,
	}
	go func() {
		for {
			err := m.connect(conf)
			if err != nil {
				logger.WithError(err).Error("connect to kafka broker error")
				time.Sleep(30 * time.Second)
			} else {
				break
			}
		}
		if m.OnConnect != nil {
			m.OnConnect()
		}
	}()
	return m
}
