package kafka

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/go-utils/mq"
	"reflect"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	client sarama.Client
	logger logrus.FieldLogger
	cb     func(m *sarama.ConsumerMessage)
	cancel context.CancelFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		consumer.cb(message)
		//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}
	return nil
}

func (consumer *Consumer) Unsubscribe() error {
	if consumer.cancel != nil {
		consumer.cancel()
	}
	return nil
}

func newConsumer(client sarama.Client, logger logrus.FieldLogger, topic, queue string, cb mq.Handler) (*Consumer, error) {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(queue, client)
	if err != nil {
		return nil, err
	}

	if cb == nil {
		return nil, errors.New("kafka: Handler required for EncodedConn Subscription")
	}
	argType, numArgs := argInfo(cb)
	if argType == nil {
		return nil, errors.New("kafka: Handler requires at least one argument")
	}

	cbValue := reflect.ValueOf(cb)
	var emptyMsgType = reflect.TypeOf(&sarama.ConsumerMessage{})
	wantsRaw := argType == emptyMsgType

	kcb := kafkaCB(wantsRaw, argType, numArgs, cbValue)

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client,
		logger: logger,
		cb:     kcb,
		cancel: cancel,
	}

	go func() {
		for {
			err = consumerGroup.Consume(ctx, []string{topic}, consumer)
			if err != nil {
				logger.Error(err)
				return
			}
			if ctx.Err() != nil {
				logger.Error(ctx.Err())
				return
			}
		}
	}()
	logger.Info("Sarama consumer up and running!...")
	return consumer, nil
}

func newConsumerWithChan(client sarama.Client, logger logrus.FieldLogger, topic, queue string, channel interface{}) (*Consumer, error) {
	chVal := reflect.ValueOf(channel)
	if chVal.Kind() != reflect.Chan {
		return nil, errors.New("argument needs to be a channel type")
	}
	argType := chVal.Type().Elem()

	consumerGroup, err := sarama.NewConsumerGroupFromClient(queue, client)
	if err != nil {
		return nil, err
	}
	cb := kafkaCB(false, argType, 1, chVal)
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client,
		logger: logger,
		cb:     cb,
		cancel: cancel,
	}

	go func() {
		for {
			err = consumerGroup.Consume(ctx, []string{topic}, consumer)
			if err != nil {
				logger.Error(err)
				return
			}
			if ctx.Err() != nil {
				logger.Error(ctx.Err())
				return
			}
		}
	}()
	logger.Info("Sarama consumer up and running!...")
	return consumer, nil
}
