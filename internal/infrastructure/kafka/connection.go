package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

type KafkaConnection struct {
	Config *config.KafkaConfig
	Logger *zap.Logger
}

var (
	kafkaInstance *KafkaConnection
	kafkaOnce     sync.Once
)

func NewKafkaConnection(cfg *config.KafkaConfig, logger *zap.Logger) *KafkaConnection {
	kafkaOnce.Do(func() {
		kafkaInstance = &KafkaConnection{
			Config: cfg,
			Logger: logger,
		}
	})
	return kafkaInstance
}

func (k *KafkaConnection) CreateWriter(topic string) *kafka.Writer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(k.Config.Brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},

		AllowAutoTopicCreation: true,
		Async:                  false,
		RequiredAcks:           kafka.RequireAll,

		BatchSize:    100,
		BatchBytes:   1048576,
		BatchTimeout: 10 * time.Millisecond,

		Compression: kafka.Snappy,

		MaxAttempts:  3,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,

		Logger:      kafka.LoggerFunc(k.kafkaLoggerAdapter),
		ErrorLogger: kafka.LoggerFunc(k.kafkaErrorLoggerAdapter),
	}

	k.Logger.Info("Kafka writer created",
		zap.String("topic", topic),
		zap.Strings("brokers", k.Config.Brokers),
	)

	return writer
}

func (k *KafkaConnection) CreateReader(topic string, groupID string) *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           k.Config.Brokers,
		GroupID:           groupID,
		Topic:             topic,
		MinBytes:          10e3,
		MaxBytes:          10e6,
		SessionTimeout:    k.Config.SessionTimeout,
		HeartbeatInterval: k.Config.HeartbeatInterval,
		CommitInterval:    0,
		StartOffset:       kafka.LastOffset,
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
		},
		ReadBackoffMin: 100 * time.Millisecond,
		ReadBackoffMax: 1 * time.Second,
		Logger:         kafka.LoggerFunc(k.kafkaLoggerAdapter),
		ErrorLogger:    kafka.LoggerFunc(k.kafkaErrorLoggerAdapter),
	})

	k.Logger.Info("Kafka reader created",
		zap.String("topic", topic),
		zap.String("group_id", groupID),
		zap.Strings("brokers", k.Config.Brokers),
	)

	return reader
}

func (k *KafkaConnection) EnsureTopics(ctx context.Context, topics []string) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.Config.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial kafka broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(
		ctx,
		"tcp",
		fmt.Sprintf("%s:%d", controller.Host, controller.Port),
	)
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: "604800000",
				},
				{
					ConfigName:  "compression.type",
					ConfigValue: "snappy",
				},
			},
		})
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		k.Logger.Warn("Some topics may already exist", zap.Error(err))
	} else {
		k.Logger.Info("Topics created successfully", zap.Strings("topics", topics))
	}

	return nil
}

func (k *KafkaConnection) ListTopics(ctx context.Context) ([]string, error) {
	conn, err := kafka.DialContext(ctx, "tcp", k.Config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	topicMap := make(map[string]bool)
	for _, p := range partitions {
		topicMap[p.Topic] = true
	}

	topics := make([]string, 0, len(topicMap))
	for topic := range topicMap {
		topics = append(topics, topic)
	}

	return topics, nil
}

func (k *KafkaConnection) GetTopicMetadata(ctx context.Context, topic string) (*kafka.Partition, error) {
	conn, err := kafka.DialContext(ctx, "tcp", k.Config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found for topic: %s", topic)
	}

	return &partitions[0], nil
}

func (k *KafkaConnection) DeleteTopic(ctx context.Context, topic string) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.Config.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(
		ctx,
		"tcp",
		fmt.Sprintf("%s:%d", controller.Host, controller.Port),
	)
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.DeleteTopics(topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	k.Logger.Info("Topic deleted", zap.String("topic", topic))
	return nil
}

func (k *KafkaConnection) CheckConnection(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.Config.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("failed to get brokers: %w", err)
	}

	k.Logger.Info("Kafka connection successful", zap.Strings("brokers", k.Config.Brokers))
	return nil
}

func (k *KafkaConnection) kafkaLoggerAdapter(msg string, args ...interface{}) {
	k.Logger.Debug(fmt.Sprintf(msg, args...))
}

func (k *KafkaConnection) kafkaErrorLoggerAdapter(msg string, args ...interface{}) {
	k.Logger.Error(fmt.Sprintf(msg, args...))
}
