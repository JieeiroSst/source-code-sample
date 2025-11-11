package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/yourusername/hexagon-app/internal/core/ports"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	kafkaconn "github.com/yourusername/hexagon-app/internal/infrastructure/kafka"
	"go.uber.org/zap"
)

type kafkaPublisher struct {
	writer       *kafka.Writer
	errorWriter  *kafka.Writer
	config       *config.KafkaConfig
	logger       *zap.Logger
	errorHandler *ErrorHandler
}

type MessageMetadata struct {
	MessageID   string                 `json:"message_id"`
	Timestamp   time.Time              `json:"timestamp"`
	RetryCount  int                    `json:"retry_count"`
	Key         string                 `json:"key"`
	Headers     map[string]string      `json:"headers"`
	OriginalMsg map[string]interface{} `json:"original_message,omitempty"`
}

func NewKafkaPublisher(
	kafkaConn *kafkaconn.KafkaConnection,
	cfg *config.KafkaConfig,
	logger *zap.Logger,
) (ports.MessagePublisher, error) {
	writer := kafkaConn.CreateWriter(cfg.Topic)
	errorWriter := kafkaConn.CreateWriter(cfg.ErrorTopic)

	errorHandler := NewErrorHandler(kafkaConn, cfg, logger)

	publisher := &kafkaPublisher{
		writer:       writer,
		errorWriter:  errorWriter,
		config:       cfg,
		logger:       logger,
		errorHandler: errorHandler,
	}

	return publisher, nil
}

func (p *kafkaPublisher) Publish(ctx context.Context, key string, message interface{}) error {
	messageID := uuid.New().String()

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	metadata := MessageMetadata{
		MessageID:  messageID,
		Timestamp:  time.Now(),
		RetryCount: 0,
		Key:        key,
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(key),
		Value: body,
		Headers: []kafka.Header{
			{Key: "message-id", Value: []byte(messageID)},
			{Key: "timestamp", Value: []byte(metadata.Timestamp.Format(time.RFC3339))},
			{Key: "retry-count", Value: []byte("0")},
			{Key: "content-type", Value: []byte("application/json")},
		},
		Time: time.Now(),
	}

	// Write message
	err = p.writer.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		p.logger.Error("Failed to publish message to Kafka",
			zap.String("key", key),
			zap.String("message_id", messageID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to write message: %w", err)
	}

	p.logger.Info("Message published successfully to Kafka",
		zap.String("key", key),
		zap.String("message_id", messageID),
		zap.String("topic", p.config.Topic),
	)

	return nil
}

func (p *kafkaPublisher) PublishWithRetry(ctx context.Context, key string, message interface{}, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := p.Publish(ctx, key, message)
		if err == nil {
			return nil
		}

		lastErr = err
		p.logger.Warn("Retry publishing message to Kafka",
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", maxRetries),
			zap.String("key", key),
			zap.Error(err),
		)

		backoff := time.Duration(attempt+1) * p.config.RetryBackoff
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	if err := p.errorHandler.HandleError(ctx, key, message, lastErr, maxRetries); err != nil {
		p.logger.Error("Failed to send message to error topic",
			zap.String("key", key),
			zap.Error(err),
		)
	}

	return fmt.Errorf("failed to publish after %d retries: %w", maxRetries, lastErr)
}

func (p *kafkaPublisher) Close() error {
	var errs []error

	if err := p.writer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close writer: %w", err))
	}

	if err := p.errorWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close error writer: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing publisher: %v", errs)
	}

	p.logger.Info("Kafka publisher closed successfully")
	return nil
}
