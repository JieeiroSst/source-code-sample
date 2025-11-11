package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yourusername/hexagon-app/internal/core/ports"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

type publisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	config       *config.RabbitMQConfig
	logger       *zap.Logger
	errorHandler *ErrorHandler
}

type MessageMetadata struct {
	MessageID   string    `json:"message_id"`
	Timestamp   time.Time `json:"timestamp"`
	RetryCount  int       `json:"retry_count"`
	OriginalKey string    `json:"original_key"`
}

func NewPublisher(
	conn *amqp.Connection,
	cfg *config.RabbitMQConfig,
	logger *zap.Logger,
) (ports.MessagePublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		cfg.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	if err := ch.ExchangeDeclare(
		cfg.DeadLetterExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("failed to declare dead letter exchange: %w", err)
	}

	errorHandler := NewErrorHandler(ch, cfg, logger)

	return &publisher{
		conn:         conn,
		channel:      ch,
		config:       cfg,
		logger:       logger,
		errorHandler: errorHandler,
	}, nil
}

func (p *publisher) Publish(ctx context.Context, routingKey string, message interface{}) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	metadata := MessageMetadata{
		MessageID:   fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp:   time.Now(),
		RetryCount:  0,
		OriginalKey: routingKey,
	}

	headers := amqp.Table{
		"x-message-id":   metadata.MessageID,
		"x-timestamp":    metadata.Timestamp.Unix(),
		"x-retry-count":  metadata.RetryCount,
		"x-original-key": metadata.OriginalKey,
	}

	err = p.channel.PublishWithContext(
		ctx,
		p.config.Exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Headers:      headers,
		},
	)

	if err != nil {
		p.logger.Error("Failed to publish message",
			zap.String("routing_key", routingKey),
			zap.Error(err),
		)
		return err
	}

	p.logger.Info("Message published successfully",
		zap.String("routing_key", routingKey),
		zap.String("message_id", metadata.MessageID),
	)

	return nil
}

func (p *publisher) PublishWithRetry(ctx context.Context, routingKey string, message interface{}, maxRetries int) error {
	var lastErr error
	
	for i := 0; i < maxRetries; i++ {
		err := p.Publish(ctx, routingKey, message)
		if err == nil {
			return nil
		}

		lastErr = err
		p.logger.Warn("Retry publishing message",
			zap.Int("attempt", i+1),
			zap.Int("max_retries", maxRetries),
			zap.Error(err),
		)

		time.Sleep(time.Duration(i+1) * time.Second)
	}

	if err := p.errorHandler.HandleError(ctx, routingKey, message, lastErr, maxRetries); err != nil {
		p.logger.Error("Failed to send message to error queue", zap.Error(err))
	}

	return fmt.Errorf("failed to publish after %d retries: %w", maxRetries, lastErr)
}

func (p *publisher) Close() error {
	if err := p.channel.Close(); err != nil {
		return err
	}
	return nil
}
