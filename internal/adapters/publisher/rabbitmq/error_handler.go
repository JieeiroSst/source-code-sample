package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

type ErrorHandler struct {
	channel *amqp.Channel
	config  *config.RabbitMQConfig
	logger  *zap.Logger
}

type ErrorMessage struct {
	OriginalMessage interface{} `json:"original_message"`
	RoutingKey      string      `json:"routing_key"`
	Error           string      `json:"error"`
	RetryCount      int         `json:"retry_count"`
	Timestamp       time.Time   `json:"timestamp"`
	FailedAt        time.Time   `json:"failed_at"`
}

func NewErrorHandler(channel *amqp.Channel, config *config.RabbitMQConfig, logger *zap.Logger) *ErrorHandler {
	return &ErrorHandler{
		channel: channel,
		config:  config,
		logger:  logger,
	}
}

func (h *ErrorHandler) HandleError(ctx context.Context, routingKey string, message interface{}, err error, retryCount int) error {
	errorMsg := ErrorMessage{
		OriginalMessage: message,
		RoutingKey:      routingKey,
		Error:           err.Error(),
		RetryCount:      retryCount,
		Timestamp:       time.Now(),
		FailedAt:        time.Now(),
	}

	body, marshalErr := json.Marshal(errorMsg)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal error message: %w", marshalErr)
	}

	if err := h.ensureErrorQueue(); err != nil {
		return fmt.Errorf("failed to ensure error queue: %w", err)
	}

	publishErr := h.channel.PublishWithContext(
		ctx,
		h.config.DeadLetterExchange,
		h.config.ErrorQueue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Headers: amqp.Table{
				"x-original-routing-key": routingKey,
				"x-error":                err.Error(),
				"x-retry-count":          retryCount,
				"x-failed-at":            time.Now().Unix(),
			},
		},
	)

	if publishErr != nil {
		h.logger.Error("Failed to publish error message",
			zap.String("routing_key", routingKey),
			zap.Error(publishErr),
		)
		return publishErr
	}

	h.logger.Info("Error message sent to error queue",
		zap.String("routing_key", routingKey),
		zap.String("error", err.Error()),
		zap.Int("retry_count", retryCount),
	)

	return nil
}

func (h *ErrorHandler) ensureErrorQueue() error {
	args := amqp.Table{
		"x-message-ttl": int32(86400000),
	}

	_, err := h.channel.QueueDeclare(
		h.config.ErrorQueue,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return err
	}

	err = h.channel.QueueBind(
		h.config.ErrorQueue,
		h.config.ErrorQueue,
		h.config.DeadLetterExchange,
		false,
		nil,
	)

	return err
}
