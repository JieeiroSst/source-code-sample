package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	kafkaconn "github.com/yourusername/hexagon-app/internal/infrastructure/kafka"
	"go.uber.org/zap"
)

type ErrorHandler struct {
	errorWriter *kafka.Writer
	dlqWriter   *kafka.Writer
	config      *config.KafkaConfig
	logger      *zap.Logger
}

type ErrorMessage struct {
	MessageID       string                 `json:"message_id"`
	OriginalKey     string                 `json:"original_key"`
	OriginalMessage interface{}            `json:"original_message"`
	Error           string                 `json:"error"`
	ErrorTime       time.Time              `json:"error_time"`
	RetryCount      int                    `json:"retry_count"`
	StackTrace      string                 `json:"stack_trace,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

func NewErrorHandler(
	kafkaConn *kafkaconn.KafkaConnection,
	cfg *config.KafkaConfig,
	logger *zap.Logger,
) *ErrorHandler {
	return &ErrorHandler{
		errorWriter: kafkaConn.CreateWriter(cfg.ErrorTopic),
		dlqWriter:   kafkaConn.CreateWriter(cfg.DeadLetterTopic),
		config:      cfg,
		logger:      logger,
	}
}

func (h *ErrorHandler) HandleError(ctx context.Context, key string, message interface{}, err error, retryCount int) error {
	errorMsg := ErrorMessage{
		MessageID:       uuid.New().String(),
		OriginalKey:     key,
		OriginalMessage: message,
		Error:           err.Error(),
		ErrorTime:       time.Now(),
		RetryCount:      retryCount,
		Metadata: map[string]interface{}{
			"topic":        h.config.Topic,
			"retry_policy": fmt.Sprintf("max_retry:%d", h.config.MaxRetry),
		},
	}

	body, marshalErr := json.Marshal(errorMsg)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal error message: %w", marshalErr)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(key),
		Value: body,
		Headers: []kafka.Header{
			{Key: "error-message-id", Value: []byte(errorMsg.MessageID)},
			{Key: "original-key", Value: []byte(key)},
			{Key: "error-time", Value: []byte(errorMsg.ErrorTime.Format(time.RFC3339))},
			{Key: "retry-count", Value: []byte(fmt.Sprintf("%d", retryCount))},
			{Key: "error-type", Value: []byte("publish_error")},
		},
		Time: time.Now(),
	}

	writeErr := h.errorWriter.WriteMessages(ctx, kafkaMsg)
	if writeErr != nil {
		h.logger.Error("Failed to write error message to error topic",
			zap.String("key", key),
			zap.Error(writeErr),
		)
		return writeErr
	}

	h.logger.Info("Error message written to error topic",
		zap.String("error_topic", h.config.ErrorTopic),
		zap.String("key", key),
		zap.String("error_message_id", errorMsg.MessageID),
		zap.Int("retry_count", retryCount),
	)

	return nil
}

func (h *ErrorHandler) SendToDeadLetterQueue(ctx context.Context, errorMsg ErrorMessage) error {
	body, err := json.Marshal(errorMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal error message for DLQ: %w", err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(errorMsg.OriginalKey),
		Value: body,
		Headers: []kafka.Header{
			{Key: "dlq-message-id", Value: []byte(errorMsg.MessageID)},
			{Key: "original-key", Value: []byte(errorMsg.OriginalKey)},
			{Key: "final-error", Value: []byte(errorMsg.Error)},
			{Key: "retry-count", Value: []byte(fmt.Sprintf("%d", errorMsg.RetryCount))},
			{Key: "moved-to-dlq", Value: []byte(time.Now().Format(time.RFC3339))},
		},
		Time: time.Now(),
	}

	err = h.dlqWriter.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		h.logger.Error("Failed to write message to DLQ",
			zap.String("key", errorMsg.OriginalKey),
			zap.Error(err),
		)
		return err
	}

	h.logger.Warn("Message moved to Dead Letter Queue",
		zap.String("dlq_topic", h.config.DeadLetterTopic),
		zap.String("key", errorMsg.OriginalKey),
		zap.Int("retry_count", errorMsg.RetryCount),
	)

	return nil
}

func (h *ErrorHandler) ReprocessFromErrorTopic(ctx context.Context, errorMsg ErrorMessage, targetWriter *kafka.Writer) error {
	if errorMsg.RetryCount >= h.config.MaxRetry {
		h.logger.Warn("Max retry exceeded, sending to DLQ",
			zap.String("key", errorMsg.OriginalKey),
			zap.Int("retry_count", errorMsg.RetryCount),
		)
		return h.SendToDeadLetterQueue(ctx, errorMsg)
	}

	errorMsg.RetryCount++

	body, err := json.Marshal(errorMsg.OriginalMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal original message: %w", err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(errorMsg.OriginalKey),
		Value: body,
		Headers: []kafka.Header{
			{Key: "message-id", Value: []byte(errorMsg.MessageID)},
			{Key: "retry-count", Value: []byte(fmt.Sprintf("%d", errorMsg.RetryCount))},
			{Key: "reprocessed", Value: []byte("true")},
			{Key: "reprocessed-at", Value: []byte(time.Now().Format(time.RFC3339))},
		},
		Time: time.Now(),
	}

	err = targetWriter.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		h.logger.Error("Failed to reprocess message",
			zap.String("key", errorMsg.OriginalKey),
			zap.Int("retry_count", errorMsg.RetryCount),
			zap.Error(err),
		)
		return err
	}

	h.logger.Info("Message reprocessed from error topic",
		zap.String("key", errorMsg.OriginalKey),
		zap.Int("retry_count", errorMsg.RetryCount),
	)

	return nil
}

func (h *ErrorHandler) Close() error {
	var errs []error

	if err := h.errorWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close error writer: %w", err))
	}

	if err := h.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close DLQ writer: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing error handler: %v", errs)
	}

	return nil
}
