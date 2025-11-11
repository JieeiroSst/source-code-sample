package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yourusername/hexagon-app/internal/core/domain"
	"go.uber.org/zap"
)

type KafkaMessageHandlers struct {
	logger *zap.Logger
}

func NewKafkaMessageHandlers(logger *zap.Logger) *KafkaMessageHandlers {
	return &KafkaMessageHandlers{
		logger: logger,
	}
}

func (h *KafkaMessageHandlers) HandleUserCreated(ctx context.Context, key string, value []byte, headers map[string]string) error {
	var event domain.UserCreatedEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal user created event: %w", err)
	}

	h.logger.Info("Processing user created event from Kafka",
		zap.String("user_id", event.UserID.String()),
		zap.String("email", event.Email),
		zap.String("message_id", headers["message-id"]),
	)

	h.logger.Info("User created event processed successfully",
		zap.String("user_id", event.UserID.String()),
	)

	return nil
}

func (h *KafkaMessageHandlers) HandleUserUpdated(ctx context.Context, key string, value []byte, headers map[string]string) error {
	var event domain.UserUpdatedEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal user updated event: %w", err)
	}

	h.logger.Info("Processing user updated event from Kafka",
		zap.String("user_id", event.UserID.String()),
		zap.String("email", event.Email),
		zap.String("message_id", headers["message-id"]),
	)

	h.logger.Info("User updated event processed successfully",
		zap.String("user_id", event.UserID.String()),
	)

	return nil
}

func (h *KafkaMessageHandlers) HandleGenericEvent(ctx context.Context, key string, value []byte, headers map[string]string) error {
	h.logger.Info("Processing generic event from Kafka",
		zap.String("key", key),
		zap.Int("payload_size", len(value)),
		zap.Any("headers", headers),
	)

	var data map[string]interface{}
	if err := json.Unmarshal(value, &data); err != nil {
		return fmt.Errorf("failed to unmarshal generic event: %w", err)
	}

	h.logger.Info("Generic event processed",
		zap.String("key", key),
		zap.Any("data", data),
	)

	return nil
}

func (h *KafkaMessageHandlers) RegisterHandlers(consumer *KafkaConsumer) {
	consumer.RegisterHandler("user.created", h.HandleUserCreated)
	consumer.RegisterHandler("user.updated", h.HandleUserUpdated)
	consumer.RegisterHandler("*", h.HandleGenericEvent) 

	h.logger.Info("Kafka handlers registered successfully")
}
