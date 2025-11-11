package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yourusername/hexagon-app/internal/core/domain"
	"go.uber.org/zap"
)

type MessageHandlers struct {
	logger *zap.Logger
}

func NewMessageHandlers(logger *zap.Logger) *MessageHandlers {
	return &MessageHandlers{
		logger: logger,
	}
}

func (h *MessageHandlers) HandleUserCreated(ctx context.Context, body []byte, headers amqp.Table) error {
	var event domain.UserCreatedEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal user created event: %w", err)
	}

	h.logger.Info("Processing user created event",
		zap.String("user_id", event.UserID.String()),
		zap.String("email", event.Email),
	)

	return nil
}

func (h *MessageHandlers) HandleUserUpdated(ctx context.Context, body []byte, headers amqp.Table) error {
	var event domain.UserUpdatedEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal user updated event: %w", err)
	}

	h.logger.Info("Processing user updated event",
		zap.String("user_id", event.UserID.String()),
		zap.String("email", event.Email),
	)

	return nil
}

func (h *MessageHandlers) RegisterHandlers(consumer *Consumer) {
	consumer.RegisterHandler("user.created", h.HandleUserCreated)
	consumer.RegisterHandler("user.updated", h.HandleUserUpdated)
}
