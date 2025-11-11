package rabbitmq

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

var (
	conn     *amqp.Connection
	connErr  error
	connOnce sync.Once
)

func NewRabbitMQConnection(cfg *config.RabbitMQConfig, logger *zap.Logger) (*amqp.Connection, error) {
	connOnce.Do(func() {
		c, err := amqp.Dial(cfg.URL)
		if err != nil {
			connErr = fmt.Errorf("failed to connect to RabbitMQ: %w", err)
			logger.Error("failed to connect to RabbitMQ", zap.Error(connErr))
			return
		}
		conn = c
		logger.Info("RabbitMQ connected")
	})
	return conn, connErr
}
