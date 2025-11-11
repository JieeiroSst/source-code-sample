package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

type Consumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	config   *config.RabbitMQConfig
	logger   *zap.Logger
	handlers map[string]MessageHandler
}

type MessageHandler func(ctx context.Context, body []byte, headers amqp.Table) error

func NewConsumer(
	conn *amqp.Connection,
	cfg *config.RabbitMQConfig,
	logger *zap.Logger,
) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	consumer := &Consumer{
		conn:     conn,
		channel:  ch,
		config:   cfg,
		logger:   logger,
		handlers: make(map[string]MessageHandler),
	}

	if err := consumer.declareQueue(); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (c *Consumer) declareQueue() error {
	args := amqp.Table{
		"x-dead-letter-exchange":    c.config.DeadLetterExchange,
		"x-dead-letter-routing-key": c.config.ErrorQueue,
	}

	_, err := c.channel.QueueDeclare(
		c.config.Queue,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	patterns := []string{"user.*", "order.*"} 
	for _, pattern := range patterns {
		if err := c.channel.QueueBind(
			c.config.Queue,
			pattern,
			c.config.Exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind queue: %w", err)
		}
	}

	return nil
}

func (c *Consumer) RegisterHandler(routingKey string, handler MessageHandler) {
	c.handlers[routingKey] = handler
	c.logger.Info("Handler registered", zap.String("routing_key", routingKey))
}

func (c *Consumer) Start(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		c.config.Queue,
		"",    // consumer tag (empty = auto-generated)
		false, // auto-ack (false = manual ack for reliability)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	c.logger.Info("Consumer started successfully",
		zap.String("queue", c.config.Queue),
		zap.Int("max_retry", c.config.MaxRetry),
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.logger.Info("Consumer context cancelled, stopping...")
				return

			case msg, ok := <-msgs:
				if !ok {
					c.logger.Warn("Message channel closed, attempting to reconnect...")
					if err := c.reconnect(ctx); err != nil {
						c.logger.Error("Failed to reconnect consumer", zap.Error(err))
						return
					}
					continue
				}

				go c.handleMessage(ctx, msg)
			}
		}
	}()

	return nil
}

func (c *Consumer) reconnect(ctx context.Context) error {
	c.logger.Info("Attempting to reconnect consumer...")

	if c.channel != nil {
		c.channel.Close()
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open new channel: %w", err)
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	c.channel = ch

	if err := c.declareQueue(); err != nil {
		return fmt.Errorf("failed to redeclare queue: %w", err)
	}

	return c.Start(ctx)
}

func (c *Consumer) handleMessage(ctx context.Context, msg amqp.Delivery) {
	startTime := time.Now()
	routingKey := msg.RoutingKey
	retryCount := c.getRetryCount(msg.Headers)
	messageID := c.getMessageID(msg.Headers)

	c.logger.Info("Processing message",
		zap.String("routing_key", routingKey),
		zap.String("message_id", messageID),
		zap.Int("retry_count", retryCount),
		zap.Int64("body_size", int64(len(msg.Body))),
	)

	handler, exists := c.handlers[routingKey]
	if !exists {
		c.logger.Warn("No handler registered for routing key",
			zap.String("routing_key", routingKey),
			zap.String("message_id", messageID),
		)
		if err := msg.Reject(false); err != nil {
			c.logger.Error("Failed to reject message", zap.Error(err))
		}
		return
	}

	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := c.executeHandler(processCtx, handler, msg)

	if err != nil {
		c.handleError(msg, err, retryCount, messageID)
		return
	}

	if err := msg.Ack(false); err != nil {
		c.logger.Error("Failed to acknowledge message",
			zap.String("message_id", messageID),
			zap.Error(err),
		)
		return
	}

	duration := time.Since(startTime)
	c.logger.Info("Message processed successfully",
		zap.String("routing_key", routingKey),
		zap.String("message_id", messageID),
		zap.Duration("duration", duration),
	)
}

func (c *Consumer) executeHandler(ctx context.Context, handler MessageHandler, msg amqp.Delivery) error {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic recovered in message handler",
				zap.String("routing_key", msg.RoutingKey),
				zap.Any("panic", r),
			)
		}
	}()

	return handler(ctx, msg.Body, msg.Headers)
}

func (c *Consumer) handleError(msg amqp.Delivery, err error, retryCount int, messageID string) {
	c.logger.Error("Failed to process message",
		zap.String("routing_key", msg.RoutingKey),
		zap.String("message_id", messageID),
		zap.Int("retry_count", retryCount),
		zap.Error(err),
	)

	if retryCount >= c.config.MaxRetry {
		c.logger.Warn("Max retries reached, sending to dead letter queue",
			zap.String("routing_key", msg.RoutingKey),
			zap.String("message_id", messageID),
			zap.Int("total_retries", retryCount),
		)

		if err := msg.Reject(false); err != nil {
			c.logger.Error("Failed to reject message to DLX",
				zap.String("message_id", messageID),
				zap.Error(err),
			)
		}
		return
	}

	delay := c.calculateBackoffDelay(retryCount)

	c.logger.Info("Requeuing message with backoff",
		zap.String("routing_key", msg.RoutingKey),
		zap.String("message_id", messageID),
		zap.Int("retry_count", retryCount),
		zap.Int("next_retry", retryCount+1),
		zap.Duration("backoff_delay", delay),
	)

	updatedHeaders := c.updateRetryHeaders(msg.Headers, retryCount+1, delay)

	if err := c.republishWithDelay(msg, updatedHeaders, delay); err != nil {
		c.logger.Error("Failed to republish message, using nack",
			zap.String("message_id", messageID),
			zap.Error(err),
		)

		if nackErr := msg.Nack(false, true); nackErr != nil {
			c.logger.Error("Failed to nack message",
				zap.String("message_id", messageID),
				zap.Error(nackErr),
			)
		}
		return
	}

	if err := msg.Ack(false); err != nil {
		c.logger.Error("Failed to ack message after republish",
			zap.String("message_id", messageID),
			zap.Error(err),
		)
	}
}

func (c *Consumer) calculateBackoffDelay(retryCount int) time.Duration {
	baseDelay := time.Duration(1<<uint(retryCount)) * time.Second
	maxDelay := 5 * time.Minute

	if baseDelay > maxDelay {
		return maxDelay
	}
	return baseDelay
}

func (c *Consumer) updateRetryHeaders(headers amqp.Table, newRetryCount int, delay time.Duration) amqp.Table {
	if headers == nil {
		headers = amqp.Table{}
	}

	headers["x-retry-count"] = int32(newRetryCount)
	headers["x-retry-timestamp"] = time.Now().Unix()
	headers["x-backoff-delay-ms"] = int64(delay.Milliseconds())

	return headers
}

func (c *Consumer) republishWithDelay(msg amqp.Delivery, headers amqp.Table, delay time.Duration) error {
	time.Sleep(delay)

	return c.channel.Publish(
		c.config.Exchange,
		msg.RoutingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  msg.ContentType,
			Body:         msg.Body,
			DeliveryMode: msg.DeliveryMode,
			Priority:     msg.Priority,
			Timestamp:    time.Now(),
			Headers:      headers,
		},
	)
}

func (c *Consumer) getRetryCount(headers amqp.Table) int {
	if count, ok := headers["x-retry-count"].(int32); ok {
		return int(count)
	}
	if count, ok := headers["x-retry-count"].(int); ok {
		return count
	}
	return 0
}

func (c *Consumer) getMessageID(headers amqp.Table) string {
	if msgID, ok := headers["x-message-id"].(string); ok {
		return msgID
	}
	return fmt.Sprintf("msg-%d", time.Now().UnixNano())
}

func (c *Consumer) Close() error {
	if err := c.channel.Close(); err != nil {
		return err
	}
	return nil
}
