package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	kafkapub "github.com/yourusername/hexagon-app/internal/adapters/publisher/kafka"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	kafkaconn "github.com/yourusername/hexagon-app/internal/infrastructure/kafka"
	"go.uber.org/zap"
)

type KafkaConsumer struct {
	reader       *kafka.Reader
	config       *config.KafkaConfig
	logger       *zap.Logger
	handlers     map[string]MessageHandler
	errorHandler *kafkapub.ErrorHandler
	kafkaConn    *kafkaconn.KafkaConnection
}

type MessageHandler func(ctx context.Context, key string, value []byte, headers map[string]string) error

type ConsumeResult struct {
	Success     bool
	Message     kafka.Message
	Error       error
	ProcessTime time.Duration
	RetryCount  int
}

func NewKafkaConsumer(
	kafkaConn *kafkaconn.KafkaConnection,
	cfg *config.KafkaConfig,
	logger *zap.Logger,
) (*KafkaConsumer, error) {
	reader := kafkaConn.CreateReader(cfg.Topic, cfg.ConsumerGroup)
	errorHandler := kafkapub.NewErrorHandler(kafkaConn, cfg, logger)

	return &KafkaConsumer{
		reader:       reader,
		config:       cfg,
		logger:       logger,
		handlers:     make(map[string]MessageHandler),
		errorHandler: errorHandler,
		kafkaConn:    kafkaConn,
	}, nil
}

func (c *KafkaConsumer) RegisterHandler(key string, handler MessageHandler) {
	c.handlers[key] = handler
	c.logger.Info("Kafka handler registered", zap.String("key", key))
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer",
		zap.String("topic", c.config.Topic),
		zap.String("consumer_group", c.config.ConsumerGroup),
	)

	go c.consumeMessages(ctx)

	return nil
}

func (c *KafkaConsumer) consumeMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Kafka consumer stopped")
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				c.logger.Error("Failed to fetch message", zap.Error(err))
				continue
			}

			result := c.processMessage(ctx, msg)
			c.handleConsumeResult(ctx, result)
		}
	}
}

func (c *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) ConsumeResult {
	startTime := time.Now()

	key := string(msg.Key)
	headers := c.extractHeaders(msg.Headers)
	retryCount := c.getRetryCount(headers)

	c.logger.Info("Processing Kafka message",
		zap.String("key", key),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Int("retry_count", retryCount),
	)

	result := ConsumeResult{
		Message:    msg,
		RetryCount: retryCount,
	}

	handler, exists := c.handlers[key]
	if !exists {
		handler, exists = c.handlers["*"]
		if !exists {
			c.logger.Warn("No handler found for key", zap.String("key", key))
			result.Success = false
			result.Error = fmt.Errorf("no handler for key: %s", key)
			result.ProcessTime = time.Since(startTime)
			return result
		}
	}

	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := handler(processCtx, key, msg.Value, headers)
	result.ProcessTime = time.Since(startTime)

	if err != nil {
		result.Success = false
		result.Error = err
		c.logger.Error("Failed to process message",
			zap.String("key", key),
			zap.Int("retry_count", retryCount),
			zap.Duration("process_time", result.ProcessTime),
			zap.Error(err),
		)
		return result
	}

	result.Success = true
	c.logger.Info("Message processed successfully",
		zap.String("key", key),
		zap.Duration("process_time", result.ProcessTime),
	)

	return result
}

func (c *KafkaConsumer) handleConsumeResult(ctx context.Context, result ConsumeResult) {
	if result.Success {
		if err := c.reader.CommitMessages(ctx, result.Message); err != nil {
			c.logger.Error("Failed to commit message",
				zap.String("key", string(result.Message.Key)),
				zap.Error(err),
			)
		}
		return
	}

	if result.RetryCount >= c.config.MaxRetry {
		c.logger.Warn("Max retry exceeded, sending to error handler",
			zap.String("key", string(result.Message.Key)),
			zap.Int("retry_count", result.RetryCount),
		)

		var originalMsg map[string]interface{}
		if err := json.Unmarshal(result.Message.Value, &originalMsg); err != nil {
			originalMsg = map[string]interface{}{
				"raw": string(result.Message.Value),
			}
		}

		if err := c.errorHandler.HandleError(
			ctx,
			string(result.Message.Key),
			originalMsg,
			result.Error,
			result.RetryCount,
		); err != nil {
			c.logger.Error("Failed to handle error", zap.Error(err))
		}

		if err := c.reader.CommitMessages(ctx, result.Message); err != nil {
			c.logger.Error("Failed to commit failed message", zap.Error(err))
		}
		return
	}

	c.logger.Warn("Message will be redelivered",
		zap.String("key", string(result.Message.Key)),
		zap.Int("retry_count", result.RetryCount),
		zap.Int("max_retry", c.config.MaxRetry),
	)

	backoff := time.Duration(result.RetryCount+1) * c.config.RetryBackoff
	time.Sleep(backoff)
}

func (c *KafkaConsumer) extractHeaders(headers []kafka.Header) map[string]string {
	result := make(map[string]string)
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

func (c *KafkaConsumer) getRetryCount(headers map[string]string) int {
	if countStr, exists := headers["retry-count"]; exists {
		if count, err := strconv.Atoi(countStr); err == nil {
			return count
		}
	}
	return 0
}

func (c *KafkaConsumer) Close() error {
	if err := c.reader.Close(); err != nil {
		c.logger.Error("Failed to close Kafka reader", zap.Error(err))
		return err
	}

	if err := c.errorHandler.Close(); err != nil {
		c.logger.Error("Failed to close error handler", zap.Error(err))
		return err
	}

	c.logger.Info("Kafka consumer closed successfully")
	return nil
}

func (c *KafkaConsumer) StartErrorTopicReprocessor(ctx context.Context) error {
	errorReader := c.kafkaConn.CreateReader(c.config.ErrorTopic, c.config.ConsumerGroup+"-error-reprocessor")

	c.logger.Info("Starting error topic reprocessor",
		zap.String("error_topic", c.config.ErrorTopic),
	)

	go func() {
		defer errorReader.Close()

		for {
			select {
			case <-ctx.Done():
				c.logger.Info("Error topic reprocessor stopped")
				return
			default:
				msg, err := errorReader.FetchMessage(ctx)
				if err != nil {
					if err == context.Canceled {
						return
					}
					c.logger.Error("Failed to fetch error message", zap.Error(err))
					continue
				}

				c.reprocessErrorMessage(ctx, msg, errorReader)
			}
		}
	}()

	return nil
}

func (c *KafkaConsumer) reprocessErrorMessage(ctx context.Context, msg kafka.Message, reader *kafka.Reader) {
	var errorMsg kafkapub.ErrorMessage
	if err := json.Unmarshal(msg.Value, &errorMsg); err != nil {
		c.logger.Error("Failed to unmarshal error message", zap.Error(err))
		reader.CommitMessages(ctx, msg)
		return
	}

	c.logger.Info("Reprocessing error message",
		zap.String("original_key", errorMsg.OriginalKey),
		zap.Int("retry_count", errorMsg.RetryCount),
	)

	writer := c.kafkaConn.CreateWriter(c.config.Topic)
	defer writer.Close()

	if err := c.errorHandler.ReprocessFromErrorTopic(ctx, errorMsg, writer); err != nil {
		c.logger.Error("Failed to reprocess error message", zap.Error(err))
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit error message", zap.Error(err))
	}
}
