package ports

import "context"

type MessagePublisher interface {
	Publish(ctx context.Context, routingKey string, message interface{}) error
	PublishWithRetry(ctx context.Context, routingKey string, message interface{}, maxRetries int) error
	Close() error
}
