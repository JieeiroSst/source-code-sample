package fx

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"

	rediscache "github.com/yourusername/hexagon-app/internal/adapters/cache/redis"
	kafkaconsumer "github.com/yourusername/hexagon-app/internal/adapters/consumer/kafka"
	"github.com/yourusername/hexagon-app/internal/adapters/consumer/rabbitmq"
	"github.com/yourusername/hexagon-app/internal/adapters/http/handlers"
	"github.com/yourusername/hexagon-app/internal/adapters/http/middleware"
	kafkapub "github.com/yourusername/hexagon-app/internal/adapters/publisher/kafka"
	rabbitmqpub "github.com/yourusername/hexagon-app/internal/adapters/publisher/rabbitmq"
	"github.com/yourusername/hexagon-app/internal/core/ports"
	"github.com/yourusername/hexagon-app/internal/core/services"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	kafkaconn "github.com/yourusername/hexagon-app/internal/infrastructure/kafka"
	"github.com/yourusername/hexagon-app/pkg/security"
)

// Module defines all application dependencies
var Module = fx.Options(
	// Config - Load configuration first
	fx.Provide(config.LoadConfig),

	// Logger - Initialize logger early
	fx.Provide(NewLogger),

	// Security Components
	fx.Provide(NewJWTService),
	fx.Provide(NewCryptoService),

	// Infrastructure - Database
	fx.Provide(NewDatabase),

	// Infrastructure - Redis
	fx.Provide(NewRedisClient),

	// Infrastructure - RabbitMQ
	fx.Provide(NewRabbitMQConnection),

	// Infrastructure - Kafka
	fx.Provide(NewKafkaConnection),

	// Repositories
	fx.Provide(
		fx.Annotate(
			rediscache.NewCacheRepository,
			fx.As(new(ports.CacheRepository)),
		),
	),
	// TODO: Add PostgreSQL repository when implemented
	// fx.Provide(
	// 	fx.Annotate(
	// 		postgres.NewUserRepository,
	// 		fx.As(new(ports.UserRepository)),
	// 	),
	// ),

	// Message Publishers
	// RabbitMQ Publisher
	fx.Provide(
		fx.Annotate(
			rabbitmqpub.NewPublisher,
			fx.As(new(ports.MessagePublisher)),
			fx.ResultTags(`name:"rabbitmq"`),
		),
	),
	// Kafka Publisher
	fx.Provide(
		fx.Annotate(
			kafkapub.NewKafkaPublisher,
			fx.As(new(ports.MessagePublisher)),
			fx.ResultTags(`name:"kafka"`),
		),
	),

	// Message Consumers
	// RabbitMQ Consumer
	fx.Provide(rabbitmq.NewConsumer),
	fx.Provide(rabbitmq.NewMessageHandlers),
	// Kafka Consumer
	fx.Provide(kafkaconsumer.NewKafkaConsumer),
	fx.Provide(kafkaconsumer.NewKafkaMessageHandlers),

	// Business Services
	fx.Provide(
		fx.Annotate(
			services.NewUserService,
			fx.As(new(ports.UserService)),
		),
	),

	// HTTP Layer
	fx.Provide(NewGinEngine),
	fx.Provide(handlers.NewUserHandler),

	// HTTP Middleware
	fx.Provide(NewAuthMiddleware),
	fx.Provide(NewRateLimiter),
	fx.Provide(NewSecurityMiddleware),

	// Lifecycle Management
	fx.Invoke(RegisterLifecycle),
)

// ==================== Logger ====================

// NewLogger creates a new zap logger instance
func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	var logger *zap.Logger
	var err error

	if cfg.App.Env == "production" {
		// Production logger - JSON format
		logger, err = zap.NewProduction()
	} else {
		// Development logger - Console format with colors
		logger, err = zap.NewDevelopment()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	logger.Info("Logger initialized",
		zap.String("environment", cfg.App.Env),
		zap.String("app_name", cfg.App.Name),
	)

	return logger, nil
}

// ==================== Security ====================

// NewJWTService creates a new JWT service
func NewJWTService(cfg *config.Config) *security.JWTService {
	return security.NewJWTService(cfg.Security.JWTSecret, cfg.Security.JWTExpiry)
}

// NewCryptoService creates a new crypto service
func NewCryptoService(cfg *config.Config) (*security.CryptoService, error) {
	crypto, err := security.NewCryptoService(cfg.Security.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create crypto service: %w", err)
	}
	return crypto, nil
}

// ==================== Database ====================

// NewDatabase creates a new PostgreSQL database connection
func NewDatabase(cfg *config.Config, logger *zap.Logger) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.DBName,
		cfg.Database.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0)

	// Test connection
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Database connected successfully",
		zap.String("host", cfg.Database.Host),
		zap.Int("port", cfg.Database.Port),
		zap.String("database", cfg.Database.DBName),
	)

	return db, nil
}

// ==================== Redis ====================

// NewRedisClient creates a new Redis client
func NewRedisClient(cfg *config.Config, logger *zap.Logger) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     10,
		MinIdleConns: 5,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.Info("Redis connected successfully",
		zap.String("host", cfg.Redis.Host),
		zap.String("port", cfg.Redis.Port),
		zap.Int("db", cfg.Redis.DB),
	)

	return client, nil
}

// ==================== RabbitMQ ====================

// NewRabbitMQConnection creates a new RabbitMQ connection
func NewRabbitMQConnection(cfg *config.Config, logger *zap.Logger) (*amqp.Connection, error) {
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Setup connection close handler
	go func() {
		connErr := <-conn.NotifyClose(make(chan *amqp.Error))
		if connErr != nil {
			logger.Error("RabbitMQ connection closed", zap.Error(connErr))
		}
	}()

	logger.Info("RabbitMQ connected successfully",
		zap.String("url", maskPassword(cfg.RabbitMQ.URL)),
	)

	return conn, nil
}

// ==================== Kafka ====================

// NewKafkaConnection creates a new Kafka connection
func NewKafkaConnection(cfg *config.Config, logger *zap.Logger) (*kafkaconn.KafkaConnection, error) {
	kafkaConn := kafkaconn.NewKafkaConnection(&cfg.Kafka, logger)

	// Ensure topics exist
	ctx := context.Background()
	topics := []string{
		cfg.Kafka.Topic,
		cfg.Kafka.ErrorTopic,
		cfg.Kafka.DeadLetterTopic,
	}

	if err := kafkaConn.EnsureTopics(ctx, topics); err != nil {
		logger.Warn("Failed to ensure Kafka topics (may already exist)", zap.Error(err))
	}

	logger.Info("Kafka connection initialized successfully",
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("consumer_group", cfg.Kafka.ConsumerGroup),
		zap.String("main_topic", cfg.Kafka.Topic),
	)

	return kafkaConn, nil
}

// ==================== HTTP ====================

// NewGinEngine creates a new Gin engine
func NewGinEngine(cfg *config.Config) *gin.Engine {
	// Set Gin mode based on environment
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	engine := gin.New()

	// Use Gin's default recovery middleware
	engine.Use(gin.Recovery())

	// Use custom logger middleware if needed
	if cfg.App.Env != "production" {
		engine.Use(gin.Logger())
	}

	return engine
}

// ==================== HTTP Middleware ====================

// NewAuthMiddleware creates authentication middleware
func NewAuthMiddleware(jwtService *security.JWTService, logger *zap.Logger) *middleware.AuthMiddleware {
	return middleware.NewAuthMiddleware(jwtService, logger)
}

// NewRateLimiter creates rate limiter middleware
func NewRateLimiter(cfg *config.Config, logger *zap.Logger) *middleware.RateLimiter {
	return middleware.NewRateLimiter(
		cfg.Security.RateLimit,
		cfg.Security.RateLimitWindow,
		logger,
	)
}

// NewSecurityMiddleware creates security middleware
func NewSecurityMiddleware(logger *zap.Logger) *middleware.SecurityMiddleware {
	return middleware.NewSecurityMiddleware(logger)
}

// ==================== Helper Functions ====================

// maskPassword masks password in connection string for logging
func maskPassword(url string) string {
	// Simple masking: amqp://user:password@host:port/
	// Replace password with ****
	var result string
	inPassword := false
	for i, char := range url {
		if char == ':' && i > 0 && url[i-1] != '/' {
			inPassword = true
			result += ":"
			continue
		}
		if inPassword && char == '@' {
			inPassword = false
			result += "****@"
			continue
		}
		if !inPassword {
			result += string(char)
		}
	}
	return result
}
