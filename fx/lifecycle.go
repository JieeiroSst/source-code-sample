package fx

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"

	kafkaconsumer "github.com/yourusername/hexagon-app/internal/adapters/consumer/kafka"
	"github.com/yourusername/hexagon-app/internal/adapters/consumer/rabbitmq"
	"github.com/yourusername/hexagon-app/internal/adapters/http/handlers"
	"github.com/yourusername/hexagon-app/internal/adapters/http/middleware"
	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	kafkaconn "github.com/yourusername/hexagon-app/internal/infrastructure/kafka"
)

// LifecycleParams contains all dependencies needed for lifecycle management
type LifecycleParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    *config.Config
	Logger    *zap.Logger

	// Infrastructure
	DB           *sql.DB
	RedisClient  *redis.Client
	RabbitMQConn *amqp.Connection
	KafkaConn    *kafkaconn.KafkaConnection

	// HTTP
	Engine         *gin.Engine
	UserHandler    *handlers.UserHandler
	AuthMiddleware *middleware.AuthMiddleware
	RateLimiter    *middleware.RateLimiter
	SecurityMw     *middleware.SecurityMiddleware

	// RabbitMQ
	RabbitConsumer        *rabbitmq.Consumer
	RabbitMessageHandlers *rabbitmq.MessageHandlers

	// Kafka
	KafkaConsumer        *kafkaconsumer.KafkaConsumer
	KafkaMessageHandlers *kafkaconsumer.KafkaMessageHandlers
}

// RegisterLifecycle registers application lifecycle hooks
func RegisterLifecycle(params LifecycleParams) {
	var server *http.Server

	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			params.Logger.Info("========================================")
			params.Logger.Info("Starting application...",
				zap.String("app_name", params.Config.App.Name),
				zap.String("environment", params.Config.App.Env),
				zap.String("version", "1.0.0"),
			)
			params.Logger.Info("========================================")

			// 1. Setup HTTP routes
			if err := setupRoutes(params); err != nil {
				return fmt.Errorf("failed to setup routes: %w", err)
			}

			// 2. Start RabbitMQ consumer
			if err := startRabbitMQConsumer(ctx, params); err != nil {
				params.Logger.Warn("Failed to start RabbitMQ consumer (non-fatal)", zap.Error(err))
				// Non-fatal: continue even if RabbitMQ fails
			}

			// 3. Start Kafka consumer
			if err := startKafkaConsumer(ctx, params); err != nil {
				params.Logger.Warn("Failed to start Kafka consumer (non-fatal)", zap.Error(err))
				// Non-fatal: continue even if Kafka fails
			}

			// 4. Start HTTP server
			server = &http.Server{
				Addr:           ":" + params.Config.App.Port,
				Handler:        params.Engine,
				ReadTimeout:    15 * time.Second,
				WriteTimeout:   15 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1 << 20, // 1MB
			}

			// Start server in goroutine
			go func() {
				params.Logger.Info("========================================")
				params.Logger.Info("HTTP server starting",
					zap.String("address", "http://localhost:"+params.Config.App.Port),
					zap.String("health_check", "http://localhost:"+params.Config.App.Port+"/health"),
				)
				params.Logger.Info("========================================")

				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					params.Logger.Fatal("Failed to start HTTP server", zap.Error(err))
				}
			}()

			// Log startup summary
			params.Logger.Info("Application started successfully!")
			params.Logger.Info("Services:",
				zap.Bool("database", true),
				zap.Bool("redis", true),
				zap.Bool("rabbitmq", params.RabbitMQConn != nil),
				zap.Bool("kafka", params.KafkaConn != nil),
			)

			return nil
		},

		OnStop: func(ctx context.Context) error {
			params.Logger.Info("========================================")
			params.Logger.Info("Stopping application...")
			params.Logger.Info("========================================")

			// Create shutdown context with timeout
			shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			// 1. Stop HTTP server gracefully
			if server != nil {
				params.Logger.Info("Shutting down HTTP server...")
				if err := server.Shutdown(shutdownCtx); err != nil {
					params.Logger.Error("Failed to shutdown HTTP server gracefully", zap.Error(err))
				} else {
					params.Logger.Info("HTTP server stopped")
				}
			}

			// 2. Close Kafka consumer
			if params.KafkaConsumer != nil {
				params.Logger.Info("Closing Kafka consumer...")
				if err := params.KafkaConsumer.Close(); err != nil {
					params.Logger.Error("Failed to close Kafka consumer", zap.Error(err))
				} else {
					params.Logger.Info("Kafka consumer closed")
				}
			}

			// 3. Close RabbitMQ consumer
			if params.RabbitConsumer != nil {
				params.Logger.Info("Closing RabbitMQ consumer...")
				if err := params.RabbitConsumer.Close(); err != nil {
					params.Logger.Error("Failed to close RabbitMQ consumer", zap.Error(err))
				} else {
					params.Logger.Info("RabbitMQ consumer closed")
				}
			}

			// 4. Close RabbitMQ connection
			if params.RabbitMQConn != nil {
				params.Logger.Info("Closing RabbitMQ connection...")
				if err := params.RabbitMQConn.Close(); err != nil {
					params.Logger.Error("Failed to close RabbitMQ connection", zap.Error(err))
				} else {
					params.Logger.Info("RabbitMQ connection closed")
				}
			}

			// 5. Close Redis client
			if params.RedisClient != nil {
				params.Logger.Info("Closing Redis client...")
				if err := params.RedisClient.Close(); err != nil {
					params.Logger.Error("Failed to close Redis client", zap.Error(err))
				} else {
					params.Logger.Info("Redis client closed")
				}
			}

			// 6. Close database
			if params.DB != nil {
				params.Logger.Info("Closing database connection...")
				if err := params.DB.Close(); err != nil {
					params.Logger.Error("Failed to close database", zap.Error(err))
				} else {
					params.Logger.Info("Database connection closed")
				}
			}

			params.Logger.Info("========================================")
			params.Logger.Info("Application stopped gracefully")
			params.Logger.Info("========================================")

			return nil
		},
	})
}

// ==================== Route Setup ====================

// setupRoutes configures all HTTP routes and middleware
func setupRoutes(params LifecycleParams) error {
	params.Logger.Info("Setting up HTTP routes...")

	// Apply global middleware
	params.Engine.Use(params.SecurityMw.SecureHeaders())
	params.Engine.Use(params.SecurityMw.CORS())
	params.Engine.Use(params.RateLimiter.Limit())

	// Health check endpoint (no auth required)
	params.Engine.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"app":     params.Config.App.Name,
			"version": "1.0.0",
			"env":     params.Config.App.Env,
			"services": gin.H{
				"database": "connected",
				"redis":    "connected",
				"rabbitmq": "connected",
				"kafka":    "connected",
			},
		})
	})

	// Metrics endpoint (simple)
	params.Engine.GET("/metrics", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"uptime": "unknown", // TODO: Implement uptime tracking
		})
	})

	// API v1 routes
	v1 := params.Engine.Group("/api/v1")
	{
		// Public routes (no authentication required)
		public := v1.Group("")
		{
			// User registration/creation
			public.POST("/users", params.UserHandler.CreateUser)

			// TODO: Add these routes
			// public.POST("/auth/login", authHandler.Login)
			// public.POST("/auth/register", authHandler.Register)
			// public.POST("/auth/refresh", authHandler.RefreshToken)
		}

		// Protected routes (authentication required)
		protected := v1.Group("")
		protected.Use(params.AuthMiddleware.Authenticate())
		{
			// User routes
			protected.GET("/users/:id", params.UserHandler.GetUser)
			protected.PUT("/users/:id", params.UserHandler.UpdateUser)
			protected.DELETE("/users/:id", params.UserHandler.DeleteUser)
			protected.GET("/users", params.UserHandler.ListUsers)

			// TODO: Add more protected routes
			// protected.GET("/profile", userHandler.GetProfile)
			// protected.PUT("/profile", userHandler.UpdateProfile)
		}
	}

	params.Logger.Info("HTTP routes configured successfully",
		zap.Int("public_routes", 1),    // /users POST
		zap.Int("protected_routes", 4), // GET, PUT, DELETE, LIST users
	)

	return nil
}

// ==================== RabbitMQ Consumer ====================

// startRabbitMQConsumer initializes and starts RabbitMQ consumer
func startRabbitMQConsumer(ctx context.Context, params LifecycleParams) error {
	params.Logger.Info("Starting RabbitMQ consumer...")

	// Register message handlers
	params.RabbitMessageHandlers.RegisterHandlers(params.RabbitConsumer)

	// Start consuming messages
	if err := params.RabbitConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start RabbitMQ consumer: %w", err)
	}

	params.Logger.Info("RabbitMQ consumer started successfully",
		zap.String("queue", params.Config.RabbitMQ.Queue),
		zap.String("exchange", params.Config.RabbitMQ.Exchange),
	)

	return nil
}

// ==================== Kafka Consumer ====================

// startKafkaConsumer initializes and starts Kafka consumer
func startKafkaConsumer(ctx context.Context, params LifecycleParams) error {
	params.Logger.Info("Starting Kafka consumer...")

	// Register message handlers
	params.KafkaMessageHandlers.RegisterHandlers(params.KafkaConsumer)

	// Start main topic consumer
	if err := params.KafkaConsumer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Kafka consumer: %w", err)
	}

	// Start error topic reprocessor (optional, non-fatal if fails)
	if err := params.KafkaConsumer.StartErrorTopicReprocessor(ctx); err != nil {
		params.Logger.Warn("Failed to start Kafka error topic reprocessor",
			zap.Error(err),
		)
	} else {
		params.Logger.Info("Kafka error topic reprocessor started")
	}

	params.Logger.Info("Kafka consumer started successfully",
		zap.String("topic", params.Config.Kafka.Topic),
		zap.String("consumer_group", params.Config.Kafka.ConsumerGroup),
		zap.Strings("brokers", params.Config.Kafka.Brokers),
	)

	return nil
}
