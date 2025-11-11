package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	App      AppConfig
	Database DatabaseConfig
	RabbitMQ RabbitMQConfig
	Redis    RedisConfig
	Security SecurityConfig
	Kafka    KafkaConfig
}

type AppConfig struct {
	Name string
	Env  string
	Port string
}

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type RabbitMQConfig struct {
	URL                string
	Exchange           string
	Queue              string
	ErrorQueue         string
	DeadLetterExchange string
	MaxRetry           int
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
	DB       int
	CacheTTL time.Duration
}

type SecurityConfig struct {
	JWTSecret       string
	JWTExpiry       time.Duration
	APIKey          string
	RateLimit       int
	RateLimitWindow time.Duration
	EncryptionKey   string
}

type KafkaConfig struct {
	Brokers           []string
	ConsumerGroup     string
	Topic             string
	ErrorTopic        string
	DeadLetterTopic   string
	MaxRetry          int
	RetryBackoff      time.Duration
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	AutoCommit        bool
}

func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		fmt.Println("Warning: .env file not found, using environment variables")
	}

	jwtExpiry, err := time.ParseDuration(getEnv("JWT_EXPIRY", "24h"))
	if err != nil {
		jwtExpiry = 24 * time.Hour
	}

	rateLimitWindow, err := time.ParseDuration(getEnv("RATE_LIMIT_WINDOW", "1m"))
	if err != nil {
		rateLimitWindow = 1 * time.Minute
	}

	cacheTTL, err := time.ParseDuration(getEnv("REDIS_CACHE_TTL", "3600s"))
	if err != nil {
		cacheTTL = 1 * time.Hour
	}

	config := &Config{
		App: AppConfig{
			Name: getEnv("APP_NAME", "hexagon-app"),
			Env:  getEnv("APP_ENV", "development"),
			Port: getEnv("APP_PORT", "8080"),
		},
		Database: DatabaseConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvAsInt("DB_PORT", 5432),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			DBName:   getEnv("DB_NAME", "hexagon_db"),
			SSLMode:  getEnv("DB_SSL_MODE", "disable"),
		},
		RabbitMQ: RabbitMQConfig{
			URL:                getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Exchange:           getEnv("RABBITMQ_EXCHANGE", "hexagon_exchange"),
			Queue:              getEnv("RABBITMQ_QUEUE", "hexagon_queue"),
			ErrorQueue:         getEnv("RABBITMQ_ERROR_QUEUE", "hexagon_error_queue"),
			DeadLetterExchange: getEnv("RABBITMQ_DEAD_LETTER_EXCHANGE", "hexagon_dlx"),
			MaxRetry:           getEnvAsInt("RABBITMQ_MAX_RETRY", 3),
		},
		Redis: RedisConfig{
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnv("REDIS_PORT", "6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvAsInt("REDIS_DB", 0),
			CacheTTL: cacheTTL,
		},
		Security: SecurityConfig{
			JWTSecret:       getEnv("JWT_SECRET", "your-secret-key"),
			JWTExpiry:       jwtExpiry,
			APIKey:          getEnv("API_KEY", "your-api-key"),
			RateLimit:       getEnvAsInt("RATE_LIMIT", 100),
			RateLimitWindow: rateLimitWindow,
			EncryptionKey:   getEnv("ENCRYPTION_KEY", "32-character-encryption-key!!"),
		},
		Kafka: KafkaConfig{
			Brokers:           getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			ConsumerGroup:     getEnv("KAFKA_CONSUMER_GROUP", "hexagon-consumer-group"),
			Topic:             getEnv("KAFKA_TOPIC", "hexagon-events"),
			ErrorTopic:        getEnv("KAFKA_ERROR_TOPIC", "hexagon-events-error"),
			DeadLetterTopic:   getEnv("KAFKA_DEAD_LETTER_TOPIC", "hexagon-events-dlq"),
			MaxRetry:          getEnvAsInt("KAFKA_MAX_RETRY", 3),
			RetryBackoff:      getEnvAsDuration("KAFKA_RETRY_BACKOFF", "2s"),
			SessionTimeout:    getEnvAsDuration("KAFKA_SESSION_TIMEOUT", "10s"),
			HeartbeatInterval: getEnvAsDuration("KAFKA_HEARTBEAT_INTERVAL", "3s"),
			AutoCommit:        getEnvAsBool("KAFKA_AUTO_COMMIT", false),
		},
	}

	return config, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsSlice(key string, defaultValue []string) []string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return splitString(value, ",")
}

func splitString(s string, sep string) []string {
	var result []string
	for _, part := range splitByRune(s, ',') {
		trimmed := trimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func splitByRune(s string, r rune) []string {
	var result []string
	var current string
	for _, c := range s {
		if c == r {
			result = append(result, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	result = append(result, current)
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}

func getEnvAsDuration(key string, defaultValue string) time.Duration {
	value := getEnv(key, defaultValue)
	duration, err := time.ParseDuration(value)
	if err != nil {
		duration, _ = time.ParseDuration(defaultValue)
	}
	return duration
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
