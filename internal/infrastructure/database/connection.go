package database

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/yourusername/hexagon-app/internal/infrastructure/config"
	"go.uber.org/zap"
)

var (
	dbInstance *sql.DB
	dbOnce     sync.Once
	dbErr      error
)

func NewPostgresDB(cfg *config.DatabaseConfig, logger *zap.Logger) (*sql.DB, error) {
	dbOnce.Do(func() {
		dsn := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode,
		)
		dbInstance, dbErr = sql.Open("postgres", dsn)
		if dbErr != nil {
			return
		}
		if err := dbInstance.Ping(); err != nil {
			dbErr = err
			return
		}
		logger.Info("PostgreSQL connected")
	})
	return dbInstance, dbErr
}
