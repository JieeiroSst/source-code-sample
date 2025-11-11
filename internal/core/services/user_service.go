package services

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yourusername/hexagon-app/internal/core/domain"
	"github.com/yourusername/hexagon-app/internal/core/ports"
	"github.com/yourusername/hexagon-app/pkg/security"
	"go.uber.org/zap"
)

type userService struct {
	repo      ports.UserRepository
	cache     ports.CacheRepository
	publisher ports.MessagePublisher
	logger    *zap.Logger
	crypto    *security.CryptoService
}

func NewUserService(
	repo ports.UserRepository,
	cache ports.CacheRepository,
	publisher ports.MessagePublisher,
	logger *zap.Logger,
	crypto *security.CryptoService,
) ports.UserService {
	return &userService{
		repo:      repo,
		cache:     cache,
		publisher: publisher,
		logger:    logger,
		crypto:    crypto,
	}
}

func (s *userService) CreateUser(ctx context.Context, req domain.CreateUserRequest) (*domain.User, error) {
	existingUser, _ := s.repo.GetByEmail(ctx, req.Email)
	if existingUser != nil {
		return nil, domain.ErrUserAlreadyExists
	}

	hashedPassword, err := s.crypto.HashPassword(req.Password)
	if err != nil {
		s.logger.Error("Failed to hash password", zap.Error(err))
		return nil, err
	}

	user := &domain.User{
		ID:        uuid.New(),
		Email:     req.Email,
		Password:  hashedPassword,
		Name:      req.Name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.repo.Create(ctx, user); err != nil {
		s.logger.Error("Failed to create user", zap.Error(err))
		return nil, err
	}

	cacheKey := fmt.Sprintf("user:%s", user.ID.String())
	if err := s.cache.Set(ctx, cacheKey, user); err != nil {
		s.logger.Warn("Failed to cache user", zap.Error(err))
	}

	event := domain.UserCreatedEvent{
		UserID:    user.ID,
		Email:     user.Email,
		Name:      user.Name,
		Timestamp: time.Now(),
	}
	
	if err := s.publisher.PublishWithRetry(ctx, "user.created", event, 3); err != nil {
		s.logger.Error("Failed to publish user created event", zap.Error(err))
	}

	s.logger.Info("User created successfully", zap.String("user_id", user.ID.String()))
	return user, nil
}

func (s *userService) GetUser(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	cacheKey := fmt.Sprintf("user:%s", id.String())
	var user domain.User
	
	err := s.cache.Get(ctx, cacheKey, &user)
	if err == nil {
		s.logger.Info("User found in cache", zap.String("user_id", id.String()))
		return &user, nil
	}

	dbUser, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(ctx, cacheKey, dbUser); err != nil {
		s.logger.Warn("Failed to cache user", zap.Error(err))
	}

	return dbUser, nil
}

func (s *userService) UpdateUser(ctx context.Context, id uuid.UUID, req domain.UpdateUserRequest) (*domain.User, error) {
	user, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if req.Email != "" {
		user.Email = req.Email
	}
	if req.Name != "" {
		user.Name = req.Name
	}
	user.UpdatedAt = time.Now()

	if err := s.repo.Update(ctx, user); err != nil {
		return nil, err
	}

	cacheKey := fmt.Sprintf("user:%s", id.String())
	if err := s.cache.Set(ctx, cacheKey, user); err != nil {
		s.logger.Warn("Failed to update cache", zap.Error(err))
	}

	event := domain.UserUpdatedEvent{
		UserID:    user.ID,
		Email:     user.Email,
		Name:      user.Name,
		Timestamp: time.Now(),
	}
	
	if err := s.publisher.PublishWithRetry(ctx, "user.updated", event, 3); err != nil {
		s.logger.Error("Failed to publish user updated event", zap.Error(err))
	}

	return user, nil
}

func (s *userService) DeleteUser(ctx context.Context, id uuid.UUID) error {
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}

	cacheKey := fmt.Sprintf("user:%s", id.String())
	if err := s.cache.Delete(ctx, cacheKey); err != nil {
		s.logger.Warn("Failed to delete from cache", zap.Error(err))
	}

	return nil
}

func (s *userService) ListUsers(ctx context.Context, limit, offset int) ([]*domain.User, error) {
	return s.repo.List(ctx, limit, offset)
}
