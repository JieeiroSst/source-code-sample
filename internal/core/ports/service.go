package ports

import (
	"context"

	"github.com/google/uuid"
	"github.com/yourusername/hexagon-app/internal/core/domain"
)

type UserService interface {
	CreateUser(ctx context.Context, req domain.CreateUserRequest) (*domain.User, error)
	GetUser(ctx context.Context, id uuid.UUID) (*domain.User, error)
	UpdateUser(ctx context.Context, id uuid.UUID, req domain.UpdateUserRequest) (*domain.User, error)
	DeleteUser(ctx context.Context, id uuid.UUID) error
	ListUsers(ctx context.Context, limit, offset int) ([]*domain.User, error)
}
