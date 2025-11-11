# Hexagon Architecture với Go + Uber FX

Source code sample với Hexagon Architecture (Ports & Adapters), sử dụng Uber FX cho Dependency Injection.

## 🏗️ Kiến trúc

### Hexagon Architecture (Clean Architecture)
- **Core (Domain)**: Business logic thuần túy, không phụ thuộc vào framework
- **Ports**: Interfaces định nghĩa contract giữa core và adapters
- **Adapters**: Implementation cụ thể (HTTP, RabbitMQ, Redis, Database)
- **Infrastructure**: Cấu hình, kết nối external services

### Các tính năng chính:
✅ **Hexagon Architecture** - Tách biệt business logic và infrastructure  
✅ **Dependency Injection** - Sử dụng Uber FX  
✅ **RabbitMQ** - Publisher/Consumer với error handling & retry mechanism  
✅ **Redis Cache** - Caching layer  
✅ **Security** - JWT authentication, password hashing, data encryption  
✅ **Rate Limiting** - Bảo vệ API khỏi spam  
✅ **Environment Config** - Đọc từ file .env  

## 🚀 Cài đặt và chạy

### Prerequisites
- Go 1.21+
- PostgreSQL
- Redis
- RabbitMQ

### 1. Cài đặt dependencies
```bash
go mod download
```

### 2. Setup môi trường
Copy file `.env.example` thành `.env` và cấu hình:
```bash
cp .env.example .env
```

### 3. Khởi tạo database
```sql
CREATE DATABASE hexagon_db;

CREATE TABLE users (
    id UUID PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 4. Chạy application
```bash
go run cmd/api/main.go
```

## 📡 API Endpoints

### Public Endpoints
```bash
# Health check
GET /health

# Create user
POST /api/v1/users
{
  "email": "user@example.com",
  "password": "password123",
  "name": "John Doe"
}
```

### Protected Endpoints (Requires JWT Token)
```bash
# Get user by ID
GET /api/v1/users/:id
Authorization: Bearer <token>

# Update user
PUT /api/v1/users/:id
Authorization: Bearer <token>

# Delete user
DELETE /api/v1/users/:id
Authorization: Bearer <token>

# List users
GET /api/v1/users?limit=10&offset=0
Authorization: Bearer <token>
```

## 🔐 Security Features

1. **JWT Authentication** - Token-based authentication
2. **Password Security** - Bcrypt hashing
3. **Data Encryption** - AES-GCM encryption
4. **Rate Limiting** - IP-based rate limiting
5. **Security Headers** - XSS, CSRF protection

## 📨 RabbitMQ Message Flow

1. **Normal Flow**: Message → Handler → Ack
2. **Retry Flow**: Message → Error → Nack + Requeue (max 3 lần)
3. **Dead Letter**: Sau 3 lần retry → Error Queue

## 💾 Redis Cache Strategy

Cache-Aside Pattern:
1. Check cache first
2. If miss → Get from DB → Update cache
3. If hit → Return cached data

## 🐳 Docker Support

```bash
docker-compose up -d
```

## 📄 License
MIT License
