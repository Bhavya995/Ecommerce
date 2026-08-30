# E-Commerce Microservices

## Services

| Service | Port | Database |
|---|---:|---|
| User Service | 8081 | user_db |
| Order Service | 8082 | order_db |
| Inventory Service | 8083 | inventory_db |
| Payment Service | 8084 | payment_db |

## URLs

### User Service
http://localhost:8081

### Order Service
http://localhost:8082

### Inventory Service
http://localhost:8083

### Payment Service
http://localhost:8084

## Kafka Topics

- order-created
- inventory-reserved
- inventory-failed
- payment-completed
- payment-failed