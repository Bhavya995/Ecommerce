# Kafka E-Commerce — Completed Order → Inventory Flow

## 1. What we have built

```text
Postman
   ↓
OrderController
   ↓
OrderService.createOrder()
   ↓
OrderCreatedEvent
   ↓
OrderKafkaProducer.sendOrderCreatedEvent()
   ↓
KafkaTemplate.send()
   ↓
Kafka
   ↓
order-created
   ↓
@KafkaListener
   ↓
InventoryKafkaConsumer.consumeOrderCreated()
   ↓
Check Stock
   ├── SUCCESS → InventoryReservedEvent → sendInventoryReserved() → inventory-reserved
   └── FAILURE → InventoryFailedEvent   → sendInventoryFailed()   → inventory-failed
```

The important point:

> Order Service does NOT directly call Inventory Service. Kafka is between them.

---

## 2. Order Service flow

### Step 1 — Postman

Example request:

```json
{
    "userId": 1,
    "productId": 101,
    "quantity": 2
}
```

The request enters:

```text
Postman
   ↓
OrderController
```

### Step 2 — OrderController

The controller calls:

```java
orderService.createOrder(request);
```

Flow:

```text
OrderController
      ↓
OrderService.createOrder()
```

### Step 3 — OrderService

OrderService creates/saves the order and creates:

```text
OrderCreatedEvent
```

Example:

```json
{
    "orderId": 1,
    "userId": 1,
    "productId": 101,
    "quantity": 2
}
```

Then:

```java
orderKafkaProducer.sendOrderCreatedEvent(event);
```

Flow:

```text
OrderService
      ↓
OrderKafkaProducer.sendOrderCreatedEvent()
```

### Step 4 — OrderKafkaProducer

Inside:

```java
kafkaTemplate.send(
    "order-created",
    event.getOrderId().toString(),
    event
);
```

Flow:

```text
OrderKafkaProducer
      ↓
KafkaTemplate.send()
      ↓
Kafka
      ↓
order-created
```

---

# 3. Kafka → Inventory

Kafka now contains the event:

```text
order-created
      ↓
OrderCreatedEvent
```

Inventory Service is listening.

Its listener is:

```java
@KafkaListener(
    topics = "order-created",
    groupId = "inventory-test-group"
)
public void consumeOrderCreated(OrderCreatedEvent event) {
```

Spring Kafka automatically calls:

```java
consumeOrderCreated(event)
```

when a message arrives.

We do NOT manually call this method.

Flow:

```text
Kafka
   ↓
@KafkaListener
   ↓
InventoryKafkaConsumer.consumeOrderCreated()
```

---

# 4. JSON conversion

Producer side:

```text
OrderCreatedEvent
      ↓
JacksonJsonSerializer
      ↓
JSON/bytes
      ↓
Kafka
```

Consumer side:

```text
Kafka
      ↓
ErrorHandlingDeserializer
      ↓
JacksonJsonDeserializer
      ↓
OrderCreatedEvent
      ↓
consumeOrderCreated(event)
```

The consumer is configured to deserialize the incoming event into Inventory Service's local `OrderCreatedEvent`.

---

# 5. Inventory checks stock

Inside:

```java
consumeOrderCreated(OrderCreatedEvent event)
```

we read:

```java
event.getOrderId();
event.getProductId();
event.getQuantity();
```

For our initial test we used:

```java
int availableStock = 4;
```

This is temporary testing logic.

The real database stock logic has NOT been connected yet.

---

# 6. Inventory SUCCESS

Example:

```text
Requested quantity = 2
Available stock    = 4
```

Condition:

```java
if (event.getQuantity() > availableStock)
```

becomes:

```text
2 > 4
```

FALSE.

Therefore inventory succeeds.

Create:

```text
InventoryReservedEvent
```

Then:

```java
inventoryKafkaProducer.sendInventoryReserved(reservedEvent);
```

Flow:

```text
InventoryKafkaConsumer
      ↓
InventoryKafkaProducer.sendInventoryReserved()
      ↓
KafkaTemplate.send()
      ↓
Kafka
      ↓
inventory-reserved
```

---

# 7. Inventory FAILURE

Example:

```text
Requested quantity = 10
Available stock    = 4
```

Condition:

```text
10 > 4
```

TRUE.

Create:

```text
InventoryFailedEvent
```

Example:

```json
{
    "orderId": 1,
    "productId": 101,
    "quantity": 10,
    "reason": "Insufficient stock"
}
```

Then:

```java
inventoryKafkaProducer.sendInventoryFailed(failedEvent);
```

Flow:

```text
InventoryKafkaConsumer
      ↓
InventoryKafkaProducer.sendInventoryFailed()
      ↓
KafkaTemplate.send()
      ↓
Kafka
      ↓
inventory-failed
```

After publishing failure:

```java
return;
```

This prevents the order from also being treated as successfully reserved.

---

# 8. Which class calls which class?

## Order side

```text
OrderController
      ↓
OrderService
      ↓
OrderKafkaProducer
      ↓
KafkaTemplate
```

## Inventory side

Kafka/Spring Kafka triggers:

```text
Kafka
   ↓
InventoryKafkaConsumer
   ↓
consumeOrderCreated()
```

Then the consumer calls:

```text
InventoryKafkaConsumer
      ↓
InventoryKafkaProducer
      ↓
KafkaTemplate
```

So Inventory is BOTH:

```text
Consumer + Producer
```

---

# 9. Complete SUCCESS method flow

```text
Postman
   ↓
OrderController
   ↓
OrderService.createOrder()
   ↓
OrderCreatedEvent
   ↓
OrderKafkaProducer.sendOrderCreatedEvent()
   ↓
KafkaTemplate.send()
   ↓
Kafka
   ↓
order-created
   ↓
@KafkaListener
   ↓
InventoryKafkaConsumer.consumeOrderCreated()
   ↓
Check Stock
   ↓
InventoryReservedEvent
   ↓
InventoryKafkaProducer.sendInventoryReserved()
   ↓
KafkaTemplate.send()
   ↓
Kafka
   ↓
inventory-reserved
```

---

# 10. Complete FAILURE method flow

```text
Postman
   ↓
OrderController
   ↓
OrderService.createOrder()
   ↓
OrderCreatedEvent
   ↓
OrderKafkaProducer.sendOrderCreatedEvent()
   ↓
KafkaTemplate.send()
   ↓
Kafka
   ↓
order-created
   ↓
@KafkaListener
   ↓
InventoryKafkaConsumer.consumeOrderCreated()
   ↓
Check Stock
   ↓
InventoryFailedEvent
   ↓
InventoryKafkaProducer.sendInventoryFailed()
   ↓
KafkaTemplate.send()
   ↓
Kafka
   ↓
inventory-failed
```

---

# 11. Kafka is NOT a normal method call

Normal Java:

```text
OrderService
     ↓
InventoryService.method()
```

Our architecture:

```text
OrderService
     ↓
Kafka
     ↓
InventoryKafkaConsumer
```

Order Service does not directly know which Inventory method executes.

It publishes an event.

---

# 12. Important classes

| Class | Responsibility |
|---|---|
| `OrderController` | Receives HTTP request |
| `OrderService` | Handles order business logic |
| `OrderCreatedEvent` | Represents created order data |
| `OrderKafkaProducer` | Publishes `OrderCreatedEvent` |
| `KafkaTemplate` | Sends Kafka messages |
| `InventoryKafkaConsumer` | Receives/processes `OrderCreatedEvent` |
| `InventoryReservedEvent` | Represents successful stock reservation |
| `InventoryFailedEvent` | Represents inventory failure |
| `InventoryKafkaProducer` | Publishes Inventory events |
| `JacksonJsonSerializer` | Java object → JSON/bytes |
| `JacksonJsonDeserializer` | JSON/bytes → Java object |

---

# 13. Current Kafka topics

```text
__consumer_offsets
order-created
inventory-reserved
inventory-failed
```

### `order-created`

Produced by Order Service.

### `inventory-reserved`

Produced by Inventory Service when stock is successfully reserved.

### `inventory-failed`

Produced by Inventory Service when stock is unavailable.

### `__consumer_offsets`

Kafka internal topic used for consumer offset tracking.

Do NOT delete it.

---

# 14. What we have completed

```text
✅ Kafka Docker setup
✅ order-created topic
✅ inventory-reserved topic
✅ inventory-failed topic
✅ Order Kafka producer
✅ Inventory Kafka consumer
✅ JSON serialization
✅ JSON deserialization
✅ Inventory success event
✅ Inventory failure event
✅ Order → Kafka → Inventory flow
```

---

# 15. What is NOT completed

```text
⬜ Real database stock checking
⬜ Real database stock reservation
⬜ Payment Service
⬜ payment-completed
⬜ payment-failed
⬜ Payment → Order communication
⬜ Order confirmation
⬜ Retry
⬜ Advanced error handling
⬜ Dead Letter Topic
⬜ Idempotency
⬜ Kafka failure scenarios
⬜ Saga pattern
```

---

# 16. Next phase

Payment Service will consume:

```text
inventory-reserved
        ↓
Payment Service
```

Then:

```text
Payment Service
      ↓
Process Payment
      ↓
   ┌──┴──┐
   ↓     ↓
SUCCESS FAILURE
   ↓       ↓
payment-  payment-
completed failed
```

Eventually:

```text
OrderCreated
      ↓
InventoryReserved
      ↓
PaymentCompleted
      ↓
OrderConfirmed
```

---

# 17. Interview answer

### How does Order Service communicate with Inventory Service?

> "When an order is created, the Order Service creates an OrderCreatedEvent and publishes it to the order-created Kafka topic using KafkaTemplate. The Inventory Service listens to that topic using @KafkaListener. It receives the event, checks inventory, and publishes either an InventoryReservedEvent or an InventoryFailedEvent to Kafka."

---

# 18. Final memory picture

```text
             ORDER SERVICE
                  │
                  │ SEND
                  ▼
                KAFKA
                  │
           order-created
                  │
                  │ RECEIVE
                  ▼
          INVENTORY SERVICE
                  │
             Check Stock
              /                     /                  SUCCESS        FAILURE
           ↓              ↓
 InventoryReserved   InventoryFailed
           ↓              ↓
         Kafka            Kafka
           ↓              ↓
inventory-reserved  inventory-failed
```

## Memory trick

```text
KafkaTemplate  = SEND
@KafkaListener = RECEIVE
Topic          = CHANNEL
Event          = DATA
Producer       = SENDS
Consumer       = RECEIVES + PROCESSES
```
