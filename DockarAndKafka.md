# 🚀 E-Commerce Microservices — Kafka Notes

## 1. Project We Are Building

| Service | Port | Responsibility |
|---|---:|---|
| User Service | 8081 | User operations |
| Order Service | 8082 | Creates orders |
| Inventory Service | 8083 | Checks/reserves stock |
| Payment Service | 8084 | Handles payments |
| Kafka | 9092 | Event communication |

### Current working flow

```text
Postman
   ↓
Order Service :8082
   ↓
Kafka Producer
   ↓
Kafka :9092
   ↓
order-created
   ↓
Kafka Consumer
   ↓
Inventory Service :8083
```

---

# 2. Kafka — Very Simple

Kafka lets one application send an event and another application receive it.

```text
Producer → Topic → Consumer
```

### Memory trick

| Kafka concept | Remember it as |
|---|---|
| Producer | SENDS |
| Topic | STORES / carries |
| Consumer | RECEIVES |
| Broker | Kafka server |
| Partition | Topic split into parts |
| Consumer Group | Consumers working together |
| Offset | Message position |
| Event | Something that happened |

---

# 3. Docker + Kafka

Kafka is running in Docker.

| Item | Value |
|---|---|
| Container | `kafka` |
| Image | `apache/kafka:4.0.0` |
| Port | `9092` |

### Start Kafka

```cmd
cd C:\kafka
docker compose up -d
```

**Why?** Starts Kafka in the background.

### Check Kafka

```cmd
docker ps
```

Expected:

```text
kafka   apache/kafka:4.0.0   Up
```

### Stop Kafka

```cmd
docker compose down
```

---

# 4. Kafka Topic

Our topic:

```text
order-created
```

Create it:

```cmd
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic order-created --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

| Option | Meaning |
|---|---|
| `docker exec kafka` | Run command inside Kafka container |
| `kafka-topics.sh` | Kafka topic tool |
| `--create` | Create topic |
| `--topic order-created` | Topic name |
| `--bootstrap-server localhost:9092` | Kafka broker |
| `--partitions 3` | 3 partitions |
| `--replication-factor 1` | One copy |

List topics:

```cmd
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

# 5. Console Producer and Consumer

## Console Producer

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic order-created --bootstrap-server localhost:9092
```

Then type:

```text
HELLO INVENTORY
```

**Why did we use it?**

To test Kafka independently before connecting the real Order Service.

## Console Consumer

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic order-created --bootstrap-server localhost:9092
```

**Why?**

To watch messages arriving in the topic.

For learning:

```text
Console Producer → Kafka → Console Consumer
```

Later our real consumer became Inventory Service.

---

# 6. Order Service — Kafka Producer

We already created this producer:

```java
@Service
public class OrderKafkaProducer {

    private static final String TOPIC = "order-created";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderKafkaProducer(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderCreatedEvent(OrderCreatedEvent event) {

        kafkaTemplate.send(
                TOPIC,
                event.getOrderId().toString(),
                event
        );

        System.out.println(
                "OrderCreated event sent for order: "
                        + event.getOrderId());
    }
}
```

---

# 7. Producer — Annotation and Code Explanation

## `@Service`

```java
@Service
```

Tells Spring to create and manage this class as a bean.

**Why?** So our Order Service can inject and use the producer.

---

## `private static final`

```java
private static final String TOPIC = "order-created";
```

Stores the topic name in one place.

---

## `KafkaTemplate`

```java
KafkaTemplate<String, OrderCreatedEvent>
```

Spring Kafka's main helper for **sending messages**.

Think:

```text
KafkaTemplate = SEND button for Kafka
```

Here:

```text
String = message key
OrderCreatedEvent = message value
```

---

## Constructor Injection

```java
public OrderKafkaProducer(
        KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate)
```

Spring injects the `KafkaTemplate`.

---

## `send()`

```java
kafkaTemplate.send(TOPIC, key, event);
```

Meaning:

```text
send(
    topic,
    key,
    value
)
```

Example:

```text
topic = order-created
key   = "101"
value = OrderCreatedEvent
```

---

# 8. Why Do We Use a Kafka Key?

We used:

```java
event.getOrderId().toString()
```

as the key.

If:

```text
orderId = 101
```

then:

```text
key = "101"
```

Kafka can use the key for partition selection.

### Memory trick

```text
KEY   → Partition routing
VALUE → Actual data
```

---

# 9. OrderCreatedEvent

This represents:

> "An order has been created."

Example:

```json
{
  "orderId": 1,
  "userId": 1,
  "productId": 101,
  "quantity": 2
}
```

The event is the data travelling through Kafka.

---

# 10. Inventory Kafka Consumer

Our working consumer is:

```java
@Service
public class InventoryKafkaConsumer {

    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-test-group"
    )
    public void consumeOrderCreated(String message) {

        System.out.println("=================================");
        System.out.println("MESSAGE RECEIVED FROM KAFKA:");
        System.out.println(message);
        System.out.println("=================================");
    }
}
```

---

# 11. Consumer — Annotation Explanation

## `@Service`

```java
@Service
```

Registers the consumer as a Spring bean.

---

## `@KafkaListener`

```java
@KafkaListener(
    topics = "order-created",
    groupId = "inventory-test-group"
)
```

Tells Spring:

> Listen to `order-created` and call this method when a message arrives.

---

## `topics`

```java
topics = "order-created"
```

Specifies which Kafka topic to listen to.

---

## `groupId`

```java
groupId = "inventory-test-group"
```

Identifies the consumer group.

Consumer groups allow multiple consumer instances to share partitions.

---

# 12. Consumer Groups

Example:

```text
order-created
 ├── Partition 0
 ├── Partition 1
 └── Partition 2

inventory-test-group
 ├── Consumer 1 → Partition 0
 ├── Consumer 2 → Partition 1
 └── Consumer 3 → Partition 2
```

Within one consumer group, a partition is consumed by only one consumer at a time.

---

# 13. `KafkaConsumerConfig`

We had an error:

```text
A component required a bean named
'kafkaListenerContainerFactory' that could not be found.
```

So we explicitly created the Kafka consumer configuration.

```java
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "inventory-test-group"
        );

        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}
```

---

# 14. `@Configuration`

```java
@Configuration
```

Means:

> This class contains Spring configuration/bean definitions.

---

# 15. `@Bean`

```java
@Bean
```

Means:

> Create this object and let Spring manage it.

We used it for:

```text
ConsumerFactory
kafkaListenerContainerFactory
```

---

# 16. `ConsumerFactory`

```java
ConsumerFactory<String, String>
```

Creates/configures Kafka consumers.

Simple flow:

```text
ConsumerFactory
      ↓
Kafka Consumer
      ↓
Kafka Broker
```

---

# 17. `DefaultKafkaConsumerFactory`

```java
new DefaultKafkaConsumerFactory<>(config)
```

Standard Spring Kafka implementation for creating consumers from configuration.

---

# 18. `ConcurrentKafkaListenerContainerFactory`

```java
ConcurrentKafkaListenerContainerFactory
```

Creates/manages the listener container used by `@KafkaListener`.

Simple flow:

```text
@KafkaListener
      ↓
Listener Container
      ↓
Kafka Consumer
      ↓
Kafka
```

---

# 19. `@EnableKafka`

We also used:

```java
@EnableKafka
@SpringBootApplication
public class InventoryServiceApplication {
```

**Why?**

It enables Spring Kafka listener infrastructure so Spring can process:

```java
@KafkaListener
```

Memory:

```text
@EnableKafka
      ↓
Kafka listener support enabled
      ↓
@KafkaListener works
```

---

# 20. Serializer vs Deserializer

Kafka transfers data as bytes.

### Serializer

```text
Java object → bytes
```

Used when **sending**.

### Deserializer

```text
bytes → Java object
```

Used when **receiving**.

### Memory trick

```text
SERIALIZE   = SEND OUT
DESERIALIZE = RECEIVE IN
```

For our simple test:

```java
StringDeserializer.class
```

turns Kafka message data into a Java `String`.

---

# 21. Important Spring Kafka Classes

| Class | Purpose |
|---|---|
| `KafkaTemplate` | Send messages |
| `ConsumerFactory` | Create/configure consumers |
| `DefaultKafkaConsumerFactory` | Standard consumer factory |
| `ConcurrentKafkaListenerContainerFactory` | Manage listener containers |
| `ConsumerConfig` | Consumer configuration constants |
| `StringDeserializer` | Kafka bytes → String |
| `StringSerializer` | String → Kafka bytes |

---

# 22. Important Kafka Methods / APIs We Used

## `KafkaTemplate.send()`

```java
kafkaTemplate.send(topic, key, value);
```

Sends a message.

---

## `setConsumerFactory()`

```java
factory.setConsumerFactory(consumerFactory());
```

Connects the listener factory to the consumer factory.

---

## Kafka CLI topic commands

Create:

```cmd
kafka-topics.sh --create
```

List:

```cmd
kafka-topics.sh --list
```

---

## Console producer

```cmd
kafka-console-producer.sh
```

Manually sends messages.

---

## Console consumer

```cmd
kafka-console-consumer.sh
```

Manually reads messages.

---

## Consumer groups

```cmd
kafka-consumer-groups.sh
```

Used to inspect consumer groups.

---

# 23. Complete Code Flow — Line by Line

### 1. User sends request

```text
Postman
   ↓
POST /api/orders
```

### 2. Order Controller receives request

```text
OrderController
   ↓
OrderService
```

### 3. Order Service saves order

```text
OrderService
   ↓
MySQL
```

### 4. Order Service creates event

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

### 5. Producer sends event

```java
orderKafkaProducer.sendOrderCreatedEvent(event);
```

Inside:

```java
kafkaTemplate.send(
    "order-created",
    event.getOrderId().toString(),
    event
);
```

### 6. Kafka receives it

```text
Kafka :9092
   ↓
order-created
```

### 7. Inventory listener receives it

```java
@KafkaListener(
    topics = "order-created",
    groupId = "inventory-test-group"
)
```

### 8. Spring calls the method

```java
consumeOrderCreated(message)
```

### 9. Inventory processes the event

Currently we tested by printing the message.

Later it will check and reserve stock.

---

# 24. Complete Architecture

```text
                  POST
                   ↓
               ┌───────┐
               │Postman│
               └───┬───┘
                   ↓
          ┌─────────────────┐
          │  Order Service  │
          │     :8082       │
          └────────┬────────┘
                   ↓
          OrderKafkaProducer
                   ↓
          ┌─────────────────┐
          │   Kafka :9092   │
          │  order-created  │
          └────────┬────────┘
                   ↓
          @KafkaListener
                   ↓
       ┌─────────────────────┐
       │ Inventory Service   │
       │       :8083         │
       └─────────────────────┘
```

---

# 25. What We Tested

| Test | Result |
|---|:---:|
| Kafka Docker container | ✅ |
| `order-created` topic | ✅ |
| Console Producer | ✅ |
| Console Consumer | ✅ |
| Kafka → Inventory test | ✅ |
| Order Service Kafka Producer | ✅ |
| Order → Kafka → Inventory | ✅ |

### Final working flow

```text
Postman
   ↓
Order Service
   ↓
OrderKafkaProducer
   ↓
Kafka
   ↓
order-created
   ↓
InventoryKafkaConsumer
   ↓
Inventory Service
```

---

# 26. Why Kafka Instead of Direct REST?

Without Kafka:

```text
Order Service ─────HTTP────→ Inventory Service
```

With Kafka:

```text
Order Service
      ↓
    Kafka
      ↓
Inventory Service
```

### Benefits

- Loose coupling
- Asynchronous communication
- Services can scale independently
- Multiple services can consume an event
- Kafka retains events
- Good for event-driven architecture

---

# 27. Interview Answer — What Did You Implement?

> "In our microservices project, we use Kafka for asynchronous event-driven communication. When an order is created, the Order Service publishes an OrderCreatedEvent to the order-created topic using Spring Kafka's KafkaTemplate. The Inventory Service consumes the event using @KafkaListener with a consumer group. This decouples the Order and Inventory services and allows asynchronous communication."

---

# 28. Interview — What Is `@KafkaListener`?

> "`@KafkaListener` is a Spring Kafka annotation that marks a method as a Kafka message listener. It specifies the topic and consumer group, and Spring invokes the method whenever a message is received."

---

# 29. Interview — What Is `KafkaTemplate`?

> "`KafkaTemplate` is Spring Kafka's abstraction for publishing messages to Kafka. We use its send method to send an event to a particular topic, optionally using a key."

---

# 30. Producer vs Consumer

| Producer | Consumer |
|---|---|
| Sends data | Receives data |
| Uses `KafkaTemplate` | Uses `@KafkaListener` |
| Order Service | Inventory Service |
| Publishes events | Processes events |

---

# 31. ⭐ Daily Kafka Commands

| Purpose | Command |
|---|---|
| Go to Kafka | `cd C:\kafka` |
| Start Kafka | `docker compose up -d` |
| Check Docker | `docker ps` |
| List topics | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092` |
| Stop Kafka | `docker compose down` |

---

# 32. 🚀 Next Phase

The next flow we will build:

```text
Order Service
      ↓
OrderCreated
      ↓
Kafka
      ↓
Inventory Service
      ↓
Check stock
      ↓
Reserve stock
      ↓
InventoryReserved
      ↓
Kafka
      ↓
Payment Service
```

Then:

```text
Payment Service
      ↓
PaymentCompleted
      ↓
Kafka
      ↓
Order Service
      ↓
Order Confirmed
```

Eventually:

```text
ORDER CREATED
      ↓
INVENTORY RESERVED
      ↓
PAYMENT COMPLETED
      ↓
ORDER CONFIRMED
```

This will lead into:

- Consumer groups
- Partitions
- Offsets
- Retry
- Error handling
- Dead Letter Topic
- Kafka failure scenarios
- Idempotency
- Saga pattern
- Real microservice interview scenarios

---

# ❤️ Final Memory Picture

```text
              PRODUCER
           Order Service
                │
                │ SEND
                ▼
              KAFKA
                │
                │ TOPIC
                ▼
         order-created
                │
                │ RECEIVE
                ▼
              CONSUMER
        Inventory Service
```

### Remember these 4 words:

```text
ORDER SERVICE = PRODUCER
KAFKA         = MESSENGER
TOPIC         = EVENT CHANNEL
INVENTORY     = CONSUMER
```

**This is the Kafka foundation of our project.**
