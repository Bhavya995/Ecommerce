package com.ecommeerce.order_service.service;

import com.ecommeerce.order_service.dto.OrderCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderKafkaProducer {

    private static final String TOPIC = "order-created";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderKafkaProducer(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderCreatedEvent(OrderCreatedEvent event) {

        kafkaTemplate.send(TOPIC, event.getOrderId().toString(), event);

        System.out.println(
                "OrderCreated event sent for order: "
                        + event.getOrderId());
    }
}
