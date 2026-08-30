package com.ecommeerce.inventory_service.service;

import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

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
