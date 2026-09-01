package com.ecommeerce.order_service.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryReleaseKafkaProducer {

    private final KafkaTemplate<String, InventoryReleaseEvent>
            kafkaTemplate;

    public InventoryReleaseKafkaProducer(
            KafkaTemplate<String, InventoryReleaseEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendInventoryRelease(
            InventoryReleaseEvent event) {

        kafkaTemplate.send(
                "inventory-release",
                event.getOrderId().toString(),
                event
        );

        System.out.println(
                "Inventory release event sent for order: "
                        + event.getOrderId()
        );
    }
}