package com.ecommeerce.inventory_service.kafka;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryKafkaProducer {

    private static final String TOPIC = "inventory-reserved";

    private final KafkaTemplate<String, InventoryReservedEvent> kafkaTemplate;

    public InventoryKafkaProducer(
            KafkaTemplate<String, InventoryReservedEvent> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendInventoryReserved(
            InventoryReservedEvent event) {

        kafkaTemplate.send(
                TOPIC,
                event.getOrderId().toString(),
                event
        );

        System.out.println(
                "InventoryReserved event sent for order: "
                        + event.getOrderId()
        );
    }
}
