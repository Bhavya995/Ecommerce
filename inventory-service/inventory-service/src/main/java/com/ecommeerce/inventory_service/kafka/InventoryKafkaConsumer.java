package com.ecommeerce.inventory_service.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryKafkaConsumer {

    private final InventoryKafkaProducer inventoryKafkaProducer;

    public InventoryKafkaConsumer(
            InventoryKafkaProducer inventoryKafkaProducer) {

        this.inventoryKafkaProducer = inventoryKafkaProducer;
    }

    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-test-group"
    )
    public void consumeOrderCreated(OrderCreatedEvent event) {

        System.out.println("=================================");
        System.out.println("ORDER RECEIVED FROM KAFKA");
        System.out.println("=================================");

        System.out.println(
                "Order ID: " + event.getOrderId()
        );

        System.out.println(
                "Product ID: " + event.getProductId()
        );

        System.out.println(
                "Quantity: " + event.getQuantity()
        );

        // Temporary stock for testing
        int availableStock = 4;

        // =================================
        // INVENTORY FAILED
        // =================================

        if (event.getQuantity() > availableStock) {

            System.out.println(
                    "❌ INSUFFICIENT STOCK"
            );

            InventoryFailedEvent failedEvent =
                    new InventoryFailedEvent(
                            event.getOrderId(),
                            event.getProductId(),
                            event.getQuantity(),
                            "Insufficient stock"
                    );

            inventoryKafkaProducer.sendInventoryFailed(
                    failedEvent
            );

            return;
        }

        // =================================
        // INVENTORY RESERVED
        // =================================

        System.out.println(
                "✅ Stock available"
        );

        InventoryReservedEvent reservedEvent =
                new InventoryReservedEvent(
                        event.getOrderId(),
                        event.getProductId(),
                        event.getQuantity()
                );

        inventoryKafkaProducer.sendInventoryReserved(
                reservedEvent
        );

        System.out.println(
                "✅ Inventory reserved successfully"
        );
    }
}
