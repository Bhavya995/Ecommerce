package com.ecommeerce.inventory_service.kafka;

import com.ecommeerce.inventory_service.service.ProductService;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryKafkaConsumer {

    private final InventoryKafkaProducer inventoryKafkaProducer;
    private final ProductService productService;

    public InventoryKafkaConsumer(
            InventoryKafkaProducer inventoryKafkaProducer,
            ProductService productService) {

        this.inventoryKafkaProducer = inventoryKafkaProducer;
        this.productService = productService;
    }

    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-test-group"
    )
    public void consumeOrderCreated(
            OrderCreatedEvent event) {

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


        // =================================
        // CHECK AND RESERVE REAL STOCK
        // =================================

        boolean stockReserved =
                productService.reserveStock(
                        event.getProductId(),
                        event.getQuantity()
                );


        // =================================
        // INVENTORY FAILED
        // =================================

        if (!stockReserved) {

            System.out.println(
                    "❌ INVENTORY RESERVATION FAILED"
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
                "✅ INVENTORY RESERVED"
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
    @KafkaListener(
            topics = "inventory-release",
            groupId = "inventory-release-test-group",
            containerFactory = "inventoryReleaseKafkaListenerContainerFactory"
    )
    public void consumeInventoryRelease(
            InventoryReleaseEvent event) {

        System.out.println("=================================");
        System.out.println("INVENTORY RELEASE EVENT RECEIVED");
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

        productService.releaseStock(
                event.getProductId(),
                event.getQuantity()
        );
    }
}