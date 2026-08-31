package com.ecommeerce.payment_service.kafka;

import com.ecommeerce.payment_service.service.PaymentService;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentKafkaConsumer {

    private final PaymentService paymentService;

    public PaymentKafkaConsumer(
            PaymentService paymentService) {

        this.paymentService = paymentService;
    }

    @KafkaListener(
            topics = "inventory-reserved",
            groupId = "payment-test-group"
    )
    public void consumeInventoryReserved(
            InventoryReservedEvent event) {

        System.out.println("=================================");
        System.out.println("INVENTORY RESERVED EVENT RECEIVED");
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

        // Call Payment Service
        paymentService.processPayment(event);
    }
}