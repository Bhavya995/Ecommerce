package com.ecommeerce.order_service.kafka;

import com.ecommeerce.order_service.service.OrderService;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderKafkaConsumer {

    private final OrderService orderService;

    public OrderKafkaConsumer(
            OrderService orderService) {

        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "payment-completed",
            groupId = "order-test-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentCompleted(
            PaymentCompletedEvent event) {

        System.out.println("=================================");
        System.out.println("PAYMENT COMPLETED EVENT RECEIVED");
        System.out.println("=================================");

        System.out.println(
                "Order ID: " + event.getOrderId()
        );

        System.out.println(
                "Payment Status: " + event.getStatus()
        );

        orderService.confirmOrder(
                event.getOrderId()
        );
    }
    @KafkaListener(
            topics = "payment-failed",
            groupId = "order-test-group-failed",
            containerFactory = "paymentFailedKafkaListenerContainerFactory"
    )
    public void consumePaymentFailed(
            PaymentFailedEvent event) {

        System.out.println("=================================");
        System.out.println("PAYMENT FAILED EVENT RECEIVED");
        System.out.println("=================================");

        System.out.println(
                "Order ID: " + event.getOrderId()
        );

        System.out.println(
                "Payment Status: " + event.getStatus()
        );

        System.out.println(
                "Reason: " + event.getReason()
        );

        orderService.cancelOrder(
                event.getOrderId()
        );
    }
}
