package com.ecommeerce.payment_service.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentKafkaProducer(
            KafkaTemplate<String, Object> kafkaTemplate) {

        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentCompleted(
            PaymentCompletedEvent event) {

        kafkaTemplate.send(
                "payment-completed",
                event.getOrderId().toString(),
                event
        );

        System.out.println(
                "PaymentCompleted event sent for order: "
                        + event.getOrderId()
        );
    }

    public void sendPaymentFailed(
            PaymentFailedEvent event) {

        kafkaTemplate.send(
                "payment-failed",
                event.getOrderId().toString(),
                event
        );

        System.out.println(
                "PaymentFailed event sent for order: "
                        + event.getOrderId()
        );
    }
}
