package com.ecommeerce.payment_service.service;

import com.ecommeerce.payment_service.entity.Payment;
import com.ecommeerce.payment_service.entity.PaymentStatus;
import com.ecommeerce.payment_service.kafka.InventoryReservedEvent;
import com.ecommeerce.payment_service.kafka.PaymentCompletedEvent;
import com.ecommeerce.payment_service.kafka.PaymentFailedEvent;
import com.ecommeerce.payment_service.kafka.PaymentKafkaProducer;
import com.ecommeerce.payment_service.repository.PaymentRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PaymentService {

    private final PaymentKafkaProducer paymentKafkaProducer;
    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository,PaymentKafkaProducer paymentKafkaProducer) {
        this.paymentRepository = paymentRepository;
        this.paymentKafkaProducer = paymentKafkaProducer;;
    }

    public Payment createPayment(Payment payment) {

        payment.setStatus(PaymentStatus.PENDING);

        return paymentRepository.save(payment);
    }

    public List<Payment> getAllPayments() {
        return paymentRepository.findAll();
    }

    public Payment getPaymentById(Long id) {

        return paymentRepository.findById(id)
                .orElseThrow(() ->
                        new RuntimeException("Payment not found with id: " + id));
    }

    public Payment updatePayment(Long id, Payment payment) {

        Payment existingPayment = getPaymentById(id);

        existingPayment.setOrderId(payment.getOrderId());
        existingPayment.setAmount(payment.getAmount());

        return paymentRepository.save(existingPayment);
    }

    public void deletePayment(Long id) {

        Payment existingPayment = getPaymentById(id);

        paymentRepository.delete(existingPayment);
    }
    public void processPayment(
            InventoryReservedEvent event) {

        System.out.println("=================================");
        System.out.println("PROCESSING PAYMENT");
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

        // Temporary payment logic
        boolean paymentSuccessful = false;

        if (paymentSuccessful) {

            System.out.println(
                    "✅ PAYMENT SUCCESSFUL"
            );

            PaymentCompletedEvent completedEvent =
                    new PaymentCompletedEvent(
                            event.getOrderId(),
                            "COMPLETED"
                    );

            paymentKafkaProducer.sendPaymentCompleted(
                    completedEvent
            );

        } else {

            System.out.println(
                    "❌ PAYMENT FAILED"
            );

            PaymentFailedEvent failedEvent =
                    new PaymentFailedEvent(
                            event.getOrderId(),
                            "FAILED",
                            "Payment declined"
                    );

            paymentKafkaProducer.sendPaymentFailed(
                    failedEvent
            );
        }
    }
}
