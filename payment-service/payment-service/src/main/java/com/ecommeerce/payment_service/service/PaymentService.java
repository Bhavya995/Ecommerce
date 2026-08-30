package com.ecommeerce.payment_service.service;

import com.ecommeerce.payment_service.entity.Payment;
import com.ecommeerce.payment_service.entity.PaymentStatus;
import com.ecommeerce.payment_service.repository.PaymentRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
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
}
