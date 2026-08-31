package com.ecommeerce.order_service.kafka;

public class PaymentCompletedEvent {

    private Long orderId;
    private String status;

    public PaymentCompletedEvent() {
    }

    public PaymentCompletedEvent(
            Long orderId,
            String status) {

        this.orderId = orderId;
        this.status = status;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
