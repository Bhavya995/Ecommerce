package com.ecommeerce.order_service.service;

import com.ecommeerce.order_service.kafka.OrderCreatedEvent;
import com.ecommeerce.order_service.entity.Order;
import com.ecommeerce.order_service.entity.OrderStatus;
import com.ecommeerce.order_service.kafka.OrderKafkaProducer;
import com.ecommeerce.order_service.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderKafkaProducer orderKafkaProducer;

    public OrderService(OrderRepository orderRepository,
                        OrderKafkaProducer orderKafkaProducer) {
        this.orderRepository = orderRepository;
        this.orderKafkaProducer = orderKafkaProducer;
    }

    public Order createOrder(Order order) {

        order.setStatus(OrderStatus.PENDING);

        Order savedOrder = orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                savedOrder.getId(),
                savedOrder.getUserId(),
                savedOrder.getProductId(),
                savedOrder.getQuantity()
        );

        orderKafkaProducer.sendOrderCreatedEvent(event);

        return savedOrder;
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    public Order getOrderById(Long id) {

        return orderRepository.findById(id)
                .orElseThrow(() ->
                        new RuntimeException("Order not found with id: " + id));
    }

    public Order updateOrder(Long id, Order order) {

        Order existingOrder = getOrderById(id);

        existingOrder.setUserId(order.getUserId());
        existingOrder.setProductId(order.getProductId());
        existingOrder.setQuantity(order.getQuantity());

        return orderRepository.save(existingOrder);
    }

    public void deleteOrder(Long id) {

        Order existingOrder = getOrderById(id);

        orderRepository.delete(existingOrder);
    }
    public Order confirmOrder(Long orderId) {

        Order order = getOrderById(orderId);

        order.setStatus(OrderStatus.CONFIRMED);

        return orderRepository.save(order);
    }
    public Order cancelOrder(Long orderId) {

        Order order = getOrderById(orderId);

        order.setStatus(OrderStatus.CANCELLED);

        return orderRepository.save(order);
    }

}