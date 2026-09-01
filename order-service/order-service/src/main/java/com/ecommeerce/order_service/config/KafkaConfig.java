package com.ecommeerce.order_service.config;

import com.ecommeerce.order_service.kafka.InventoryReleaseEvent;
import com.ecommeerce.order_service.kafka.OrderCreatedEvent;
import com.ecommeerce.order_service.kafka.PaymentCompletedEvent;

import com.ecommeerce.order_service.kafka.PaymentFailedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    // ==========================================
    // PRODUCER CONFIGURATION
    // ==========================================

    @Bean
    public ProducerFactory<String, OrderCreatedEvent> producerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JacksonJsonSerializer.class
        );

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate() {

        return new KafkaTemplate<>(producerFactory());
    }


    // ==========================================
    // CONSUMER CONFIGURATION
    // ==========================================

    @Bean
    public ConsumerFactory<String, PaymentCompletedEvent>
    paymentConsumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "order-test-group"
        );

        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // Key deserializer
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        // Error handling deserializer
        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        // Actual JSON deserializer
        config.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JacksonJsonDeserializer.class
        );

        // Tell Jackson which local class to create
        config.put(
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
                PaymentCompletedEvent.class.getName()
        );

        // Trust our local Order Service event package
        config.put(
                JacksonJsonDeserializer.TRUSTED_PACKAGES,
                "com.ecommeerce.order_service.kafka"
        );

        // Ignore producer's Java class information
        config.put(
                JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS,
                false
        );

        return new DefaultKafkaConsumerFactory<>(config);
    }


    // ==========================================
    // KAFKA LISTENER FACTORY
    // ==========================================

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentCompletedEvent
            > kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentCompletedEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                paymentConsumerFactory()
        );

        return factory;
    }
    @Bean
    public ConsumerFactory<String, PaymentFailedEvent>
    paymentFailedConsumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "order-test-group-failed"
        );

        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        config.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JacksonJsonDeserializer.class
        );

        config.put(
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
                PaymentFailedEvent.class.getName()
        );

        config.put(
                JacksonJsonDeserializer.TRUSTED_PACKAGES,
                "com.ecommeerce.order_service.kafka"
        );

        config.put(
                JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS,
                false
        );

        return new DefaultKafkaConsumerFactory<>(config);
    }
    @Bean(name = "paymentFailedKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentFailedEvent
            > paymentFailedKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentFailedEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                paymentFailedConsumerFactory()
        );

        return factory;
    }
    @Bean
    public ProducerFactory<String, InventoryReleaseEvent>
    inventoryReleaseProducerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JacksonJsonSerializer.class
        );

        return new DefaultKafkaProducerFactory<>(config);
    }
    @Bean
    public KafkaTemplate<String, InventoryReleaseEvent>
    inventoryReleaseKafkaTemplate() {

        return new KafkaTemplate<>(
                inventoryReleaseProducerFactory()
        );
    }
}