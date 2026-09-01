package com.ecommeerce.inventory_service.config;

import com.ecommeerce.inventory_service.kafka.InventoryReleaseEvent;
import com.ecommeerce.inventory_service.kafka.OrderCreatedEvent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "inventory-test-group"
        );

        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // Key
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        // Error handling
        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        // Actual JSON deserializer
        config.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JacksonJsonDeserializer.class
        );

        // Tell consumer what class to create
        config.put(
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
                OrderCreatedEvent.class.getName()
        );

        // Trust our Inventory event package
        config.put(
                JacksonJsonDeserializer.TRUSTED_PACKAGES,
                "com.ecommeerce.inventory_service.kafka"
        );

        // IMPORTANT:
        // Ignore the producer's Java type information
        config.put(
                JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS,
                false
        );

        return new DefaultKafkaConsumerFactory<>(
                config
        );
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>
    kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>
                factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
    @Bean
    public ConsumerFactory<String, InventoryReleaseEvent>
    inventoryReleaseConsumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "inventory-release-test-group"
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
                InventoryReleaseEvent.class.getName()
        );

        config.put(
                JacksonJsonDeserializer.TRUSTED_PACKAGES,
                "com.ecommeerce.inventory_service.kafka"
        );

        config.put(
                JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS,
                false
        );

        return new DefaultKafkaConsumerFactory<>(
                config
        );
    }
    @Bean(name = "inventoryReleaseKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<
            String,
            InventoryReleaseEvent
            > inventoryReleaseKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                InventoryReleaseEvent
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                inventoryReleaseConsumerFactory()
        );

        return factory;
    }
}