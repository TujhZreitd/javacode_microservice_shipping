package ru.javacode.ShippingMicroservice.handler;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import ru.javacode.core.OrderCreatedEvent;

import java.util.concurrent.ExecutionException;

@Component
@KafkaListener(topics = "payed-orders-topic")
@AllArgsConstructor
public class OrderCreatedEventHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaHandler
    public void handle(OrderCreatedEvent orderCreatedEvent) throws ExecutionException, InterruptedException {
        LOGGER.info("Received event: {}", orderCreatedEvent.getNameProduct());
        //TODO logic shipping order
        SendResult<String, Object> result = kafkaTemplate
                .send("sent-orders-topic", orderCreatedEvent.getProductId(), orderCreatedEvent).get();
        LOGGER.info("Topic: {}", result.getRecordMetadata().topic());
        LOGGER.info("Partition: {}", result.getRecordMetadata().partition());
        LOGGER.info("Offset: {}", result.getRecordMetadata().offset());

        LOGGER.info("Return: {}", orderCreatedEvent.getProductId());

    }
}
