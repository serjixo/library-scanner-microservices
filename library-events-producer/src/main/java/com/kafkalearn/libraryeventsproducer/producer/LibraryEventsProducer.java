package com.kafkalearn.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class LibraryEventsProducer {
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${spring.kafka.topic}")
    public String topic;

    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(topic, key, value);

        return completableFuture.whenComplete((sendResult, error) -> {

            if (error != null) {
                handleErrorOnSendMessageToTopic(error);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully for the key: {} and the value: {}, partition is: {}", key, value, sendResult.getRecordMetadata().partition());
    }

    public void handleErrorOnSendMessageToTopic(Throwable error) {
        log.error("Error sending the message from the producer to kafka: {} ", error.getMessage(), error);
    }
}
