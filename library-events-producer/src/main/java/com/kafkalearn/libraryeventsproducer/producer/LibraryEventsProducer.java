package com.kafkalearn.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventAsync(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        /*//
         TODO this is an asynchronous call that will not block the thread waiting for a response,
           so it will return a 201 created status from the APi before the code knows the execution of the send is correct
           */

        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(topic, key, value);

        return completableFuture.whenComplete((sendResult, error) -> {

            if (error != null) {
                handleErrorOnSendMessageToTopic(error);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });

    }


    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        /*
         * TODO using the following line
         *       SendResult<Integer, String> completableFutureResult = kafkaTemplate.send(topic, key, value).get();
         * you can use .get(3,TimeUnit.SECONDS) for timeOut
         * we can make the result to block till processed having the return type  SendResult instead of  a CompletableFuture
         *  */
        SendResult<Integer, String> sendResult = kafkaTemplate.send(topic, key, value).get(3, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;
    }

    public SendResult<Integer, String> sendLibraryEventProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        /*TODO this approach uses a wrapper to send the event info  ProducerRecord.*/

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value);
        SendResult<Integer, String> sendResult = kafkaTemplate.send(producerRecord).get(3, TimeUnit.SECONDS);

        handleSuccess(key, value, sendResult);

        return sendResult;

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully for the key: {} and the value: {}, partition is: {}", key, value, sendResult.getRecordMetadata().partition());
    }

    public void handleErrorOnSendMessageToTopic(Throwable error) {
        log.error("Error sending the message from the producer to kafka: {} ", error.getMessage(), error);
    }
}
