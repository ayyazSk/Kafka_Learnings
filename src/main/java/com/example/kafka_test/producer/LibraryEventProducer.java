package com.example.kafka_test.producer;

import com.example.kafka_test.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {


    @Value("${spring.kafka.topic}")
    String topicName;

    private final KafkaTemplate<Integer,String> kafkaTemplate;

    ObjectMapper obj;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper obj) {
        this.kafkaTemplate = kafkaTemplate;
        this.obj = obj;
    }



    public CompletableFuture<SendResult<Integer, String>> publishLibraryEvent(LibraryEvent libraryEvent) {

        var key = libraryEvent.libraryEventId();
        var value = obj.writeValueAsString(libraryEvent);

        // 1. for very first call  to kafka template there is blocking call that happens to get metadata of kafka cluster
        // 2. once kafka cluster details are received send msg happens and completable future is returned

    var completableFuture =   kafkaTemplate.send(topicName,key,value );
       return completableFuture.whenComplete((sendResult , throwable) ->{
            if(throwable !=null){
                handleFailure(key,value,throwable);
            }
            else{
                handleSuccess(key,value,sendResult);
            }
        } );

    }


    public SendResult<Integer, String> publishLibraryEvent_approach2(LibraryEvent libraryEvent) throws ExecutionException, InterruptedException {

        var key = libraryEvent.libraryEventId();
        var value = obj.writeValueAsString(libraryEvent);

        // blocking call happens to get cluster meta data
        //1.  blocks and waits till message is sent !
        var sendResult =   kafkaTemplate.send(topicName,key,value ).get();
        handleSuccess(key,value,sendResult);

        return sendResult;

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message Sent successfully key : {} value : {} partition details : {} ",key,value, sendResult.getRecordMetadata().partition());

    }

    private void handleFailure(Integer key, String value, Throwable throwable) {

        log.error("Error sending the message and the exception is {} ",throwable.getMessage());

    }
}
