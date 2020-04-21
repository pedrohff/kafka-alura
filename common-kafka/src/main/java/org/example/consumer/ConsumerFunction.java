package org.example.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.Message;

public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
