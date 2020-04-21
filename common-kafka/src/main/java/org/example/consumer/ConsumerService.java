package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.Message;

public interface ConsumerService<T> {
    String getConsumerGroup();

    String getTopic();


    void parse(ConsumerRecord<String, Message<T>> record);
}
