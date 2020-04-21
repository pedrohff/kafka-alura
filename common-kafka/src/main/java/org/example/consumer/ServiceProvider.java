package org.example.consumer;

import java.util.HashMap;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {
    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() throws Exception {
        ConsumerService emailService = factory.create();
        var service = new KafkaService(
                emailService.getConsumerGroup(),
                emailService.getTopic(),
                emailService::parse,
                new HashMap<>()
        );
        service.run();
        return null;
    }
}
