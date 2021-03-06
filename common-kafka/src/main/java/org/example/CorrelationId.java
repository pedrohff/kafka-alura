package org.example;

import java.util.UUID;

public class CorrelationId {
    private final String id;


    public CorrelationId(String handlerName) {
        this.id = handlerName + "(" + UUID.randomUUID().toString() + ")";
    }

    @Override
    public String toString() {
        return "br.com.alura.ecommerce.CorrelationId{" +
                "id='" + id + '\'' +
                '}';
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(id + "-" + title);
    }
}
