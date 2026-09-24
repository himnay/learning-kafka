package com.learnavro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
class CoffeeOrdersConsumerApplication {

    /** Application entry point. */
    public static void main(String[] args) {
        // Avro >= 1.12.1 only instantiates SpecificRecord classes from trusted packages
        // (ClassSecurityValidator); without this, (de)serializing CoffeeOrder throws SecurityException.
        System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "com.learnavro.domain.generated");
        SpringApplication.run(CoffeeOrdersConsumerApplication.class, args);
    }

}
