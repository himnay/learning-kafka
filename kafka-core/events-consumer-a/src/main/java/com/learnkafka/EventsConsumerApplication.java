package com.learnkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling            // RetryTopicDrainer
@ConfigurationPropertiesScan // RetryProperties (app.retry.*)
class EventsConsumerApplication {

	/** Application entry point. */
	public static void main(String[] args) {
		// Avro >= 1.12.1 only (de)serializes SpecificRecord classes from trusted packages
		// (ClassSecurityValidator); without this, LibraryEvent / CoffeeOrder throw SecurityException.
		System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "com.learnkafka.avro,com.learnavro.domain.generated");
		SpringApplication.run(EventsConsumerApplication.class, args);
	}

}
