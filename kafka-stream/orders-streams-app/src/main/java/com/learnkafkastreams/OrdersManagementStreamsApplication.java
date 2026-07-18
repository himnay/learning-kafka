package com.learnkafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
class OrdersManagementStreamsApplication {

	/** Application entry point. */
	public static void main(String[] args) {
		SpringApplication.run(OrdersManagementStreamsApplication.class, args);
	}

}
