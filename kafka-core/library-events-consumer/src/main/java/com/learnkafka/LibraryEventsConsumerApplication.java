package com.learnkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
class LibraryEventsConsumerApplication {

	/** Application entry point. */
	public static void main(String[] args) {
		SpringApplication.run(LibraryEventsConsumerApplication.class, args);
	}

}
