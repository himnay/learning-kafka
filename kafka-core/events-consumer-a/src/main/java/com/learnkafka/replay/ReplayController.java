package com.learnkafka.replay;

import com.learnkafka.consumer.LibraryEventsConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REPLAY library events by seeking the consumer back. Kafka keeps the records (retention),
 * so a consumer can re-read them at any time — it only has to move its position.
 *
 * Replayed records are delivered to the listener again, and the idempotent consumer
 * (processed_event) skips the ones already applied: a replay is safe by design. To really
 * re-apply a range, delete its processed_event rows first.
 */
@RestController
@RequiredArgsConstructor
@Profile("!manual-offset")
@RequestMapping("/v1/library-events/replay")
public class ReplayController {

    private final LibraryEventsConsumer libraryEventsConsumer;

    @Value("${spring.application.name}")
    private String appName;

    /** Which partitions this app owns — replay requests must go to the owner. */
    @GetMapping("/partitions")
    public Map<String, Object> partitions() {
        return Map.of("app", appName, "topic", LibraryEventsConsumer.TOPIC,
                "partitions", libraryEventsConsumer.assignedPartitions());
    }

    /** POST /v1/library-events/replay?partition=0&offset=5 */
    @PostMapping(params = {"partition", "offset"})
    public ResponseEntity<Map<String, Object>> replay(@RequestParam int partition, @RequestParam long offset) {
        if (!libraryEventsConsumer.replay(partition, offset)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "app", appName, "error", "partition " + partition + " is not assigned to this app",
                    "ownedPartitions", libraryEventsConsumer.assignedPartitions()));
        }
        return ResponseEntity.accepted().body(Map.of("app", appName, "partition", partition, "fromOffset", offset));
    }

    /** POST /v1/library-events/replay?timestamp=1760000000000 — every owned partition */
    @PostMapping(params = "timestamp")
    public ResponseEntity<Map<String, Object>> replayFrom(@RequestParam long timestamp) {
        return ResponseEntity.accepted().body(Map.of("app", appName,
                "partitions", libraryEventsConsumer.replayFrom(timestamp), "fromTimestamp", timestamp));
    }
}
