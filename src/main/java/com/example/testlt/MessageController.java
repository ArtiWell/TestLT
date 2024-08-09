package com.example.testlt;

import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
@AllArgsConstructor
public class MessageController {

    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "postedmessages";


    @PostMapping("/post-message")
    public ResponseEntity<?> postMessage(@RequestBody Map<String, String> request) {
        try {
            String msgId = request.get("msg_id");
            long timestamp = Instant.now().toEpochMilli();
            String method = "POST";
            String uri = "/post-message";

            Map<String, Object> message = new HashMap<>();
            message.put("msg_id", msgId);
            message.put("timestamp", timestamp);
            message.put("method", method);
            message.put("uri", uri);

            kafkaTemplate.send(TOPIC, message.toString());

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending message to Kafka");
        }
    }
}
