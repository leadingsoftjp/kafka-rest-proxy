package net.leadingsoft.benchmark.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class ReceiverController {

  @Autowired
  KafkaTemplate kafkaTemplate;

  @PostMapping("/api/{topic}")
  public ResponseEntity<Void> receive(@PathVariable String topic, @RequestBody String body) throws ExecutionException, InterruptedException {
    kafkaTemplate.send(topic, body).get();
    return new ResponseEntity<>(HttpStatus.OK);
  }
}
