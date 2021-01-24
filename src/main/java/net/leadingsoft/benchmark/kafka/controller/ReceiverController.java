package net.leadingsoft.benchmark.kafka.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
  public ResponseEntity<Void> receive(@PathVariable String topic, @RequestBody String body)
      throws ExecutionException, InterruptedException {
    kafkaTemplate.send(topic, body).get();
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @PostMapping("/api/json/{id}/{topic}")
  public ResponseEntity<Void> receiveJson(@PathVariable String id, @PathVariable String topic, @RequestBody String body)
      throws ExecutionException, InterruptedException {
    Object o = JSON.parse(body);
    if (o instanceof JSONObject) {
      JSONObject jo = (JSONObject) o;
      if (!id.equals(jo.getString("appId"))) {
        throw new IllegalArgumentException("illegal id.");
      }
    } else if (o instanceof JSONArray) {
      ((JSONArray) o).forEach(jo -> {
        if (!id.equals(((JSONObject) jo).getString("appId"))) {
          throw new IllegalArgumentException("illegal id.");
        }
      });
    }

    kafkaTemplate.send(topic, body).get();
    return new ResponseEntity<>(HttpStatus.OK);
  }
}
