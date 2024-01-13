package com.events.kafkarealworld.controller;

import com.events.kafkarealworld.service.KafkaProducerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/producer")
public class ProducerController {

    private KafkaProducerService kafkaProducerService;

    public ProducerController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @GetMapping("/wikimedia-changes")
    public void produceWikimediaChanges() throws InterruptedException {
        kafkaProducerService.produceWikimediaChanges();
    }
}
