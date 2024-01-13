package com.events.kafkarealworld.service;

import com.events.kafkarealworld.config.WikimediaEventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducerService {

    private final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topicName;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produceWikimediaChanges() throws InterruptedException {
        log.info("in producerWikiMediaChanges()");
        // Asynchronously send the message to Kafka
        BackgroundEventHandler eventHandler = new WikimediaEventHandler(kafkaTemplate, topicName);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSource = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backgroundEventSourceBuilder = new BackgroundEventSource.Builder(eventHandler, eventSource);
        try (BackgroundEventSource backgroundEventSource = backgroundEventSourceBuilder.build()) {
            backgroundEventSource.start();
            TimeUnit.MINUTES.sleep(1);
        } finally {
            // Log a message indicating the termination
            log.info("BackgroundEventSource terminated.");
        }
    }
}
