package com.events.kafkarealworld.config;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class WikimediaEventHandler implements BackgroundEventHandler {


    private final KafkaTemplate<String, String> kafkaTemplate;
    private String topicName;

    private Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class);

    public WikimediaEventHandler(KafkaTemplate<String, String> kafkaTemplate, String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    @Override
    public void onOpen() throws Exception {
        //nothing here
    }

    @Override
    public void onClosed() throws Exception {
        kafkaTemplate.getProducerFactory().createProducer().close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        String messageData = messageEvent.getData();
        log.info(messageData);
        kafkaTemplate.send(topicName, messageData);
    }

    @Override
    public void onComment(String s) throws Exception {
        //Nothing here
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in stream reading", throwable);
    }
}
