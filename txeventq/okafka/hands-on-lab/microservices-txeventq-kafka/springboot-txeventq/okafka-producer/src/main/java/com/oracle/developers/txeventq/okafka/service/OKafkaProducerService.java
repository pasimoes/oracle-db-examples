/*
 **
 ** Copyright (c) 2021 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 */
package com.oracle.developers.txeventq.okafka.service;


import com.oracle.developers.txeventq.okafka.config.producer.OKafkaProducerConfig;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.oracle.okafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.security.Provider;
import java.security.Security;
import java.util.Map;
import java.util.concurrent.Future;

@Service
public class OKafkaProducerService {
    private static final Logger LOG = LoggerFactory.getLogger(OKafkaProducerService.class);

    //private final KafkaProducer<String, String> kafkaProducer;

    private final Producer<String, String> producer;

    private final OKafkaProducerConfig producerConfig;


    public OKafkaProducerService(OKafkaProducerConfig producerConfig) {

        Map<String, Object> prodConfig = producerConfig.producerConfig();
        for (Map.Entry<String, Object> entry : prodConfig.entrySet()) {
            LOG.info("txeventqProducerConfig Key='{}', value='{}'", entry.getKey(), entry.getValue());
        }
        this.producerConfig = producerConfig;

        //this.kafkaProducer = new KafkaProducer<String, String>(producerConfig.producerConfig());

        this.producer = new KafkaProducer<>(prodConfig);

        addOraclePKIProvider();
    }

    public void send(String topicName, String key, String message) {
        LOG.info("Sending message='{}' to topic='{}'", message, topicName);

        RecordHeader rH1 = new RecordHeader("CLIENT_ID", "FIRST_CLIENT".getBytes());
        RecordHeader rH2 = new RecordHeader("REPLY_TO", "TXEQ_2".getBytes());

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, key, message);
        producerRecord.headers().add(rH1).add(rH2);

//        ProducerRecord<String, String> prodRec = new ProducerRecord<>(topicName, 0, key, message);
        LOG.info("Created ProdRec: {}", producerRecord);
        producer.send(producerRecord);
        LOG.info("Sent message key: {} ", key);
    }

    public void send(ProducerRecord<String, String> prodRec) {
        LOG.info("Sending message='{}' to topic='{}'", prodRec.value(), prodRec.topic());
        producer.send(prodRec);
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            LOG.info("Closing kafka producer!");
            producer.close();
        }
    }

    private static void addOraclePKIProvider() {
        System.out.println("Installing Oracle PKI provider.");
        Provider oraclePKI = new oracle.security.pki.OraclePKIProvider();
        Security.insertProviderAt(oraclePKI,3);
    }
}
